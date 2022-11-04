import copy
import hashlib
import json
import os
from typing import Any, Dict, List, Optional, Tuple

import requests
from bson import ObjectId, json_util
from document_utils.document import record_duplicates
from kafka_utils.consts import DATASOURCE_STATS_KAFKA_TOPIC_NAME
from kafka_utils.consumer import Consumer
from kafka_utils.producer import Producer
from logger_utils import logger
from mongo_utils import SINGLETON_MONGO_CLIENT as MONGO_CLIENT
from mongo_utils.consts import (AWS_S3_DATASOURCE, DATASOURCE, DOCUMENT, OBJ_TYPE_DOCUMENT, PII_RECORD,
                                AttachmentDownloadFailedSkipReason, TikaRequestFailedSkipReason)
from requests import Response
from tika_utils.v1.response_processor import TikaResponseProcessor as ResponseProcessor
from lb_common.service_requests import LBRequestMeta, get_session, send_file_to_tika
from app.config import DATASOURCE_TYPE, S3_CLIENT
from app.consts import (CHECKSUM_TYPE, DATASOURCE_ID, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_OFFSET,
                        KAFKA_MAX_POLL_INTERVAL_MS, KAFKA_MAX_POLL_RECORDS, KAFKA_TOPIC_NAME, TARGET_TEXT_EXTRACTION)

STATS_AGGREGATOR_PRODUCER = Producer(os.getenv(KAFKA_BOOTSTRAP_SERVERS))


class AWSS3Consumer:
    def __init__(self, datasource_id: ObjectId):
        """
        Creates a S3 consumer object responsible for consuming messages from the datasource topic and processes
        the file by fetching and sending it to the handler service and writing the results to MongoDB.
        """
        self.datasource_id = datasource_id
        datasource_record = MONGO_CLIENT[DATASOURCE].find_one({"_id": datasource_id},
                                                              {"_id": 0, "name": 1, "location": 1})
        datasource_name = datasource_record["name"]
        datasource_location = datasource_record["location"]
        self.response_processor = ResponseProcessor(DATASOURCE_ID, datasource_name, datasource_location,
                                                    AWS_S3_DATASOURCE)

        if TARGET_TEXT_EXTRACTION:
            text_extraction_base_url = os.getenv("TEXT_EXTRACTION_BASE_URL")
            if not text_extraction_base_url:
                raise Exception("TEXT_EXTRACTION_BASE_URL environment variable not set")
            self.file_processor_endpoint = f"{text_extraction_base_url}/identify"
            self.file_processor_timeout = int(os.getenv("TEXT_EXTRACTION_REQUEST_TIMEOUT", 900))
        else:
            tika_base_url = os.getenv("TIKA_BASE_URL")
            if not tika_base_url:
                raise Exception("TIKA_BASE_URL environment variable not set")
            self.file_processor_endpoint = f"{tika_base_url}/process"
            self.file_processor_timeout = int(os.getenv("TIKA_REQUEST_TIMEOUT", 900))

        # Config kafka consumer.
        kafka_topic_name = os.getenv(KAFKA_TOPIC_NAME)
        kafka_bootstrap_servers = os.getenv(KAFKA_BOOTSTRAP_SERVERS)
        kafka_consumer_offset = os.getenv(KAFKA_CONSUMER_OFFSET)
        kafka_max_poll_records = int(os.getenv(KAFKA_MAX_POLL_RECORDS, 2))
        kafka_max_poll_interval_ms = int(os.getenv(KAFKA_MAX_POLL_INTERVAL_MS, 420000))

        # As file processing takes quite some time, we need to ensure that we fetch a limited number of messages in each
        # poll call of the consumer so that consumer polling interval is within threshold and that the consumer stays
        # alive.
        if (kafka_max_poll_interval_ms - 100) < (kafka_max_poll_records * (self.file_processor_timeout * 1000)):
            raise Exception("Kafka consumer polling parameter values not compatible with max tika response time")
        kafka_consumer = Consumer(kafka_bootstrap_servers,
                                  kafka_consumer_offset,
                                  kafka_max_poll_records,
                                  kafka_max_poll_interval_ms,
                                  kafka_topic_name)
        self.consumer = kafka_consumer.consumer
        logger.info(f"Bootstrap info server: {KAFKA_BOOTSTRAP_SERVERS}, topic: {kafka_topic_name}")
        self.raise_request_failure_exception = os.getenv("RAISE_REQUEST_FAILURE_EXCEPTION", "False").lower() == "true"

        # Added to use once a request to file processor is made to fetch as the status code of that request
        # is needed to update the scanned data counter on prometheus
        self.file_size = 0
        tika_endpoint = f"{os.getenv('TIKA_BASE_URL')}/process"
        tika_timeout = int(os.getenv("TIKA_TIMEOUT", 180))
        session_with_retry = get_session()
        self._tika_request_meta_object = LBRequestMeta(request_session=session_with_retry,
                                                       service_endpoint=tika_endpoint,
                                                       request_timeout=tika_timeout)

    @staticmethod
    def build_additional_metadata(key: str, bucket_name: str, object_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        Builds and returns a metadata dictionary to be used in creating/updating the document record in the
        MongoDB document collection.
        """
        additional_metadata = {
            "key": key,
            "bucketName": bucket_name,
        }
        if "VersionId" in object_details:
            additional_metadata["versionId"] = object_details["VersionId"]
        if object_details.get("Owner"):
            additional_metadata["ownerId"] = object_details["Owner"]["ID"]
            additional_metadata["ownerName"] = object_details["Owner"].get("DisplayName", "")
        return additional_metadata

    def handle_new_document(self, key: str, bucket_name: str, file_type: str,
                            object_details: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a new record in the MongoDB document collection, checks and records if the document is a duplicate
        and updates the summary collections.
        """
        additional_metadata = self.build_additional_metadata(key, bucket_name, object_details)
        checksum = object_details["checksum"]

        mongo_document = {
            "name": key.split("/")[-1],
            "type": file_type,
            "objectType": OBJ_TYPE_DOCUMENT,
            "size": object_details["ContentLength"],
            "datasourceId": self.datasource_id,
            "lastModifiedTime": object_details["LastModified"],
            "checksum": checksum,
            "checksumType": CHECKSUM_TYPE,
            "additionalMetadata": additional_metadata,
            "processed": False,
            "scanned": True,
            "version": 1
        }
        result = MONGO_CLIENT[DOCUMENT].insert_one(mongo_document)
        document_id = result.inserted_id
        mongo_document["_id"] = document_id
        logger.info(f"Inserted MongoDB document {document_id} and file {mongo_document['name']}")
        record_duplicates(document_id, checksum, CHECKSUM_TYPE)
        return mongo_document

    def handle_existing_document(self, old_document: Dict[str, Any], key: str, bucket_name: str,
                                 object_details: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        """
        Update database records for existing document. Based on checking file content for changes, returns whether
        the document should be further processed or skipped
        """
        additional_metadata = self.build_additional_metadata(key, bucket_name, object_details)
        checksum = object_details["checksum"]
        file_size = object_details["ContentLength"]
        last_modified_time = object_details["LastModified"]
        document_id = old_document["_id"]
        update_dict = {"lastModifiedTime": last_modified_time, "additionalMetadata": additional_metadata}
        if checksum == old_document["checksum"]:
            # This method is called after checking that the modified time of the current version of the file is
            # different than the one in the database record for the file. Although, the checksum has not changed,
            # we want to update the modified time along with any change in additional metadata fields.
            logger.info(f"Updating metadata for document {document_id} in the db")
            MONGO_CLIENT[DOCUMENT].update_one({"_id": document_id}, {"$set": update_dict})
            old_document.update(update_dict)
            return old_document, True

        update_dict.update({
            "checksum": checksum,
            "processed": False,
            "scanned": True,
            "size": file_size
        })
        MONGO_CLIENT[DOCUMENT].update_one({"_id": document_id}, {"$set": update_dict,
                                                                 "$inc": {"version": 1}})
        logger.info(f"Updated MongoDB document {document_id}")
        old_document.update(update_dict)
        record_duplicates(document_id, checksum, CHECKSUM_TYPE)
        return old_document, False

    def should_process_document(
            self, key: str, bucket_name: str, file_type: str, object_details: Dict[str, Any],
            document_id: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool]:
        """
        Checks if the document should be processed.

        Existing documents having the same modified time, version_id and checksum need no processing at all. For
        existing documents, when modified time or version_id has changed, but there is no change is checksum, then only
        metadata is updated and further processing is skipped. New documents need to be processed as there exists no
        records for them in the system.
        """
        if document_id:
            # Used for the rescan job.
            document_id = ObjectId(document_id)
            return MONGO_CLIENT[DOCUMENT].find_one({"_id": document_id}), None, True
        if "ServerSideEncryption" not in object_details or object_details["ServerSideEncryption"] == "AES256":
            # Server-side unencrypted objects and objects encrypted with AES256 have their ETag equal to their
            # MD5 digest. Etags are in the format of '"6d8cb2f77d0c0a01fd3909c271480c9f"' so we omit the
            # first and last characters.
            checksum = object_details["checksum"] = object_details["ETag"][1:-1]
        else:
            object_details["file_content"] = file_content = S3_CLIENT.get_object(
                Bucket=bucket_name, Key=key)["Body"].read()
            hashes = hashlib.md5()
            hashes.update(file_content)
            checksum = object_details["checksum"] = hashes.hexdigest()

        old_doc: Optional[Dict[str, Any]] = None
        old_document = MONGO_CLIENT[DOCUMENT].find_one(
            {"datasourceId": DATASOURCE_ID, "additionalMetadata.key": key,
             "additionalMetadata.bucketName": bucket_name})
        if old_document:
            old_doc = copy.deepcopy(old_document)
            last_modified_time = object_details["LastModified"]
            version_id = object_details.get("VersionId")
            is_document_processed = old_document.get("processed", False)
            old_version_id = old_document["additionalMetadata"].get("versionId")
            old_last_modified_time = old_document["lastModifiedTime"]
            old_checksum = old_document["checksum"]
            if all((is_document_processed, old_last_modified_time == last_modified_time, checksum == old_checksum,
                    old_version_id == version_id)):
                return old_document, None, False
            new_document, skip_document = self.handle_existing_document(old_document, key, bucket_name, object_details)
            if is_document_processed and skip_document:
                return old_doc, new_document, False
        else:
            new_document = self.handle_new_document(key, bucket_name, file_type, object_details)
        return old_doc, new_document, True

    def handle_tika_failure(self, document: Dict[str, Any], previous_document: Optional[Dict[str, Any]] = None,
                            previous_pii_records: Optional[List[Dict[str, Any]]] = None):
        """
        Handle failure from ML services, mark the document as skipped and materialise counters by sending message to the
        stats aggregator
        :param document: Current document object.
        :param previous_document: Old/previous document object.
        :param previous_pii_records: List of PII records corresponding to the old document.
        """
        if not previous_pii_records:
            previous_pii_records = []
        skip_reason = TikaRequestFailedSkipReason
        document["skipped"] = True
        document["skipReason"] = skip_reason
        kafka_message_body = json.loads(json_util.dumps({
            "datasourceType": DATASOURCE_TYPE,
            "document": document,
            "piiRecords": [],
            "previousDocument": previous_document,
            "previousPiiRecords": previous_pii_records
        }))
        STATS_AGGREGATOR_PRODUCER.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                 str(DATASOURCE_ID),
                                                 kafka_message_body, synchronous=True)

    def process_object(self, key: str, bucket_name: str, file_type: str, document_id: Optional[str] = None):
        """
        :param key: Key of the AWS S3 object.
        :param bucket_name: Name of the bucket that the object belongs to.
        :param file_type: File type of the AWS S3 object.
        :param document_id: ID of the document for the object. It is only available for rescan jobs.
        """
        s3_style_key = f"s3://{bucket_name}{key}"
        try:
            object_details = S3_CLIENT.head_object(Bucket=bucket_name, Key=key)
        except S3_CLIENT.exceptions.NoSuchKey:
            logger.exception(f"Warning: The object {s3_style_key} does not exist anymore. Skipping without "
                             f"error.")
            return
        object_details["Owner"] = S3_CLIENT.get_object_acl(Bucket=bucket_name, Key=key)["Owner"]
        old_document, new_document, should_process = self.should_process_document(
            key, bucket_name, file_type, object_details, document_id=document_id)

        if document_id and not new_document:
            msg = f"Document {document_id} not found for S3 key {s3_style_key}"
            logger.error(msg)
            raise Exception(msg)

        if not should_process:
            document_id = old_document["_id"]
            logger.warning(
                f"Skipping document {document_id} for S3 key {s3_style_key} as it's already processed for its "
                f"latest version.")
            return

        file_name = new_document["name"]
        if "file_content" in object_details:
            # We download the objects encrypted with SSE-KMS to generate the checksum, so no need to make an
            # additional request in such cases
            file_content = object_details["file_content"]
        else:
            try:
                file_content = S3_CLIENT.get_object(Bucket=bucket_name, Key=key)["Body"].read()
            except S3_CLIENT.exceptions.NoSuchKey:
                logger.exception(f"Warning: The object {s3_style_key} does not exist anymore. Skipping without "
                                 f"error.")
                if not old_document:
                    new_document["skipped"] = True
                    new_document["skipReason"] = AttachmentDownloadFailedSkipReason

                    kafka_message_body = json.loads(json_util.dumps({
                        "datasourceType": DATASOURCE_TYPE,
                        "document": new_document,
                        "piiRecords": [],
                        "previousDocument": None,
                        "previousPiiRecords": []
                    }))
                    STATS_AGGREGATOR_PRODUCER.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME, str(DATASOURCE_ID),
                                                             kafka_message_body, synchronous=True)
                return

        request_failed = False
        response: Optional[Response] = None
        try:
            status_code, response_json = send_file_to_tika(file_content, file_name, self._request_meta_object)
            if(status_code!=200):
                self.handle_tika_failure(new_document, old_document, previous_pii_records)
                return 
        except Exception:
            logger.exception(f"Failed to get ML services response for file {file_name}, Skipping this document")
            request_failed = True
            self.handle_tika_failure(new_document, old_document, previous_pii_records)
        if response_json and status_code != 200:
            # TODO: Raise an alert in such cases.
            msg = (f"File processor request for object with key: {key} failed with "
                   f"status_code: {status_code}, response_body: {response.json}")
            logger.error(msg)
            if self.raise_request_failure_exception:
                raise Exception(msg)

        if request_failed:
            previous_pii_records: List[Dict[str, Any]] = []
            if old_document:
                previous_pii_records = list(MONGO_CLIENT[PII_RECORD].find({"documentId": old_document["_id"]}))
            logger.info(f"Marking document {new_document} as skipped")
            self.handle_tika_failure(new_document, old_document, previous_pii_records)
            return

        logger.debug(f"File processor response: {response_json}")
        pii_records, aggregator_kafka_payload = self.response_processor.process(
            new_document, response_json, is_new_document=False if old_document else True, old_document=old_document)
        STATS_AGGREGATOR_PRODUCER.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME, str(DATASOURCE_ID),
                                                 aggregator_kafka_payload, synchronous=True)

    def consume_kafka_topic(self):
        """
        This method contains the consumer logic which has a handler for all the messages and notifies the sender
        with respective warning and sensitive content information.
        """
        for message in self.consumer:
            value = message.value
            logger.info(f"Consuming Kafka message with key {message.key} and value {value}")
            self.process_object(**value)
            # Commit offset of the message in Kafka topic for the consumer.
            self.consumer.commit()


if __name__ == "__main__":
    logger.info("Starting consumer")
    consumer = AWSS3Consumer(DATASOURCE_ID)
    consumer.consume_kafka_topic()
