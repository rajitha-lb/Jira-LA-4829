"""
Module to consume files from Kafka topic for the datasource.
from multiple document formats and additionally calls ml-service for files which it cannot extract data from
(e.g. pdf and image files). The response received from tika service is then written to MongoDB by this consumer.
"""
import copy
import json
import os
import signal
import sys
import tempfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, NamedTuple, Optional, Tuple, Union
import pytz
import rclone
import requests
from bson import ObjectId, json_util
from kafka.consumer.fetcher import ConsumerRecord
from kafka_utils.consts import DATASOURCE_STATS_KAFKA_TOPIC_NAME
from kafka_utils.consumer import Consumer
from kafka_utils.producer import Producer
from lb_common.service_requests import send_file_to_tika
from lb_common.utils import process_log_file_request
from logger_utils import logger
from mongo_utils.consts import (ARCHIVED_DOCUMENT, DATASOURCE, DOCUMENT, DOCUMENT_ANNOTATION_TYPE_FILTER,
                                DUPLICATE_RECORDS, GOOGLE_DRIVE_DATASOURCE, MONGODB_DATABASE_NAME, OBJ_TYPE_DOCUMENT,
                                PII_RECORD, AttachmentDownloadFailedSkipReason, TikaProcessingFailedSkipReason,
                                TikaRequestFailedSkipReason)
from mongo_utils.mongo import SINGLETON_CLIENT as SINGLETON_MONGO_CLIENT
from prometheus_client import CollectorRegistry, Counter, push_to_gateway
from prometheus_utils.constants import PROMETHEUS_PUSH_GATEWAY_URL
from pymongo import InsertOne, UpdateOne
from requests.adapters import HTTPAdapter, Retry
from requests.sessions import Session as RequestsSession
from retry import retry
from tika_utils.consts import VALID_TYPES
from tika_utils.v1.response_processor import TikaResponseProcessor

# Constants dealing with the location and name of the rclone config file.
from vault_utils.vault_client import get_configuration

from common.config import Config
from common.consts import (DELEGATED_CREDENTIAL_ENV, DOMAIN_ENV, KAFKA_BOOTSTRAP_SERVERS, KAFKA_CONSUMER_OFFSET,
                           KAFKA_MAX_POLL_INTERVAL_MS, KAFKA_MAX_POLL_RECORDS, KAFKA_POLL_TIMEOUT, LOG_FILE_EXTENSIONS,
                           LOG_FILE_MAX_ALLOWED_CHARS, ML_SERVICES_RETRY_ATTEMPTS, ML_SERVICES_RETRY_DELAY_SECONDS,
                           PERSON_ACCOUNT_TYPE, PROCESSED_BYTES_LABEL_NAMES)
from common.gsuite_utils import (CHECKSUM_TYPE, DRIVE_TYPE_USER, fetch_updated_cred, handle_permissions,
                                 retrieve_permissions, service_account_login_drive_with_subject)
from common.rclone_utils import (RCLONE_CONF_DRIVE_NAME, create_rclone_config, get_personal_google_drive_rclone_config,
                                 process_rclone_env_variables)

MODULE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = (MODULE_DIR / "../.." / "config").resolve()


SIGTERM_SHUTDOWN = False
"""Whether the service has received the shutdown signal from SIGTERM."""


def sigterm_handler(signal: Any, frame: Any):
    """
    SIGTERM handler to ensure that the consumer does not immediately shutdown during processing of a message.
    """
    global SIGTERM_SHUTDOWN
    SIGTERM_SHUTDOWN = True


def create_config(datasource_id: ObjectId) -> Config:
    """
    Process GSuite and Kafka environment variables and creates a config object.
    :param datasource_id: Id of the datasource instance in MongoDB.
    :return: Config object.
    """
    datasource = SINGLETON_MONGO_CLIENT[MONGODB_DATABASE_NAME][DATASOURCE].find_one({"_id": datasource_id}, {"_id": 0})
    datasource["configuration"] = get_configuration()
    datasource_config = datasource["configuration"]
    service_account_file = process_rclone_env_variables(datasource_id)
    delegated_credential = os.getenv(DELEGATED_CREDENTIAL_ENV)
    domain = os.getenv(DOMAIN_ENV)

    # Kafka config.
    kafka_bootstrap_servers = os.getenv(KAFKA_BOOTSTRAP_SERVERS)
    kafka_consumer_offset = os.getenv(KAFKA_CONSUMER_OFFSET)
    kafka_max_poll_records = int(os.getenv(KAFKA_MAX_POLL_RECORDS, 2))
    kafka_max_poll_interval_ms = int(os.getenv(KAFKA_MAX_POLL_INTERVAL_MS, 420000))

    tika_request_timeout = int(os.getenv("TIKA_REQUEST_TIMEOUT", 900))
    # As Tika processing takes quite some time, we need to ensure that we fetch a limited number of messages in each
    # poll call of the consumer so that consumer polling interval is within threshold and that the consumer stays
    # alive.
    if (kafka_max_poll_interval_ms - 100) < (kafka_max_poll_records * (tika_request_timeout * 1000)):
        raise Exception("Kafka consumer polling parameter values not compatible with max tika response time")
    config = Config(datasource_id, service_account_file, delegated_credential, domain, kafka_bootstrap_servers,
                    kafka_consumer_offset=kafka_consumer_offset, kafka_max_poll_records=kafka_max_poll_records,
                    kafka_max_poll_interval_ms=kafka_max_poll_interval_ms,
                    account_type=datasource_config["accountType"])
    return config


class Document(NamedTuple):
    """
    Represents a document record being processed by the consumer.

    document: MongoDB document record.
    skip_document: Whether the consumer should skip further processing of the document based on initial checks.
    is_new_document: Whether the document being processed is identified by the system for the first time or is an
    updated version of an existing document record in the system.
    """
    document: Dict[str, Any]
    skip_document: bool
    is_new_document: bool


class GoogleDriveConsumer:
    def __init__(self, datasource_id: ObjectId):
        """
        Create a Google Drive consumer object responsible for consuming message from the datasource topic and processing
        the file by downloading it and sending to the file handler service and writing the results to MongoDB.
        :param datasource_id: MongoDB ObjectId of the datasource to be used as Kafka topic to consume files from.
        """
        self._config = create_config(datasource_id)
        self._stats_aggregator_producer = Producer(self._config.kafka_bootstrap_servers)
        self._db = SINGLETON_MONGO_CLIENT[MONGODB_DATABASE_NAME]
        self._datasource_id = datasource_id
        datasource_record = self._db[DATASOURCE].find_one({"_id": datasource_id},
                                                          {"_id": 0, "name": 1, "location": 1, "type": 1})
        if not datasource_record:
            raise Exception(f"Datasource {datasource_id} does not exist")
        self._datasource_name = datasource_record["name"]
        self._datasource_location = datasource_record["location"]
        self._datasource_type = datasource_record["type"]

        self._topic_name = os.getenv("KAFKA_TOPIC_NAME")
        if not self._topic_name:
            raise Exception(f"Topic name not set for consumer deployment of datasource {datasource_id}")
        self._consumer = Consumer(self._config.kafka_bootstrap_servers,
                                  self._config.kafka_consumer_offset,
                                  self._config.kafka_max_poll_records,
                                  self._config.kafka_max_poll_interval_ms,
                                  self._topic_name).consumer
        self._drive = None
        self._rclone_instance = None
        self._tika_response_processor = TikaResponseProcessor(self._datasource_id, self._datasource_name,
                                                              self._datasource_location, GOOGLE_DRIVE_DATASOURCE)
        self._raise_request_failure_exception = True if os.getenv(
            "RAISE_REQUEST_FAILURE_EXCEPTION", "False").lower() == "true" else False

        self._prometheus_registry = CollectorRegistry()
        self._processed_bytes = Counter("processed_bytes", "Google Drive Total Bytes Processed",
                                        labelnames=PROCESSED_BYTES_LABEL_NAMES,
                                        registry=self._prometheus_registry)

        # A map maintained for each message that contains keys as collection names and values are a
        # list of MongoDB operations to be performed on the collection. The operations in these map are executed in a
        # transaction so that consistency is ensured.
        self._message_db_ops: DefaultDict[str, List[Union[InsertOne, UpdateOne]]] = defaultdict(list)
        # Current processing document details and corresponding pii records.
        self._current_document: Optional[Dict[str, Any]] = None
        self._current_pii_records: Optional[List[Dict[str, Any]]] = None
        # Previous document details and and corresponding pii records, in case of document update.
        self._prev_document: Optional[Dict[str, Any]] = None
        self._prev_pii_records: Optional[List[Dict[str, Any]]] = None

    def _create_rclone_instance(self):
        """Creates rclone instance. Rclone instance is used to copy files from GDrive account."""
        rclone_config = create_rclone_config(self._config)
        self._rclone_instance = rclone.with_config(rclone_config)

    def download_file(self, file_path: str, target_dir: Union[str, bytes], drive_id: str,
                      shared_drive_user_email: Optional[str] = None, trashed: bool = False,
                      parent_folder_shared_with_user: bool = False) -> Tuple[bool, int]:
        """Downloads the file to the target directory.
        It is possible for the file to not be present because of the race condition where the file gets deleted after
        it is accessed so it needs to handle that case as well.
        :param file_path: File path in the source Google Drive.
        :param target_dir: Local directory where the file should be downloaded to.
        :param drive_id: In the case of an individual drive, this is the email Id of the user on whose behalf the drive
        is accessed to download the file. Otherwise, in the case of shared drive, this is the drive Id given by
        Google API which is to be specified as the root folder to look in for the document to be downloaded and the
        user email is passed in a separate param `shared_drive_user_email` on whose behalf the drive is accessed to
        download the file.
        :param shared_drive_user_email: Only set and used in case of a shared drive to impersonate a user who has
        access to the shared drive.
        :param trashed: Whether document is in the trash bin.
        :param parent_folder_shared_with_user: Whether the file owned by the user belongs to a parent folder which is
            not owned by the user but instead is shared with the user.
        :return: Returns whether the file was downloaded successfully and the size of the file measured in bytes.
        """
        if self._config.account_type == PERSON_ACCOUNT_TYPE:
            # For personal Google Drive, we need to refresh the access token and update rclone config.
            cred = fetch_updated_cred(self._config.service_account_file)
            rclone_config = get_personal_google_drive_rclone_config(cred)
            self._rclone_instance = rclone.with_config(rclone_config)

        user = drive_id
        flags = [f"--drive-impersonate={drive_id}"]
        if parent_folder_shared_with_user:
            flags.append("--drive-shared-with-me")
        if shared_drive_user_email:
            user = shared_drive_user_email
            flags = [f"--drive-impersonate={shared_drive_user_email}", f"--drive-root-folder-id={drive_id}"]
        if trashed:
            flags.append("--drive-trashed-only")
        res = self._rclone_instance.copy(source=f"{RCLONE_CONF_DRIVE_NAME}:/{file_path}", dest=target_dir,
                                         flags=flags)
        if res["code"] != 0:
            error = res["error"].decode("utf-8")
            if "directory not found" in error:
                logger.info(f"Skipping {file_path} for {user} as it's not present anymore")
                return False, -1
            raise Exception(f"Unable to copy file {file_path} for user {user}: {error}")
        file_name = Path(file_path).name
        downloaded_file_path = target_dir / Path(file_name)
        try:
            file_size = os.stat(downloaded_file_path).st_size
        except Exception as error:
            logger.error(f"Exception in {downloaded_file_path} while getting file size: {error}")
            return False, -1
        logger.debug(f"Downloaded {file_path} having size {file_size} from drive {drive_id} to {target_dir}")
        return True, file_size

    def _check_duplicate_exists(self, document_id: ObjectId, checksum: str, checksum_type: str) -> bool:
        """
        Checks whether a duplicate document exists having the same checksum as the given document.
        :param document_id: Id of the document being scanned.
        :param checksum: Checksum string.
        :param checksum_type: Checksum hash method.
        :return: Whether a duplicate document exists.
        """
        # TODO: Handle concurrency issues.
        duplicate_document = self._db[DOCUMENT].find_one({"checksum": checksum, "checksumType": checksum_type,
                                                          "_id": {"$ne": document_id}}, {"_id": 1})
        return bool(duplicate_document)

    def _record_duplicates(self, document_id: ObjectId, checksum: str, checksum_type: str):
        """
        Records the document Id in the duplicate records collection if it's a duplicate.
        :param document_id: Id of the document being scanned.
        :param checksum: Checksum string.
        :param checksum_type: Checksum hash calculation method.
        """
        duplicate_exists = self._check_duplicate_exists(document_id, checksum, checksum_type)
        if not duplicate_exists:
            return

        self._message_db_ops[DUPLICATE_RECORDS].append(UpdateOne({"checksum": checksum, "checksumType": checksum_type},
                                                                 {"$addToSet": {"documentIds": document_id}},
                                                                 upsert=True))

    def _handle_existing_document_metadata(self, old_document: Dict[str, Any],
                                           message: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        """Update database records for existing document. Based on checking file content for changes, returns whether
        the document should be further processed or skipped.
        :returns: A tuple consisting of the updated document record and a boolean indicating whether to send the
                  document to the downstream AI/ML service for further processing.
        """
        additional_metadata = self._build_additional_metadata(message)
        file_checksum = message.get("md5Checksum")
        file_size = int(message.get("size", -1))
        document_id = old_document["_id"]
        update_dict = {"lastModifiedTime": message["fileModDt"], "additionalMetadata": additional_metadata,
                       "trashed": message["trashed"], "skipped": message["skipped"],
                       "version": old_document["version"] + 1}
        if message["skipReason"]:
            update_dict["skipReason"] = message["skipReason"]

        if file_checksum == old_document["checksum"]:
            # This method is called after checking that the modified time of the current version of the file is
            # different than the one in the database record for the file. Although, the checksum has not changed,
            # we want to update the modified time along with any change in additional metadata fields and trash status
            # of the file.
            logger.info(f"Updating metadata for document {document_id} in the db")
            self._message_db_ops[DOCUMENT].append(UpdateOne({"_id": document_id}, {"$set": update_dict}))
            old_document.update(update_dict)
            return old_document, bool(old_document.get("processed"))

        file_path = message["filePath"]
        update_dict["checksum"] = file_checksum if file_checksum else ""
        update_dict["processed"] = False
        update_dict["scanned"] = True
        if file_size > 0:
            update_dict["size"] = file_size
        self._message_db_ops[DOCUMENT].append(UpdateOne({"_id": document_id}, {"$set": update_dict}))
        logger.info(f"Updated MongoDB document {document_id} and file {file_path}")

        if file_checksum:
            self._record_duplicates(document_id, file_checksum, CHECKSUM_TYPE)

        old_document.update(update_dict)
        return old_document, False

    def _handle_new_document_metadata(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Creates a new record in the MongoDB document collection, checks and records if the document is a duplicate
        and updates summary collections."""
        additional_metadata = self._build_additional_metadata(message)
        file_checksum = message.get("md5Checksum")
        mongo_document = {"name": message["filePath"],
                          "type": message["fileExtension"] if message["fileExtension"] in VALID_TYPES else "other",
                          "objectType": OBJ_TYPE_DOCUMENT,
                          "sharedBy": {"name": message["driveName"],
                                       "email": message["driveId"]},
                          "datasourceId": self._datasource_id,
                          "lastModifiedTime": message["fileModDt"],
                          "checksum": file_checksum if file_checksum else "",
                          "additionalMetadata": additional_metadata,
                          "checksumType": CHECKSUM_TYPE,
                          "processed": False,
                          "scanned": True,
                          "version": 1,
                          "trashed": message["trashed"],
                          "skipped": message["skipped"]}
        if message["skipReason"]:
            mongo_document["skipReason"] = message["skipReason"]
        file_size = int(message.get("size", -1))
        if file_size > 0:
            mongo_document["size"] = file_size
        else:
            file_data = self._get_file_data(message)
            mongo_document["size"] = file_data["fileSize"] if file_data else 0

        # This is the only operation which is happening outside the transaction. The reason is that we need the
        # document ID of the newly inserted document for further processing and the operations happening in the
        # transactions are bulk writes which give no option to extract ID for an individual record. In case of a
        # partial failure, meaning when we insert this document and subsequent operations fail, it should not be an
        # issue because consumer would try to reprocess the same message from the topic as the offset would not have
        # been committed and the document would be processed as an existing record and updates would only
        # happen on the metadata in case of a mismatch where idempotency is ensured.
        result = self._db[DOCUMENT].insert_one(mongo_document)

        document_id = result.inserted_id
        mongo_document["_id"] = document_id
        logger.info(f"Inserted MongoDB document {document_id} and file {mongo_document['name']}")

        if file_checksum:
            self._record_duplicates(document_id, file_checksum, CHECKSUM_TYPE)
        return mongo_document

    def _get_file_sharing_details(self, file_id: str, drive_id: str, drive_type: str) -> Dict[str, Any]:
        """Retrieves information about which all users, groups and domains that the file is shared with."""
        permissions = retrieve_permissions(self._drive, file_id, drive_type)
        file_sharing_details = {}
        if permissions:
            if drive_type == DRIVE_TYPE_USER:
                file_sharing_details = handle_permissions(permissions, self._config.domain, drive_id)
            else:
                file_sharing_details = handle_permissions(permissions, self._config.domain)
        return file_sharing_details

    def _build_additional_metadata(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Builds and returns a metadata dictionary which also includes file sharing details to be used in
        creating/updating record in MongoDB document collection."""
        file_id = message["id"]
        drive_type = message["driveType"]
        drive_id = message["driveId"]
        web_view_link = message["webViewLink"]
        drive_name = message["driveName"]

        # TODO (Keshav.G): Google Drive API returns the list of the parents. Check when this can be list and handle the
        #  case.

        additional_metadata = {"vendorItemId": file_id, "driveType": drive_type, "driveId": drive_id,
                               "driveName": drive_name, "objectLink": web_view_link,
                               "parentFolderSharedWithUser": bool(message.get("parentFolderSharedWithUser"))}
        if message.get("parents"):
            additional_metadata["folderId"] = message["parents"][0]

        if message.get("sharedDriveUserEmail"):
            additional_metadata["sharedDriveUserEmail"] = message["sharedDriveUserEmail"]
            self._drive = service_account_login_drive_with_subject(self._config, message["sharedDriveUserEmail"])
        else:
            self._drive = service_account_login_drive_with_subject(self._config, drive_id)
        file_sharing_details = self._get_file_sharing_details(file_id, drive_id, drive_type)
        additional_metadata.update(file_sharing_details)
        return additional_metadata

    def _should_skip_document(self, message: Dict[str, Any]) -> Document:
        """
        Checks if the document should be skipped for processing.
        Existing documents having the same modified time and trash status need no processing at all. Also, for existing
        documents, when modified time or trash status has changed, but there is no change in checksum, then only
        metadata is updated and further processing is skipped. New documents need to be processed as there exist no
        records for them in the system.
        """
        skip_document = False
        # Whether to update the skipped counter statistics. If a document has already been marked as skipped, we
        # should not be updating any of the skipped counters.
        # TODO: Since we create the document outside the transaction block, a failure after the creation
        # (but before the update of the skipped counter) will result in the skipped counters not getting propagated.
        is_new_document = False
        document = self._db[DOCUMENT].find_one({"datasourceId": self._datasource_id,
                                                "additionalMetadata.vendorItemId": message["id"]})

        # We need modified time for the document in datetime format for further processing. Since, the producer could
        # only write string serialized data on the Kafka topic, we convert the modified time from string to datetime
        # here in the consumer.
        message["fileModDt"] = datetime.strptime(message["modifiedTime"], "%Y-%m-%dT%H:%M:%S.%f%z")

        if document:
            self._prev_document = copy.deepcopy(document)
            self._prev_pii_records = list(self._db[PII_RECORD].find({"documentId": self._prev_document["_id"]}))
            document_last_modified_time = document["lastModifiedTime"].replace(tzinfo=pytz.UTC)
            trashed_status = document["trashed"]
            # It might happen that we update the metadata for the file but there is a failure in the later stages of
            # processing the document or the system crashes afterward. A document is marked as processed at the last
            # stage when its tika response is processed. Hence, in the case when document metadata has not changed,
            # before making a decision to skip further processing of the document, we also want to ensure that the
            # document was fully processed earlier.
            document_processed = document.get("processed")
            if (document_last_modified_time == message["fileModDt"] and trashed_status == message["trashed"] and
                    document_processed):
                # No metadata needs to be updated. This is the case when the same document record got flushed more than
                # once on the Kafka topic due to possible producer crashes.
                skip_document = True
            else:
                # Update file metadata and also check whether the file should be reprocessed based on whether the file's
                # content has changed.
                document, skip_document = self._handle_existing_document_metadata(document, message)
            self._current_document = copy.deepcopy(document)
        else:
            document = self._handle_new_document_metadata(message)
            self._current_document = copy.deepcopy(document)
            is_new_document = True

        if message["skipped"]:
            # This is the case when although the system does not support processing this file, the producer
            # has flushed the file on the Kafka topic so that we can record the metrics for such skipped files.
            if document.get("size"):
                # GDrive vendor API does not send size information for non-binary files. At this point of file
                # processing, we have not downloaded the file content and hence we do not know the size for such files.
                self._processed_bytes.labels(status_code=None, topic_name=self._topic_name,
                                             datasource_id=self._datasource_id, skipped=True,
                                             skip_reason=message["skipReason"]).inc(document["size"])
                push_to_gateway(PROMETHEUS_PUSH_GATEWAY_URL, job=self._datasource_type,
                                registry=self._prometheus_registry)
            skip_document = True

        return Document(document=document, skip_document=skip_document, is_new_document=is_new_document)

    def _handle_document_delete(self, vendor_item_id: str):
        """Handles the delete event received on a file from the vendor API. This change event is sent when the file is
        permanently deleted at the source."""
        document = self._db[DOCUMENT].find_one({"datasourceId": self._datasource_id,
                                                "additionalMetadata.vendorItemId": vendor_item_id})
        if not document:
            logger.info(f"Document with vendor item ID {vendor_item_id} not found or is already deleted")
            return

        self._prev_document = document
        document_id = document["documentId"] = document.pop("_id")
        document["datasourceType"] = self._datasource_type

        # Create a clone of the document record in the archived_document collection before deleting the record from the
        # document collection so that a background job can take care of handling its dependencies. With this approach
        # of having a backup collection, we avoid possible race conditions that could happen in other background jobs
        # like the entity resolution job if it is operating on the same document record.
        with SINGLETON_MONGO_CLIENT.start_session() as mongo_client_session:
            with mongo_client_session.start_transaction():
                self._db[ARCHIVED_DOCUMENT].insert_one(document, session=mongo_client_session)
                self._db[DOCUMENT].delete_one({"_id": document_id}, session=mongo_client_session)
        logger.info(f"Removed document {document_id} from MongoDB collection for delete event")

    def _should_rescan_document(self, document_id: str) -> Tuple[Optional[Dict[str, Any]], bool]:
        document = self._db[DOCUMENT].find_one({"_id": ObjectId(document_id),
                                                "$or": [{"annotation": None}, {"annotation": ""}],
                                                "type": DOCUMENT_ANNOTATION_TYPE_FILTER})
        if document:
            return document, True
        logger.info(f"Document {document_id} not found or is already classified")
        return None, False

    def _get_file_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Gets file content for the document in process and also returns the file size that can be updated for the
        document record in case the vendor API did not provide it."""
        file_data = {}
        file_path = message["filePath"]
        basename = file_data["basename"] = Path(file_path).name
        shared_drive_user_email = message.get("sharedDriveUserEmail")
        parent_folder_shared_with_user = bool(message.get("parentFolderSharedWithUser"))
        with tempfile.TemporaryDirectory() as tmp_dir_name:
            download_status, file_data["fileSize"] = self.download_file(
                file_path, tmp_dir_name, message["driveId"], shared_drive_user_email,
                message["trashed"], parent_folder_shared_with_user=parent_folder_shared_with_user)
            if not download_status:
                return {}
            file_extension = Path(basename).suffix.replace(".", "")
            if file_extension in LOG_FILE_EXTENSIONS:
                with open(tmp_dir_name / Path(basename)) as fh:
                    file_data["fileContent"] = fh.read(LOG_FILE_MAX_ALLOWED_CHARS)
            else:
                with open(tmp_dir_name / Path(basename), "rb") as fh:
                    file_data["fileContent"] = fh.read()
        return file_data

    @retry((requests.exceptions.Timeout, requests.exceptions.ConnectionError), tries=ML_SERVICES_RETRY_ATTEMPTS,
           delay=ML_SERVICES_RETRY_DELAY_SECONDS, backoff=2, jitter=(1, 10))
    def _process_file_data(self, document: Dict[str, Any], file_data: Dict[str, Any], is_new_document: bool):
        basename: str = file_data["basename"]
        try:
            if document["type"] in LOG_FILE_EXTENSIONS:
                response = process_log_file_request(message=file_data["fileContent"],
                                                    payload_extra_params={"object_name": basename})
                status_code = response.status_code
                response_json = response_json()
            else:
                status_code, response_json = send_file_to_tika(file_data["fileContent"], basename)
                if status_code != 200:
                    self.handle_tika_failure(basename)
                    return
                response_json = response.json()

            if not response.ok:
                self.handle_tika_failure(basename, request_error=None, response=response)
                return
        except Exception as e:
            request_error = f"Request failed due to {str(e)}"
            self.handle_tika_failure(basename, request_error=request_error, response=None)
            return

        self._processed_bytes.labels(status_code=status_code, topic_name=self._topic_name,
                                     datasource_id=self._datasource_id, skipped=False,
                                     skip_reason=None).inc(file_data["fileSize"])
        try:
            # Pushing to Prometheus gateway is best-effort so we suppress any failures that occur.
            push_to_gateway(PROMETHEUS_PUSH_GATEWAY_URL, job=self._datasource_type,
                            registry=self._prometheus_registry)
        except Exception:
            logger.exception("Unable to push fileSize metrics to Prometheus")

        logger.debug(f"Response for {basename}: {response_json}")

        with SINGLETON_MONGO_CLIENT.start_session() as mongo_session:
            with mongo_session.start_transaction():
                _, kafka_payload = self._tika_response_processor.process(
                    document, response_json, is_new_document=is_new_document, old_document=self._prev_document)

        self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME, str(self._datasource_id),
                                                       kafka_payload, synchronous=True)

    def handle_tika_failure(self, basename: str, request_error: Optional[str] = "",
                            response: Optional[requests.Response] = None):

        if request_error:
            msg = request_error
            skip_reason = TikaRequestFailedSkipReason
        else:
            msg = f"Unable to process {basename} with error code {response.status_code}: {response.text}"
            skip_reason = TikaProcessingFailedSkipReason

        logger.error(msg)
        if self._raise_request_failure_exception:
            raise Exception(msg)

        self._db[DOCUMENT].update_one({"_id": self._current_document["_id"]},
                                      {"$set": {"skipped": True, "skipReason": skip_reason}})
        self._current_document["skipped"] = True
        self._current_document["skipReason"] = skip_reason
        kafka_message_body = json.loads(json_util.dumps({
            "datasourceType": self._datasource_type,
            "document": self._current_document,
            "piiRecords": self._current_pii_records,
            "previousDocument": self._prev_document,
            "previousPiiRecords": self._prev_pii_records
        }))
        self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                       str(self._datasource_id),
                                                       kafka_message_body, synchronous=True)

    def handle_shutdown(self):
        """
        Handle shutdown of the consumer and stops the service.
        """
        self._consumer.close(autocommit=False)
        logger.info("Closed Kafka consumer")
        sys.exit(1)

    def _message_iterator(self) -> Iterable[ConsumerRecord]:
        # TODO: Handle case where we process where the consumer is in the middle of processing multiple files of
        #  same message.
        if SIGTERM_SHUTDOWN:
            self.handle_shutdown()
        while True:
            data = self._consumer.poll(timeout_ms=KAFKA_POLL_TIMEOUT, max_records=1)
            if not data:
                if SIGTERM_SHUTDOWN:
                    self.handle_shutdown()
                continue
            if len(data) != 1:
                raise ValueError(f"Found more than one key for a single message: {data}")
            for messages in data.values():
                if len(messages) != 1:
                    raise ValueError(f"Found multiple messages from poll: {messages}")
                yield messages[0]

    def consume_kafka_topic(self):
        """Processes messages written to the datasource's Kafka topic by the historical and live sync producer jobs."""
        self._create_rclone_instance()
        requests_session = requests.Session()
        retries = Retry(total=2,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])
        requests_session.mount(self._file_processor_request_endpoint, HTTPAdapter(max_retries=retries))

        for message in self._message_iterator():
            logger.info(f"Consuming message with key {message.key}")
            if SIGTERM_SHUTDOWN:
                self.handle_shutdown()
            message = message.value
            is_new_document = False
            self._current_document = None
            self._prev_document = None
            self._prev_pii_records = None
            self._current_pii_records = None

            # TODO: Check how we can optimise the rescan process by not sending the flag in each message.
            if message.get("rescan"):
                # This event is written by the rescan job for an existing document record.
                document, rescan_document = self._should_rescan_document(message["documentId"])
                if not rescan_document:
                    self._consumer.commit()
                    continue
            elif message.get("removed"):
                # This event is received when the document is deleted at source.
                self._handle_document_delete(message["id"])
                self._consumer.commit()
                continue
            else:
                document, skip_document, is_new_document = self._should_skip_document(message)
                if skip_document:
                    if self._current_document:
                        self._current_pii_records = list(self._db[PII_RECORD].find({
                            "documentId": self._current_document["_id"]}))
                    kafka_message_body = json.loads(json_util.dumps({
                        "datasourceType": self._datasource_type,
                        "document": self._current_document,
                        "piiRecords": self._current_pii_records,
                        "previousDocument": self._prev_document,
                        "previousPiiRecords": self._prev_pii_records
                    }))
                    self.process_db_ops_transaction()
                    self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                                   str(self._datasource_id),
                                                                   kafka_message_body, synchronous=True)
                    self._consumer.commit()
                    continue

            file_data = self._get_file_data(message)
            if not file_data.get("fileContent"):
                self._current_document["skipped"] = True
                self._current_document["skipReason"] = AttachmentDownloadFailedSkipReason
                self._message_db_ops[DOCUMENT].append(UpdateOne(
                    {"_id": self._current_document["_id"]},
                    {"$set": {"skipped": True,
                              "skipReason": AttachmentDownloadFailedSkipReason}}))
                kafka_message_body = json.loads(json_util.dumps({
                    "datasourceType": self._datasource_type,
                    "document": self._current_document,
                    "piiRecords": self._current_pii_records,
                    "previousDocument": self._prev_document,
                    "previousPiiRecords": self._prev_pii_records
                }))
                self.process_db_ops_transaction()
                self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                               str(self._datasource_id),
                                                               kafka_message_body, synchronous=True)
                self._consumer.commit()
                continue

            # GSuite API does not return file size for non-binary files. In such cases, we could not record the file
            # size in the document record and also did not increment the cumulative size for the drive record
            # associated with the document record.
            # Since, we now have downloaded the file using rclone and know its size, we update those.
            file_size = self._current_document["size"] = file_data["fileSize"]

            if self._prev_document:
                file_size -= self._prev_document["size"]

            if file_size != 0:
                self._message_db_ops[DOCUMENT].append(UpdateOne({"_id": self._current_document["_id"]},
                                                                {"$set": {"size": self._current_document["size"]}}))

            logger.debug(f"Firing MongoDB operations on {len(self._messag e_db_ops.keys())} collections")
            self.process_db_ops_transaction()

            self._process_file_data(self._current_document, file_data, is_new_document, requests_session)

            # Commit offset of the message in Kafka topic for the consumer.
            self._consumer.commit()

    def process_db_ops_transaction(self):
        """
        Processes all DB operations in the message_db_ops dictionary as a transaction.

        The list of DB operations is cleared after the transaction is committed.
        """
        if not self._message_db_ops:
            return
        with SINGLETON_MONGO_CLIENT.start_session() as mongo_session:
            with mongo_session.start_transaction():
                for collection, operations in self._message_db_ops.items():
                    self._db[collection].bulk_write(operations, session=mongo_session)
        self._message_db_ops.clear()


def main():
    signal.signal(signal.SIGTERM, sigterm_handler)
    datasource_id = ObjectId(os.getenv("DATASOURCE_ID"))
    consumer = GoogleDriveConsumer(datasource_id)
    consumer.consume_kafka_topic()


if __name__ == "__main__":
    main()
