"""
Module to consume files/email from a Kafka topic for the datasource.

Downloads the contents of the file/email and sends them to tika service for processing.
"""
import base64
import csv
import hashlib
import json
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Set
import requests
from bson import Int64, ObjectId, json_util
from document_utils.document import should_create_document
from googleapiclient.errors import HttpError
from kafka.consumer.fetcher import ConsumerRecord
from kafka_utils.consts import DATASOURCE_STATS_KAFKA_TOPIC_NAME
from kafka_utils.consumer import Consumer
from kafka_utils.producer import Producer
from lb_common.service_requests import send_message_to_presidio
from lb_common.utils import extract_domain_from_email
from logger_utils import logger
from mongo_utils import SINGLETON_MONGO_CLIENT
from mongo_utils.consts import (DATASOURCE, DOCUMENT, GMAIL_DATASOURCE, MAIL, OBJ_TYPE_DOCUMENT, OBJ_TYPE_MESSAGE,
                                TRANSLATE_FILE_TYPE, VIOLATION, PresidioProcessingFailedSkipReason,
                                PresidioRequestFailedSkipReason, TikaProcessingFailedSkipReason,
                                TikaRequestFailedSkipReason)
from mongo_utils.datasource_user_summary import update_mail_size_user_summary
from mongo_utils.identifier_manager import IdentifierManager
from prometheus_client import CollectorRegistry, Counter, push_to_gateway
from prometheus_utils.constants import PROMETHEUS_PUSH_GATEWAY_URL
from pymongo import UpdateOne
from requests.adapters import HTTPAdapter, Retry
from tika_utils.consts import VALID_TYPES
from tika_utils.v1.response_processor import TikaResponseProcessor
from vault_utils.vault_client import get_configuration
from common.config import Config
from common.consts import (DATASOURCE_ID, DELEGATED_CREDENTIAL_ENV, DOMAIN_ENV, FILE_MAX_SIZE_BYTES_VAL,
                           KAFKA_BOOTSTRAP_SERVERS_VAL, KAFKA_CONSUMER_OFFSET_VAL, KAFKA_MAX_POLL_INTERVAL_MS_VAL,
                           KAFKA_MAX_POLL_RECORDS_VAL, KAFKA_POLL_TIMEOUT, PROCESSED_BYTES_LABEL_NAMES)
from common.group_manager import SINGLETON_GROUP_MANAGER
from common.gsuite_utils import (check_for_mail_risk, contains_calendar_attachment, get_domain, get_name_and_email,
                                 service_account_login_gmail, update_mail_count_external_members,
                                 update_mail_count_group_summary, update_mail_count_user_summary)
from common.rclone_utils import process_rclone_env_variables
from src.common.consts import RecipientInfo
from src.common.gsuite_utils import contains_attachment

NOT_FOUND = 404
SIGTERM_SHUTDOWN = False
"""Whether the service has received the shutdown signal from SIGTERM."""


def sigterm_handler(signal: Any, frame: Any):
    """
    SIGTERM handler to ensure that the consumer does not immediately shutdown during processing of a message.
    """
    global SIGTERM_SHUTDOWN
    SIGTERM_SHUTDOWN = True


class GoogleEmailConsumer:
    def __init__(self, datasource: Dict[str, Any], config: Config):
        """
        Creates a Google email consumer object responsible for consuming message from the datasource topic and
        processing the file by downloading it and sending to the file handler service and writing the results to
        MongoDB.
        :param datasource: Datasource details.
        :param config: Gmail account config.
        """
        self._config = config
        self._mongo_client = SINGLETON_MONGO_CLIENT
        self._datasource_id = datasource["_id"]
        self._datasource_name = datasource["name"]
        self._datasource_location = datasource["location"]
        self._datasource_type = datasource["type"]

        self._kafka_topic_name = os.getenv("KAFKA_TOPIC_NAME")
        kafka_consumer = Consumer(KAFKA_BOOTSTRAP_SERVERS_VAL,
                                  KAFKA_CONSUMER_OFFSET_VAL,
                                  KAFKA_MAX_POLL_RECORDS_VAL,
                                  KAFKA_MAX_POLL_INTERVAL_MS_VAL,
                                  self._kafka_topic_name)
        self._consumer = kafka_consumer.consumer

        # Tika server request client config params.
        self._tika_url = os.getenv("TIKA_URL")
        self._tika_timeout = int(os.getenv("TIKA_REQUEST_TIMEOUT", 900))
        self._tika_response_processor = TikaResponseProcessor(self._datasource_id, self._datasource_name,
                                                              self._datasource_location, GMAIL_DATASOURCE)
        self._raise_request_failure_exception = os.getenv(
            "RAISE_TIKA_FAILURE_EXCEPTION", "False").lower() == "true"

        # Presidio server request client config params.
        self._presidio_url = os.getenv("PRESIDIO_URL")
        self._presidio_timeout = int(os.getenv("PRESIDIO_REQUEST_TIMEOUT", 900))
        self._raise_presidio_request_failure_exception = os.getenv(
            "RAISE_PRESIDIO_FAILURE_EXCEPTION", "False").lower() == "true"

        self._session = requests.Session()
        retries = Retry(total=2,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])
        self._session.mount(self._tika_url, HTTPAdapter(max_retries=retries))
        self._documents_to_be_marked_risky: List[UpdateOne] = []
        # Initialize singleton group manager in consumer to avoid unnecessary thread creation for other components.
        SINGLETON_GROUP_MANAGER.initialize(SINGLETON_MONGO_CLIENT)

        self._stats_aggregator_producer = Producer(self._config.kafka_bootstrap_servers)

        self._prometheus_registry = CollectorRegistry()
        self._processed_bytes = Counter("processed_bytes", "Google Mail Total Bytes Processed",
                                        labelnames=PROCESSED_BYTES_LABEL_NAMES,
                                        registry=self._prometheus_registry)
        self._mail_size = Int64(0)
        self._has_attachments = False
        self._document_ids: List[ObjectId] = []
        # Initialize change capture for identifier collection.
        self._identifier_manager = IdentifierManager()
        self._identifier_manager.initialize(SINGLETON_MONGO_CLIENT)
        self._mail_info: Dict[str, Any] = {}
        self._is_new_mail = False

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
        """
        This method consumes all the messages provided by gmail producer.
        """
        try:
            for message in self._message_iterator():
                logger.info(f"Consuming message with key {message.key}")
                if SIGTERM_SHUTDOWN:
                    self.handle_shutdown()
                message = message.value
                self._document_ids = []
                self._is_new_mail = False
                self._has_attachments = False
                # Resetting to 0 before processing each message, otherwise it will just keep on accumulating with each
                # message.
                self._mail_size = Int64(0)
                # Clearing document to be marked risky list before processing each message to avoid marking documents
                # risky from previous message.
                self._documents_to_be_marked_risky.clear()
                self.handle_msg(message)
                # Commit offset of the message in Kafka topic for the consumer.
                self._consumer.commit()

        except Exception as exc:
            logger.exception("Failed to process message")
            raise exc

    def handle_msg(self, kafka_message: Dict[str, Any]):
        """
        This method processes an email message.
         1. Checks for any pii data in an email message.
         2. Checks for any attachments in an email and send them to tika for PII detection.
        :param kafka_message: Message dict from kafka broker.
        """
        user_info = kafka_message["userInfo"]
        msg = kafka_message["message"]
        msg_id = msg["id"]
        logger.debug(f"Message info: {msg}, user_info: {user_info}")
        service = service_account_login_gmail(self._config, user_info["email"])

        try:
            msg_details = service.users().messages().get(userId="me", id=msg_id).execute()
            logger.debug(f"Message details: {msg_details}")
        except HttpError as ex:
            if ex.resp.status == NOT_FOUND:
                logger.info(f"Skipping message: {msg_id} unable to fetch details.")
                return
            raise ex

        label_ids = msg_details.get("labelIds")
        msg_id = msg_details["id"]
        email = user_info["email"]
        mail_time = datetime.fromtimestamp(int(msg_details["internalDate"]) / 1000, timezone.utc)

        if not label_ids or "DRAFT" in label_ids:
            # Note: In case of draft, some headers are not present. So we need to consider that while
            # implementing consumer for drafts.
            # TODO (Keshav.G) : Add support for drafts.
            logger.info(f"Skipping message {msg_id} with label {label_ids} for user {email}")
            return

        header_info = {}
        header_keys = ["To", "From", "Subject", "Date", "Message-Id", "Bcc", "Cc"]
        for header in msg_details["payload"]["headers"]:
            if header["name"] in header_keys:
                header_info[header["name"]] = header["value"]
        logger.debug(f"Header info: {header_info}")

        additional_metadata = {
            "threadId": msg_details["threadId"],
            "messageId": msg_id,
            "mailboxId": email,
            "postTime": mail_time
        }
        if header_info.get("Message-Id"):
            additional_metadata["globalMessageId"] = header_info["Message-Id"]

        sender_info = get_name_and_email(header_info["From"])
        org_domain = extract_domain_from_email(sender_info["email"])
        # Merging receiver info from 'To', 'Bcc' and 'Cc'.
        recipient_info = self.get_recipients(
            header_info.get("To", "") + header_info.get("Cc", "") + header_info.get("Bcc", ""), org_domain)
        recipients = recipient_info.recipients

        if not recipients:
            logger.error(f"Skipping not able to extract recipient info for msg {msg_details}")
            return

        additional_metadata["sharedExternally"] = recipient_info.shared_externally
        additional_metadata["sharedInternally"] = recipient_info.shared_internally

        self._mail_info = {
            "threadId": msg_details["threadId"],
            "messageId": msg_details["id"],
            "datasourceId": self._datasource_id,
            "sender": sender_info,
            "recipients": recipients,
            "subject": header_info.get("Subject", ""),
            "timestamp": mail_time
        }
        logger.debug(f"Mail info: {self._mail_info}")

        parts = msg_details["payload"].get("parts", [])
        self._has_attachments = contains_attachment(parts)
        if self._has_attachments:
            mail_result = SINGLETON_MONGO_CLIENT[MAIL].update_one(
                {"messageId": msg_details["id"], "datasourceId": self._datasource_id},
                {"$set": self._mail_info}, upsert=True)
            if mail_result.upserted_id:
                self._is_new_mail = True

        has_calendar_attachment = contains_calendar_attachment(parts)
        if has_calendar_attachment:
            logger.info(f"{msg_id} has a calendar attachment")
        pii_records = self.process_msg_parts(msg_id, [msg_details["payload"]], additional_metadata,
                                             sender_info, service, not has_calendar_attachment)
        pii_attributes = self.get_pii_attributes_from_pii_records(pii_records)

        SINGLETON_MONGO_CLIENT[MAIL].update_one({"messageId": msg["id"], "datasourceId": self._datasource_id},
                                                {"$set": {"size": int(self._mail_size),
                                                          "documentIds": self._document_ids,
                                                          "hasAttachments": self._has_attachments}})

        # Email with no PII attribute and no attachments is not needed to be recorded.
        if not pii_attributes and not self._has_attachments:
            # TODO: Need to figure out if the same email is sent mail is sent again count should not be updated here.
            # Updating mail count in group summary if present in recipient list.
            update_mail_count_group_summary(recipients, self._datasource_id)
            update_mail_count_user_summary(email, self._datasource_id)
            update_mail_size_user_summary(email, self._datasource_id, self._mail_size)
            update_mail_count_external_members(recipients, self._datasource_id)
            # Since pii attribute list is empty, there is no point in checking for risk and no need for creating
            # a new mail record.
            return

        foreign_recipients = check_for_mail_risk(recipients, header_info["From"], self._datasource_id)
        foreign_recipient_domains = set([get_domain(foreign_recipient) for foreign_recipient in foreign_recipients])

        # Check for risks.
        if foreign_recipient_domains:
            pii_record_ids = [pii_record["_id"] for pii_record in pii_records]
            self.create_violation_record(sender_info, foreign_recipient_domains, pii_record_ids, pii_attributes,
                                         mail_time)
            risk = "high"
            if self._documents_to_be_marked_risky:
                # Mark all the documents risky for the current mail that contained PII data.
                SINGLETON_MONGO_CLIENT[DOCUMENT].bulk_write(self._documents_to_be_marked_risky)
        else:
            risk = "low"
        logger.debug(f"pii attributes : {pii_attributes}")

        # Update MongoDB for the email collection.
        update_mail_info = {
            "risk": risk,
            "foreignRecipientCount": len(foreign_recipients),
            "piiAttributesCount": len(pii_attributes)
        }
        logger.debug(f"Update Mail info: {update_mail_info}")
        mail_result = SINGLETON_MONGO_CLIENT[MAIL].update_one(
            {"messageId": msg_details["id"], "datasourceId": self._datasource_id},
            {"$set": update_mail_info}, upsert=True)
        if self._is_new_mail:
            # Check if new record is inserted then only update mail count.
            if not pii_attributes:
                update_mail_count_user_summary(email, self._datasource_id)
                update_mail_count_external_members(recipients, self._datasource_id)
            else:
                update_mail_count_user_summary(email, self._datasource_id, is_sensitive=True,
                                               is_risky=bool(foreign_recipient_domains))
                update_mail_count_external_members(recipients, self._datasource_id, is_sensitive=True)
            update_mail_count_group_summary(recipients, self._datasource_id)
            update_mail_size_user_summary(email, self._datasource_id, self._mail_size)

    def create_violation_record(self, sender_info: Dict[str, Any], foreign_recipient_domains: Set[str],
                                pii_record_ids: List[str], pii_attributes: Set[str],
                                post_time: datetime):
        """
        Helper method to create a violation record.
        :param sender_info: Senders info.
        :param foreign_recipient_domains: Set of domains of people out side org.
        :param pii_record_ids: List of pii_record ids.
        :param pii_attributes: List of Pii attributes.
        :param post_time: Time at which email was posted.
        :return Dict: A violation record.
        """
        sender_id = sender_info["email"]
        if sender_info.get("name"):
            sender_name = sender_info["name"]
        else:
            sender_name = sender_info["email"]
        receivers = [{"name": domain} for domain in foreign_recipient_domains]

        record = {
            "datasourceId": self._datasource_id,
            "senderId": sender_id,
            "senderName": sender_name,
            "receivers": receivers,
            "piiRecordIds": pii_record_ids,
            "piiAttributes": list(pii_attributes),
            "dateTime": post_time
        }
        logger.debug(f"Violation record: {record}")

        violation = SINGLETON_MONGO_CLIENT[VIOLATION].insert_one(record)

        violation_id = violation.inserted_id
        logger.debug(f"Inserted violation entry with id: {violation_id}")

    @staticmethod
    def get_recipients(recipients_str: str, sender_domain: str) -> RecipientInfo:
        """
        This method creates a RecipientInfo namedtuple from a string containing all recipients.
        :param recipients_str: String containing recipient information.
        :param sender_domain: Domain of the sender.
        :return: NamedTuple RecipientInfo having recipients and boolean indicating if it contains external/internal
        recipients.
        """
        recipients = []
        shared_externally = False
        shared_internally = False
        recipients_str = next(csv.reader([recipients_str], delimiter=",", quotechar='"'))
        for recipient_str in recipients_str:
            recipient_dict = get_name_and_email(recipient_str)
            if "email" not in recipient_dict:
                logger.error(f"Unable to extract email from {recipient_str} in {recipients_str}")
                continue
            if extract_domain_from_email(recipient_dict["email"]) == sender_domain:
                recipient_dict["isInternal"] = True
                shared_internally = True
            else:
                recipient_dict["isInternal"] = False
                shared_externally = True
            recipients.append(recipient_dict)
        return RecipientInfo(recipients, shared_internally, shared_externally)

    def process_msg_parts(self, msg_id: str, msg_parts: List[Dict[str, Any]],
                          additional_metadata: Dict[str, Any], sender_info: Dict[str, Any], service: Any,
                          consider_text: True) -> List[Any]:
        """
        This method recursively process all the message parts for a message.
        :param msg_id: ID of the message.
        :param msg_parts: List of message parts.
        :param additional_metadata: Additional metadata needed for creating document.
        :param sender_info: Info of sender.
        :param service: Google mail api service instance.
        :param consider_text: Whether text has to be processed (can be False if we have determined that there is a
        calendar attachment).
        :return: List of pii records.
        """
        pii_records = []
        for msg_part in msg_parts:
            if msg_part.get("parts"):
                part_pii_records = self.process_msg_parts(msg_id, msg_part["parts"], additional_metadata, sender_info,
                                                          service, consider_text)
                pii_records.extend(part_pii_records)

            if msg_part.get("filename"):
                # This part of the message is attachment.
                file_name = msg_part["filename"]
                if msg_part["body"]["size"] > 0:
                    size = msg_part["body"]["size"]
                    if size > FILE_MAX_SIZE_BYTES_VAL:
                        logger.info(f"Skipping file {file_name} as size exceeds the max file size limit: {size}")
                        msg_part["skipped"] = True
                        msg_part["skipReason"] = "Size"
                    attachment_pii_records = self.handle_attachments(msg_part, additional_metadata, sender_info,
                                                                     service)
                    pii_records.extend(attachment_pii_records)

            elif msg_part["mimeType"] == "text/plain":
                # This part of the message is text.
                if msg_part["body"]["size"] > 0:
                    if not consider_text:
                        logger.info(f"Skipping text for message {msg_id}")
                    else:
                        text_pii_records = self.handle_text(msg_part, additional_metadata, sender_info)
                        pii_records.extend(text_pii_records)

        # We push metrics just once after processing all parts of a message instead of once for each part.
        push_to_gateway(PROMETHEUS_PUSH_GATEWAY_URL, job=self._datasource_type, registry=self._prometheus_registry)
        logger.debug(f"pii records found : {pii_records}")
        return pii_records

    def handle_attachments(self, msg_part: Dict[str, Any], additional_metadata: Dict[str, Any],
                           sender_info: Dict[str, Any],
                           service: Any) -> List[Any]:
        """
        This method fetches an attachment and does PII detection.
        :param msg_part: Message part which contains attachment.
        :param additional_metadata: Additional metadata need for document creation.
        :param sender_info: Info of sender.
        :param service: Google Api service instance.
        :return: List of PII records.
        """
        global_attachment_id = ""
        for header in msg_part.get("headers", []):
            if header["name"] == "X-Attachment-Id":
                global_attachment_id = header["value"]

        if not global_attachment_id:
            # If global attachment id is missing, then skip the image because, it a attachment that is not attached
            # added by user. Usually these image are some icons.
            file_name = msg_part["filename"]
            logger.warning(f"Global attachment id missing for file : {file_name}")
            return []

        basename, filename_ext = os.path.splitext(msg_part["filename"])
        filename_ext = filename_ext[1:].lower()

        # Translating the file type to undefined formats.
        # example : jpeg -> jpg
        filename_ext = TRANSLATE_FILE_TYPE.get(filename_ext, filename_ext)
        if filename_ext not in VALID_TYPES:
            logger.info(f"Skipping unsupported {filename_ext} for : {basename}")
            filename_ext = "other"
            msg_part["skipped"] = True
            msg_part["skipReason"] = "Type"

        attachment_id = msg_part["body"]["attachmentId"]
        additional_metadata["fileName"] = msg_part["filename"]
        additional_metadata["attachmentId"] = attachment_id
        document_name = f"{basename}_{global_attachment_id}"
        size = msg_part["body"]["size"]
        file_content = None
        updated_data = {
            "name": document_name,
            "type": filename_ext,
            "objectType": OBJ_TYPE_DOCUMENT,
            "sharedBy": sender_info,
            "datasourceId": self._datasource_id,
            "size": size,
            "additionalMetadata": additional_metadata,
            # Note: Currently setting lastModifiedTime as post time. But there is no concept of modify timestamp
            #       in Gmail.
            "lastModifiedTime": additional_metadata["postTime"],
            "skipped": msg_part.get("skipped", False),
        }
        if updated_data["skipped"]:
            updated_data["skipReason"] = msg_part["skipReason"]
        else:
            # Fetch attachment.
            attachment_info = service.users().messages().attachments().get(userId="me",
                                                                           messageId=additional_metadata["messageId"],
                                                                           id=attachment_id).execute()
            file_content = base64.urlsafe_b64decode(attachment_info["data"].encode("UTF-8"))
            hashes = hashlib.sha1()
            hashes.update(file_content)
            checksum = hashes.hexdigest()
            updated_data.update({
                "checksum": checksum,
                "checksumType": "SHA-1"
            })

        document = {
            "$set": updated_data,
            "$inc": {"version": 1}
        }
        logger.debug(f"Document info : {document}")
        # Create document with a unique attachment id in order to avoid redundant document creation.
        document = SINGLETON_MONGO_CLIENT[DOCUMENT].update_one(
            {"datasourceId": self._datasource_id, "additionalMetadata.attachmentId": attachment_id},
            document, upsert=True)
        if document.upserted_id:
            self._document_ids.append(document.upserted_id)
            self._mail_size += size

        if updated_data["skipped"]:
            self._processed_bytes.labels(status_code=None, topic_name=self._kafka_topic_name,
                                         datasource_id=self._datasource_id, skipped=True,
                                         skip_reason=updated_data["skipReason"]).inc(size)
            current_document = SINGLETON_MONGO_CLIENT[DOCUMENT].find_one({
                "datasourceId": self._datasource_id,
                "additionalMetadata.attachmentId": attachment_id})
            kafka_message_body = json.loads(json_util.dumps({
                "datasourceType": self._datasource_type,
                "document": current_document
            }))
            self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                           str(self._datasource_id),
                                                           kafka_message_body, synchronous=True)
            return []

        # Process the document.
        # TODO (Keshav.G): Handle session timeout gracefully and add exponential try.
        request_error = ""
        try:
            response = self._session.post(self._tika_url, data=file_content,
                                          headers={"Content-type": "application/octet-stream",
                                                   "X-File-Name": basename}, timeout=self._tika_timeout)
        except Exception as e:
            request_error = f"Request failed due to {str(e)}"

        if request_error or response.status_code != 200:
            if request_error:
                msg = request_error
                skip_reason = TikaRequestFailedSkipReason
            else:
                msg = f"Unable to process {basename} with error code {response.status_code}: {response.text}"
                skip_reason = TikaProcessingFailedSkipReason
            logger.error(msg)
            if self._raise_request_failure_exception:
                raise Exception(msg)
            document = SINGLETON_MONGO_CLIENT[DOCUMENT].update_one({"datasourceId": self._datasource_id,
                                                                    "additionalMetadata.attachmentId": attachment_id},
                                                                   {"$set": {"skipped": True,
                                                                             "skipReason": skip_reason}})
            if document.upserted_id:
                self._mail_size += size
            self._processed_bytes.labels(status_code=response.status_code, topic_name=self._kafka_topic_name,
                                         datasource_id=self._datasource_id, skipped=True,
                                         skip_reason=skip_reason).inc(size)
            current_document = SINGLETON_MONGO_CLIENT[DOCUMENT].find_one({
                "datasourceId": self._datasource_id,
                "additionalMetadata.attachmentId": attachment_id})
            kafka_message_body = json.loads(json_util.dumps({
                "datasourceType": self._datasource_type,
                "document": current_document
            }))
            self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                           str(self._datasource_id),
                                                           kafka_message_body, synchronous=True)
            return []

        response_json: Dict[str, Any] = response.json()
        logger.debug(f"Response for {basename}: {response_json}\n"
                     f"Time taken to process {basename}: {int(response.elapsed.total_seconds() * 1000)} ms")
        self._processed_bytes.labels(status_code=response.status_code, topic_name=self._kafka_topic_name,
                                     datasource_id=self._datasource_id, skipped=False,
                                     skip_reason=None).inc(size)

        document = SINGLETON_MONGO_CLIENT[DOCUMENT].find_one({"name": document_name,
                                                              "datasourceId": self._datasource_id})
        pii_records, aggregator_kafka_payload = self._tika_response_processor.process(document, response_json)
        self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                       str(self._datasource_id), aggregator_kafka_payload,
                                                       synchronous=True)
        if pii_records:
            self._documents_to_be_marked_risky.append(UpdateOne({"_id": document["_id"]}, {"$set": {"atRisk": True}}))

        return pii_records

    def handle_text(self, msg_part: Dict[str, Any], additional_metadata: Dict[str, Any],
                    sender_info: Dict[str, Any]) -> List[Any]:
        """
        This method handles pii data detection in a text message.
        :param msg_part: Part of message which contains the text.
        :param additional_metadata: Additional metadata needed for creating document.
        :param sender_info: Info of sender.
        :return: List of pii records.
        """
        # TODO (Keshav.G): Currently assuming English text. Check what is the behaviour in-case of different languages.
        data = msg_part["body"]["data"]
        size = msg_part["body"]["size"]
        email_text = base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8")
        email = additional_metadata["mailboxId"]
        message_id = additional_metadata["messageId"]
        part_id = msg_part["partId"]
        name = f"{email}_{message_id}_{part_id}"

        document = {
            "$set": {
                "name": name,
                "type": "message",
                "objectType": OBJ_TYPE_MESSAGE,
                "sharedBy": sender_info,
                "datasourceId": self._datasource_id,
                "size": size,
                "additionalMetadata": additional_metadata,
                # Note: Currently setting lastModifiedTime as post time. But there is no concept of modify timestamp
                #       in Gmail.
                "lastModifiedTime": additional_metadata["postTime"]
            },
            "$inc": {"version": 1}
        }
        try:
            status_code, response_json = send_message_to_presidio(message=email_text, sender_info=sender_info)
        except Exception as e:
            status_code = 500  # For exception setting status_code to 500.
            request_error = f"Request failed due to {str(e)}"
        if status_code != 200 or request_error:
            if request_error:
                msg = request_error
                skip_reason = PresidioRequestFailedSkipReason
            else:
                msg = (f"Presidio server request for message: {email_text} failed with "
                       f"status_code: {status_code}, response_body: {response_json}")
                skip_reason = PresidioProcessingFailedSkipReason
            logger.error(msg)
            if self._raise_presidio_request_failure_exception:
                raise Exception(msg)
            document["$set"].update({"skipped": True, "skipReason": skip_reason})
            if self._has_attachments:
                document_res = SINGLETON_MONGO_CLIENT[DOCUMENT].update_one(
                    {"datasourceId": self._datasource_id, "additionalMetadata.mailboxId": email,
                     "additionalMetadata.messageId": message_id, "additionalMetadata.partId": part_id},
                    document, upsert=True)
                if document_res.upserted_id:
                    self._document_ids.append(document_res.upserted_id)
                    self._mail_size += size
                self._processed_bytes.labels(status_code=status_code, topic_name=self._kafka_topic_name,
                                             datasource_id=self._datasource_id, skipped=True,
                                             skip_reason=document["skip_reason"]).inc(size)
                current_document = SINGLETON_MONGO_CLIENT[DOCUMENT].find_one({
                    "datasourceId": self._datasource_id, "name": name})
                kafka_message_body = json.loads(json_util.dumps({
                    "datasourceType": self._datasource_type,
                    "document": current_document
                }))
                self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                               str(self._datasource_id),
                                                               kafka_message_body, synchronous=True)
            return []

        logger.debug(f"Presidio response: {response_json}")

        # If the email body does not contains any PII records and if there are no attachments, we don't want to
        # record such emails.
        if not should_create_document(self._identifier_manager.identifier_details_dict,
                                      response_json) and not self._has_attachments:
            return []

        mail_result = SINGLETON_MONGO_CLIENT[MAIL].update_one(
            {"messageId": self._mail_info["messageId"], "datasourceId": self._datasource_id},
            {"$set": self._mail_info}, upsert=True)
        if mail_result.upserted_id:
            self._is_new_mail = True

        # Create document with a unique mailboxId/messageId/threadId in order to avoid redundant document creation.
        document = SINGLETON_MONGO_CLIENT[DOCUMENT].update_one(
            {"datasourceId": self._datasource_id, "additionalMetadata.mailboxId": email,
             "additionalMetadata.messageId": message_id, "additionalMetadata.partId": part_id},
            document, upsert=True)
        if document.upserted_id:
            self._document_ids.append(document.upserted_id)
            self._mail_size += size
        document = SINGLETON_MONGO_CLIENT[DOCUMENT].find_one({"name": name, "datasourceId": self._datasource_id})
        pii_records, aggregator_kafka_payload = self._tika_response_processor.process(document, response_json)
        self._stats_aggregator_producer.write_to_kafka(DATASOURCE_STATS_KAFKA_TOPIC_NAME,
                                                       str(self._datasource_id), aggregator_kafka_payload,
                                                       synchronous=True)

        if pii_records:
            self._documents_to_be_marked_risky.append(
                UpdateOne({"_id": document["_id"]}, {"$set": {"atRisk": True}}))

        # TODO (Kasim Sharif): Need to check if we want to record processed bytes only when new document entry
        #  is created.
        self._processed_bytes.labels(status_code=status_code, topic_name=self._kafka_topic_name,
                                     datasource_id=self._datasource_id, skipped=False,
                                     skip_reason=None).inc(size)
        return pii_records

    def get_pii_attributes_from_pii_records(self, pii_records: List[Dict[str, Any]]) -> Set[str]:
        """
        This method fetches all of the pii_attributes from all PII records.
        :param pii_records: Pii record dictionary.
        :return: Set of all identifiers in the pii record.
        """
        pii_attributes = set()
        for pii_record in pii_records:
            pii_attributes.update(set(pii_record["piiData"]).union(pii_record.get("dynamicPiiData", {})))
        return pii_attributes


def main():
    signal.signal(signal.SIGTERM, sigterm_handler)
    datasource = SINGLETON_MONGO_CLIENT[DATASOURCE].find_one({"_id": DATASOURCE_ID})
    datasource_config = get_configuration()
    # TODO (keshav.G): remove this process_rclone_env_variables instead create new method that use datasource_config.
    service_account_file = process_rclone_env_variables(DATASOURCE_ID)

    config = Config(datasource_id=DATASOURCE_ID,
                    service_account_file=service_account_file,
                    delegated_credential=os.getenv(DELEGATED_CREDENTIAL_ENV, {}),
                    domain=os.getenv(DOMAIN_ENV),
                    kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_VAL,
                    account_type=datasource_config["accountType"])

    consumer = GoogleEmailConsumer(datasource, config)
    consumer.consume_kafka_topic()


if __name__ == "__main__":
    main()
