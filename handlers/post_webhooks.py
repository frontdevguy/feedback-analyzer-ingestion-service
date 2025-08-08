"""
Webhook handler for POST /webhooks requests.
"""

from typing import Dict, Any
import uuid
from config import sqs_client, SQS_QUEUE_URL
from logger_setup import get_logger

# Get logger
logger = get_logger("webhook_handler")


def handler(event: Dict[str, Any], body: Any) -> Dict[str, Any]:
    """
    Handle POST /webhooks requests.

    Args:
        event: The full Lambda event dict
        body: The parsed request body

    Returns:
        Dict containing the response data
    """
    logger.info(
        "Processing webhook request",
        event_type="webhook_received",
        body=body,
        event=event,
    )

    if not SQS_QUEUE_URL:
        error_msg = "SQS_QUEUE_URL environment variable not set"
        logger.error(error_msg)
        return {"statusCode": 500, "body": {"error": error_msg}}

    try:
        # Generate a unique deduplication ID
        deduplication_id = str(uuid.uuid4())

        # Send message to SQS queue
        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=body,
            MessageGroupId="webhook_group",  # Group ID for FIFO queue
            MessageDeduplicationId=deduplication_id,  # Unique ID to prevent duplicates
        )

        logger.info(
            "Successfully sent message to queue"
        )

        return {
            "statusCode": 200,
            "message": "Webhook received and queued"
        }

    except Exception as e:
        error_msg = f"Failed to send message to queue: {str(e)}"
        logger.error(error_msg, event_type="queue_error", error=str(e))
        return {"statusCode": 500, "body": {"error": error_msg}}
