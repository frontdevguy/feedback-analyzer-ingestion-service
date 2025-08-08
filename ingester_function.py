"""
AWS Lambda ingester function.
"""

# Import config first to ensure environment variables and secrets are set up
import config
import json
import logging
from typing import Dict, Any
from urllib.parse import parse_qs, unquote_plus
from handlers.post_webhooks import handler as webhook_handler
from logger_setup import get_logger

# Configure logging
logger = get_logger("ingester")
logger.logger.setLevel(logging.INFO)


def ingester_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function.

    Args:
        event: The event dict that contains the request data
        context: The context object that contains information about the runtime

    Returns:
        Dict containing the response with statusCode, headers, and body
    """
    try:
        if event.get("source") == "aws.events":
            logger.info("Received warm-up event from CloudWatch")
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps({"message": "Warmed up!"}),
            }
            
        http_method = event.get("httpMethod", "UNKNOWN")
        path = event.get("path", "/")

        logger.info(
            "Processing request", http_method=http_method, path=path, event=event
        )

        body = event.get("body")
        
        # Process the request based on HTTP method and path
        if http_method == "POST" and path == "/webhooks":
            response_data = webhook_handler(event, body)
        else:
            response_data = {
                "message": f"Method {http_method} not implemented",
                "path": path,
            }

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
            },
            "body": json.dumps(response_data),
        }

    except Exception as e:
        logger.error(
            "Error processing request",
            error=e,
            http_method=event.get("httpMethod", "UNKNOWN"),
            path=event.get("path", "/"),
        )

        # Return error response
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps({"error": "Internal server error", "message": str(e)}),
        }
