import boto3
import json
import logging

logger = logging.getLogger('consumerLogger')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('consumer.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def handle_api_request(event, context):
    if is_post(event):
        response = create_request(event)
    else:
        response = generate_bad_method_response()

    return response


def is_post(event):
    if event["httpMethod"] == "POST":
        logger.debug("Received POST Request, from API.")
        return True
    logger.error(f"Invalid http request method {event['httpMethod']}")
    return False


def generate_bad_method_response():
    return {
        "statusCode": 400,
        "headers": {
            "Content-Type": "application/json",
        },
        "body": json.dumps({
            "error": {
                "message": "Failed to handle the request"
            }
        })
    }


def create_request(event):
    if valid_body(event):
        client = boto3.client("sqs")
        client.send_message(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/453835586573/cs5260-requests",
            MessageBody=json.dumps(event["body"])
        )
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
            },
            "body": json.dumps({
                "status": "success",
                "message": "Widget request sent to SQS."
            })
        }


def valid_body(event):
    body = event["body"]
    required = ["requestId", "widgetId", "owner", "otherAttributes"]
    if body["type"] not in ["create", "delete", "update"]:
        logger.error(f"{body['type']} not a valid request type")
        return False
    for parameter in required:
        if parameter not in body:
            log_missing_required_parameter(parameter)
            return False


def log_missing_required_parameter(parameter):
    logger.error(f"missing required parameter {parameter}.")
