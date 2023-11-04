import argparse
import sys
import boto3
import time
import json
import logging

logger = logging.getLogger('consumerLogger')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler = logging.FileHandler('consumer.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def consume():
    parser = argparse.ArgumentParser(
        prog='Consumer.py',
        description='Consumes Widget requests from given s3 bucket',
        epilog='Use ctrl-c to exit program.'
    )
    parser.add_argument('-rb', '--read_bucket')
    parser.add_argument('-wb', '--write_bucket')
    parser.add_argument('-wt', '--write_table')
    parser.add_argument('-rq', '--read_queue')

    args = parser.parse_args()

    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')
    logger.debug("Program Start")
    try:
        while True:
            request_cache = get_request_cache(s3, sqs, args)
            for request in request_cache:
                process_request(s3, request, args)
            else:
                time.sleep(.1)
    except KeyboardInterrupt:
        logger.debug("Program exited on keyboard interrupt")
        sys.exit(0)


def get_request_cache(s3, sqs, args):
    key_cache = []
    # When reading from bucket
    if args.read_bucket is not None:
        response = s3.list_objects_v2(
            Bucket=args.read_bucket,
            MaxKeys=1
        )
        contents = response.get('Contents')
        if contents is not None:
            logger.debug("Retrieved key from AWS s3")
            key = contents[0].get('Key')
            request = s3.get_object(
                Bucket=args.read_bucket,
                Key=key
            )
            s3.delete_object(
                Bucket=args.read_bucket,
                Key=key
            )
            logger.debug(f"found request {request.get('requestId')} in bucket {args.read_bucket}")
            key_cache.append(request)
            return key_cache
        logger.warning("No objects found in s3 bucket.")
        return key_cache
    # When reading from queue
    elif args.read_queue is not None:
        response = sqs.receive_message(
            QueueUrl=args.read_queue,
            AttributeNames=[
                'All'
            ],
            MessageAttributeNames=[
                'string'
            ],
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1.5
        )
        delete_batch = []
        for each in response.get("Messages"):
            delete_batch.append(
                {
                    'Id': each.get('MessageId'),
                    'ReceiptHandle': each.get('ReceiptHandle')
                }
            )
            key_cache.append(each.get('Body'))
        sqs.delete_message_batch(
            QueueUrl=args.read_queue,
            Entries=delete_batch
        )
        logger.debug(f"Retrieved {len(key_cache)} requests from sqs queue.")
        return key_cache


def process_request(client, request, args):
    request = json.loads(request.get("Body").read())
    request_type = request.get('type')
    if request_type == 'create':
        if args.write_bucket is not None:
            create_widget_s3(client, request, args)
        if args.write_table is not None:
            create_widget_dynamo(request, args)
    if request_type == "delete":
        if args.write_bucket is not None:
            delete_widget_s3(client, request, args)
        if args.write_table is not None:
            delete_widget_dynamo(request, args)
        return None
    if request_type == "update":
        if args.write_bucket is not None:
            create_widget_s3(client, request, args)
        # Todo handle update requests in future
        return None



def create_widget_dynamo(request_dictionary, args):
    widget = {
        'id': request_dictionary.get('widgetId'),
        'owner': request_dictionary.get('owner'),
        'label': request_dictionary.get('label'),
        'description': request_dictionary.get('description'),
    }
    for attribute in request_dictionary.get('otherAttributes'):
        widget[attribute.get('name')] = attribute.get('value')
    # I modified this from a code snippet found here
    # https://medium.com/@NotSoCoolCoder/handling-json-data-for-dynamodb-using-python-6bbd19ee884e
    boto3.resource('dynamodb').Table(args.write_table).put_item(Item=widget)
    logger.debug(f"Widget saved to dynamodb table {args.write_table}.")


def delete_widget_dynamo(request, args):
    boto3.client('dynamodb').delete_item(
        TableName=args.write_table,
        Key={"id": request.get("requestId")}
    )
    logger.debug("Widget deleted from dynamodb table.")

def update_widget_dynamo(request_dictionary, args):
    boto3.client('dynamodb').update_item(
        TableName=args.write_table,
        Key={"id": request_dictionary.get("requestId")},
        update_expression="Set size ="
    )


def create_widget_s3(client, request_dictionary, args):
    owner = request_dictionary.get('owner').lower().replace(' ', '-')
    widget_id = request_dictionary.get('widgetId')
    request_json = json.dumps(request_dictionary)
    client.put_object(
        Body=request_json,
        Bucket=args.write_bucket,
        Key=f"widgets/{owner}/{widget_id}"
    )
    logger.debug(f"Widget saved to s3 bucket {args.write_bucket}.")


def delete_widget_s3(client, request, args):
    owner = request.get('owner').lower().replace(' ', '-')
    widget_id = request.get('widgetId')
    client.delete_object(
        Bucket=args.write_bucket,
        Key=f"Widgets/{owner}/{widget_id}"
    )
    logger.debug(f"Widgets/{owner}/{widget_id} deleted from bucket.")


if __name__ == '__main__':
    consume()
