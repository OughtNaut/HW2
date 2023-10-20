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
        epilog=''
    )
    parser.add_argument('-rb', '--read_bucket')
    parser.add_argument('-wb', '--write_bucket')
    parser.add_argument('-wt', '--write_table')

    args = parser.parse_args()

    s3 = boto3.client('s3')
    logger.debug("Program Start")
    try:
        while True:
            request_key = get_single_key(s3, args)
            if request_key is not None:
                logger.debug(f"found key {request_key} in bucket {args.read_bucket}")
                process_request(s3, request_key, args)
            else:
                time.sleep(.1)
    except KeyboardInterrupt:
        logger.debug("Program exited on keyboard interrupt")
        sys.exit(0)


def get_single_key(client, args):
    response = client.list_objects_v2(
        Bucket=args.read_bucket,
        MaxKeys=1
    )
    contents = response.get('Contents')
    if contents is not None:
        logger.debug("Retrieved key from AWS s3")
        return contents[0].get('Key')
    logger.debug("No objects found in s3 bucket.")
    return None


def process_request(client, request_key, args):
    request = client.get_object(
        Bucket=args.read_bucket,
        Key=request_key
    )
    request_controller(client, request, args)
    client.delete_object(
        Bucket=args.read_bucket,
        Key=request_key
    )
    logger.debug(f"Request from bucket {args.read_bucket} retrieved and deleted.")


def request_controller(client, request, args):
    request = json.loads(request.get("Body").read())
    request_type = request.get('type')
    if request_type == 'create':
        if args.write_bucket is not None:
            create_widget_s3(client, request, args)
        if args.write_table is not None:
            create_widget_dynamo(request, args)
    if request_type == "delete":
        # Todo handle delete requests in future
        return None
    if request_type == "update":
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


if __name__ == '__main__':
    consume()
