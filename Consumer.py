import argparse
import boto3
import time
import json


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

    while True:
        request_key = get_single_key(s3, args)
        if request_key is not None:
            process_request(s3, request_key, args)
        else:
            time.sleep(.1)


def get_single_key(client, args):
    response = client.list_objects_v2(
        Bucket=args.read_bucket,
        MaxKeys=1
    )
    return response.get('Contents')[0].get('Key')


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


def request_controller(client, request, args):
    # print(type(request))
    # request = json.loads(request)
    request_type = request.get('type')
    if request_type == 'create':
        if args.write_bucket is not None:
            create_widget_s3(client, request, args)
        if args.write_table is not None:
            create_widget_dynamo(request, args)
    if request_type == "delete":
        # Todo handle delete requests in future
        return
    if request_type == "update":
        # Todo handle update requests in future
        return


def create_widget_dynamo(request_dictionary, args):
    # I modified this from a code snippet found here https://medium.com/@NotSoCoolCoder/handling-json-data-for-dynamodb-using-python-6bbd19ee884e
    boto3.resource('dynamodb').Table(args.write_table).put_item(request_dictionary)


def create_widget_s3(client, request_dictionary, args):
    owner = request_dictionary.get('owner').lower().replace(' ', '-')
    widget_id = request_dictionary.get('widgetId')
    request_json = json.dumps(request_dictionary)
    client.put_object(
        Body=request_json,
        Bucket=args.write_bucket,
        Key=f"widgets/{owner}/{widget_id}"
    )



if __name__ == '__main__':
    consume()
