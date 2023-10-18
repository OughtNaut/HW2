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
            process_request(request_key, args)
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
    request_controller(request)
    client.delete_object(
        Bucket=args.read_bucket,
        Key=request_key
    )


def request_controller(request):
    pass

class WidgetFactory():
    def create_widget(self, request):
        request = json.loads(request)
if __name__ == '__main__':
    consume()
