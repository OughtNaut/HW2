import argparse
import sys
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
    request_json = json.loads(event)
    if is_post(request_json):
        pass
    pass


def is_post(request_json):

if __name__ == '__main__':
    handle_api_request()
