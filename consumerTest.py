import argparse
import json

import boto3
import unittest
import moto

import Consumer


class ConsumerTest(unittest.TestCase):

    def get_test_widget(self):
        testWidget = {
            "type": "create",
            "requestId": "e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
            "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
            "owner": "Mary Matthews",
            "label": "JWJYY",
            "description": "THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
            "otherAttributes": [{"name": "width-unit", "value": "cm"},
                                {"name": "length-unit", "value": "cm"},
                                {"name": "rating", "value": "2.580677"},
                                {"name": "note",
                                 "value": "FEGYXHIJCTYNUMNMGZBEIDLKXYFNHFLVDYZRNWUDQAKQSVFLPRJTTXARVEIFDOLTUSWZZWVERNWPPOEYSUFAKKAPAGUALGXNDOVPNKQQKYWWOUHGOJWKAJGUXXBXLWAKJCIVPJYRMRWMHRUVBGVILZRMESQQJRBLXISNFCXGGUFZCLYAVLRFMJFLTBOTLKQRLWXALLBINWALJEMUVPNJWWRWLTRIBIDEARTCSLZEDLZRCJGSMKUOZQUWDGLIVILTCXLFIJIULXIFGRCANQPITKQYAKTPBUJAMGYLSXMLVIOROSBSXTTRULFYPDFJSFOMCUGDOZCKEUIUMKMMIRKUEOMVLYJNJQSMVNRTNGH"}
                                ]
        }
        return testWidget

    @moto.mock_s3
    def test_get_single_key(self):
        args = argparse.Namespace(read_bucket='test_bucket')
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test_bucket')
        self.assertIsNone(Consumer.get_single_key(s3, args))

        s3.put_object(
            Body=json.dumps(ConsumerTest.get_test_widget(self)),
            Bucket='test_bucket',
            Key='test_key',
        )
        self.assertTrue(Consumer.get_single_key(s3, args) == 'test_key')

    @moto.mock_s3
    def test_process_request(self):
        args = argparse.Namespace(read_bucket='test_bucket', write_bucket='test_write_bucket', write_table=None)
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test_bucket')
        s3.create_bucket(Bucket='test_write_bucket')
        s3.put_object(
            Body=json.dumps(ConsumerTest.get_test_widget(self)),
            Bucket='test_bucket',
            Key='test_key',
        )
        Consumer.process_request(s3, 'test_key', args)
        bucket = boto3.resource('s3').Bucket('test_bucket')
        # I lifted this counting loop from
        # https://stackoverflow.com/questions/32408167/how-do-i-get-the-size-of-a-boto3-collection
        size = sum(1 for _ in bucket.objects.all())
        self.assertEqual(size, 0)

    @moto.mock_s3
    def test_request_controller(self):
        args = argparse.Namespace(read_bucket='test_bucket', write_bucket='test_write_bucket', write_table=None)
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test_bucket')
        request = ConsumerTest.get_test_widget(self)
        request['type'] = 'delete'
        s3.put_object(
            Body=json.dumps(request),
            Bucket='test_bucket',
            Key='test_key'
        )

        self.assertIsNone(Consumer.request_controller(s3, s3.get_object(Bucket='test_bucket', Key='test_key'), args))
        request['type'] = 'update'
        s3.put_object(
            Body=json.dumps(request),
            Bucket='test_bucket',
            Key='test_key2'
        )
        self.assertIsNone(Consumer.request_controller(s3, s3.get_object(Bucket='test_bucket', Key='test_key2'), args))

    @moto.mock_s3
    def test_create_widget_s3(self):
        s3 = boto3.client('s3')
        request = ConsumerTest.get_test_widget(self)
        args = argparse.Namespace(read_bucket='test_bucket', write_bucket='test_write_bucket', write_table=None)
        s3.create_bucket(Bucket='test_write_bucket')
        Consumer.create_widget_s3(s3, request, args)
        object = s3.get_object(
            Bucket='test_write_bucket',
            Key='widgets/mary-matthews/8123f304-f23f-440b-a6d3-80e979fa4cd6'
        )
        self.assertEqual(request, json.loads(object.get('Body').read()))

    @moto.mock_dynamodb
    def test_create_widget_dynamo(self):
        boto3.client('dynamodb').create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            TableName='test_table',
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        widget = ConsumerTest.get_test_widget(self)
        args = argparse.Namespace(read_bucket='test_bucket', write_bucket='test_write_bucket', write_table='test_table')
        Consumer.create_widget_dynamo(widget, args)
        retrieved = boto3.client('dynamodb').get_item(
            TableName='test_table',
            Key={
                'id': {
                    'S': '8123f304-f23f-440b-a6d3-80e979fa4cd6'
                }
            }
        )
        self.assertEqual(widget.get('widgetId'), retrieved.get('Item').get('id').get('S'))


if __name__ == '__main__':
    unittest.main()
