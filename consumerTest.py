import argparse
import json
import boto3
import unittest
import moto

import Consumer


class ConsumerTest(unittest.TestCase):

    def get_test_update_widget(self):
        test_update_Widget = {
            "type": "update",
            "requestId": "e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
            "widgetId": "8123f304-f23f-440b-a6d3-80e979fa4cd6",
            "owner": "Mary Leet",
            "label": "Leet",
            "description": "LeetJCBLGIKKPUYAWKQAQI",
            "otherAttributes": [{"name": "leet-unit", "value": "Leet"},
                                {"name": "length-unit", "value": "Leets"},
                                {"name": "rating", "value": "2"},
                                {"name": "biggum",
                                 "value": "Biggumer"}
                                ]
        }
        return test_update_Widget
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

    def get_delete_widget(self):
        testWidget = {
            "type": "delete",
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
    @moto.mock_sqs
    def test_get_request_cache(self):
        args = argparse.Namespace(read_bucket='test_bucket')
        s3 = boto3.client('s3')
        sqs = boto3.client('sqs')
        s3.create_bucket(Bucket='test_bucket')
        sqs.create_queue(QueueName='test_queue')
        sqs.send_message(QueueUrl='test_queue',
                         MessageBody='1')
        sqs.send_message(QueueUrl='test_queue',
                         MessageBody='1')
        sqs.send_message(QueueUrl='test_queue',
                         MessageBody='1')
        self.assertListEqual(Consumer.get_request_cache(s3, sqs, args), [])

        s3.put_object(
            Body=json.dumps(ConsumerTest.get_test_widget(self)),
            Bucket='test_bucket',
            Key='test_key',
        )
        request_cache = Consumer.get_request_cache(s3, sqs, args)
        self.assertTrue(len(request_cache) != 0)
        args = argparse.Namespace(read_bucket=None, read_queue='test_queue')
        self.assertEqual(len(Consumer.get_request_cache(s3,sqs,args)),3)
        self.assertListEqual(Consumer.get_request_cache(s3, sqs, args), [])
    @moto.mock_s3
    @moto.mock_sqs
    def test_process_request_create(self):
        args = argparse.Namespace(read_bucket='test_bucket', write_bucket='test_write_bucket', write_table=None)
        s3 = boto3.client('s3')
        sqs = boto3.client('sqs')
        s3.create_bucket(Bucket='test_bucket')
        s3.create_bucket(Bucket='test_write_bucket')
        s3.put_object(
            Body=json.dumps(ConsumerTest.get_test_widget(self)),
            Bucket='test_bucket',
            Key='test_key',
        )
        cache = Consumer.get_request_cache(s3,sqs, args)
        Consumer.process_request(s3, cache[0], args)
        bucket = boto3.resource('s3').Bucket('test_bucket')
        # I lifted this counting loop from
        # https://stackoverflow.com/questions/32408167/how-do-i-get-the-size-of-a-boto3-collection
        size = sum(1 for _ in bucket.objects.all())
        self.assertEqual(size, 0)

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
    def test_delete_widget_dynamo(self):
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
        Consumer.delete_widget_dynamo(ConsumerTest.get_test_widget(self), args)
        retrieved = boto3.client('dynamodb').get_item(
            TableName='test_table',
            Key={
                'id': {
                    'S': '8123f304-f23f-440b-a6d3-80e979fa4cd6'
                }
            }
        )
        self.assertEqual(retrieved.get('Item'), None)



    @moto.mock_s3
    def test_delete_widget_s3(self):
        s3 = boto3.client('s3')
        request = ConsumerTest.get_test_widget(self)
        args = argparse.Namespace(read_bucket='test_bucket', write_bucket='test_write_bucket', write_table=None)
        s3.create_bucket(Bucket='test_write_bucket')
        s3.create_bucket(Bucket='test_bucket')
        bucket = boto3.resource('s3').Bucket('test_write_bucket')
        Consumer.create_widget_s3(s3, request, args)
        size = sum(1 for _ in bucket.objects.all())
        self.assertTrue(size == 1)
        Consumer.delete_widget_s3(s3, self.get_delete_widget(), args)
        size = sum(1 for _ in bucket.objects.all())
        self.assertTrue(size == 0)


    @moto.mock_dynamodb
    def test_update_widget_dynamo(self):
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
        Consumer.update_widget_dynamo(self.get_test_update_widget(), args)
        retrieved = boto3.client('dynamodb').get_item(
            TableName='test_table',
            Key={
                'id': {
                    'S': '8123f304-f23f-440b-a6d3-80e979fa4cd6'
                }
            }
        )
        self.assertEqual(retrieved.get('Item').get('owner').get("S"),"Mary Leet")



if __name__ == '__main__':
    unittest.main()
