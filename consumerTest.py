import argparse
import json

import boto3
import unittest
import moto
import Consumer


class ConsumerTest(unittest.TestCase):

    def get_test_widget(self):
        testWidget = {
            "type":"create",
            "requestId":"e80fab52-71a5-4a76-8c4d-11b66b83ca2a",
            "widgetId":"8123f304-f23f-440b-a6d3-80e979fa4cd6",
            "owner":"Mary Matthews",
            "label":"JWJYY",
            "description":"THBRNVNQPYAWNHGRGUKIOWCKXIVNDLWOIQTADHVEVMUAJWDONEPUEAXDITDSHJTDLCMHHSESFXSDZJCBLGIKKPUYAWKQAQI",
            "otherAttributes":[{"name":"width-unit","value":"cm"},
                               {"name":"length-unit","value":"cm"},
                               {"name":"rating","value":"2.580677"},
                               {"name":"note","value":"FEGYXHIJCTYNUMNMGZBEIDLKXYFNHFLVDYZRNWUDQAKQSVFLPRJTTXARVEIFDOLTUSWZZWVERNWPPOEYSUFAKKAPAGUALGXNDOVPNKQQKYWWOUHGOJWKAJGUXXBXLWAKJCIVPJYRMRWMHRUVBGVILZRMESQQJRBLXISNFCXGGUFZCLYAVLRFMJFLTBOTLKQRLWXALLBINWALJEMUVPNJWWRWLTRIBIDEARTCSLZEDLZRCJGSMKUOZQUWDGLIVILTCXLFIJIULXIFGRCANQPITKQYAKTPBUJAMGYLSXMLVIOROSBSXTTRULFYPDFJSFOMCUGDOZCKEUIUMKMMIRKUEOMVLYJNJQSMVNRTNGH"}
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
        Consumer.process_request(s3,'test_key',args)
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

        self.assertIsNone(Consumer.request_controller(s3,s3.get_object(Bucket='test_bucket', Key='test_key'),args))
        request['type'] = 'update'
        s3.put_object(
            Body=json.dumps(request),
            Bucket='test_bucket',
            Key='test_key2'
        )
        self.assertIsNone(Consumer.request_controller(s3, s3.get_object(Bucket='test_bucket', Key='test_key2'), args))



if __name__ == '__main__':
    unittest.main()
