import argparse

import boto3
import unittest
import moto
import Consumer
from unittest.mock import patch

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
    @moto.mock_s3
    def test_get_single_key(self):
        args = argparse.Namespace(read_bucket='test_bucket')
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test_bucket')
        self.assertIsNone(Consumer.get_single_key(s3,args))
