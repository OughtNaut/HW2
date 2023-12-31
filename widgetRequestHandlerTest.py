import json
import unittest

import boto3

import WidgetRequestHandler
import moto



class WidgetRequestHandlerTest(unittest.TestCase):


    def get_valid_event(self):
        jsonl = json.dumps({
            "httpMethod": "POST",
            "headers": {
                "Content-Type": "application/json"
            },
            "body": {"type":"update","requestId":"840117b1-b4b8-4750-8757-36bc5a441f9e","widgetId":"8123f304-f23f-440b-a6d3-80e979fa4cd6","owner":"Mary Matthews","description":"GEYPOLQOEELYDNNLBCLPQGTNIAGBJMIGGKD","otherAttributes":[{"name":"color","value":"yellow"},{"name":"size","value":"301"},{"name":"size-unit","value":"cm"},{"name":"height","value":"637"},{"name":"height-unit","value":"cm"},{"name":"width","value":"85"},{"name":"rating","value":"0.23102671"},{"name":"price","value":"24.44"},{"name":"quantity","value":"843"},{"name":"vendor","value":"TKDWESJEFIW"},{"name":"note","value":"ZDXTSRLNLKGBPEZOUAQOKQSXONDVUGTOAVAKVMMGJJCMHVSXUNVFWCTKEDPIZROGHEKCIVAWEOMKMWGQCRZJXWAECGNCUCXGKBWNMKGHUDRJPXYQOFNZXYRPYFWXGYKEMJNGAKIHLXHHNOJJIJFFTVGUIVCPJOKOEEJWLDAJDKWMZREXDWTLPJMPOOEASBTZUOCSAZOVCNHOWWMVURWXOHSYMNXBKTBHVWCCUNULSLRNDUTZHKHDNBWTOPRPERHKEUTPOBQAJYSNJXFDDKSWJACWUJIBQFORREFKZIWEVBBIGZUEYPFTCVJQWMVAYXLENZZVYPRBRRXGPAAKLFBDIMNKDDNEBJZVORUVRUUBOLWTAKXJO"}]},
            "isBase64Encoded": False
        })
        return jsonl
    def get_test_bodies(self):
        return {
            "body": {"type":"update","requestId":"840117b1-b4b8-4750-8757-36bc5a441f9e","widgetId":"8123f304-f23f-440b-a6d3-80e979fa4cd6","owner":"Mary Matthews","description":"GEYPOLQOEELYDNNLBCLPQGTNIAGBJMIGGKD","otherAttributes":[{"name":"color","value":"yellow"},{"name":"size","value":"301"},{"name":"size-unit","value":"cm"},{"name":"height","value":"637"},{"name":"height-unit","value":"cm"},{"name":"width","value":"85"},{"name":"rating","value":"0.23102671"},{"name":"price","value":"24.44"},{"name":"quantity","value":"843"},{"name":"vendor","value":"TKDWESJEFIW"},{"name":"note","value":"ZDXTSRLNLKGBPEZOUAQOKQSXONDVUGTOAVAKVMMGJJCMHVSXUNVFWCTKEDPIZROGHEKCIVAWEOMKMWGQCRZJXWAECGNCUCXGKBWNMKGHUDRJPXYQOFNZXYRPYFWXGYKEMJNGAKIHLXHHNOJJIJFFTVGUIVCPJOKOEEJWLDAJDKWMZREXDWTLPJMPOOEASBTZUOCSAZOVCNHOWWMVURWXOHSYMNXBKTBHVWCCUNULSLRNDUTZHKHDNBWTOPRPERHKEUTPOBQAJYSNJXFDDKSWJACWUJIBQFORREFKZIWEVBBIGZUEYPFTCVJQWMVAYXLENZZVYPRBRRXGPAAKLFBDIMNKDDNEBJZVORUVRUUBOLWTAKXJO"}]},
            "badbody": {"type":"update","requestId":"840117b1-b4b8-4750-8757-36bc5a441f9e","widgetId":"8123f304-f23f-440b-a6d3-80e979fa4cd6","description":"GEYPOLQOEELYDNNLBCLPQGTNIAGBJMIGGKD","otherAttributes":[{"name":"color","value":"yellow"},{"name":"size","value":"301"},{"name":"size-unit","value":"cm"},{"name":"height","value":"637"},{"name":"height-unit","value":"cm"},{"name":"width","value":"85"},{"name":"rating","value":"0.23102671"},{"name":"price","value":"24.44"},{"name":"quantity","value":"843"},{"name":"vendor","value":"TKDWESJEFIW"},{"name":"note","value":"ZDXTSRLNLKGBPEZOUAQOKQSXONDVUGTOAVAKVMMGJJCMHVSXUNVFWCTKEDPIZROGHEKCIVAWEOMKMWGQCRZJXWAECGNCUCXGKBWNMKGHUDRJPXYQOFNZXYRPYFWXGYKEMJNGAKIHLXHHNOJJIJFFTVGUIVCPJOKOEEJWLDAJDKWMZREXDWTLPJMPOOEASBTZUOCSAZOVCNHOWWMVURWXOHSYMNXBKTBHVWCCUNULSLRNDUTZHKHDNBWTOPRPERHKEUTPOBQAJYSNJXFDDKSWJACWUJIBQFORREFKZIWEVBBIGZUEYPFTCVJQWMVAYXLENZZVYPRBRRXGPAAKLFBDIMNKDDNEBJZVORUVRUUBOLWTAKXJO"}]},
        }

    def test_is_post(self):
        event = {"httpMethod": "POST"}
        self.assertEqual(WidgetRequestHandler.is_post(event), True)
        event = {"httpMethod": "GET"}
        self.assertEqual(WidgetRequestHandler.is_post(event), False)
        event = {"httpMethod": "PUT"}
        self.assertEqual(WidgetRequestHandler.is_post(event), False)
        event = {"httpMethod": "post"}
        self.assertEqual(WidgetRequestHandler.is_post(event), False)
        event = {"httpMethod": ""}
        self.assertEqual(WidgetRequestHandler.is_post(event), False)

    @moto.mock_sqs
    def test_create_request(self):
        sqs = boto3.client("sqs")
        queue_url = sqs.create_queue(QueueName="cs5260-requests")["QueueUrl"]
        event = json.loads(self.get_valid_event())
        response = WidgetRequestHandler.create_request(event, queue_url)
        self.assertIsNotNone(response)
        body = event["body"]
        body.pop("requestId")
        response = WidgetRequestHandler.create_request(event, queue_url)
        self.assertIsNone(response)


    def test_valid_body(self):
        test_bodies = self.get_test_bodies()
        event = {"body": test_bodies["body"]}
        self.assertTrue(WidgetRequestHandler.valid_body(event))
        event["body"] = test_bodies["badbody"]
        self.assertFalse(WidgetRequestHandler.valid_body(event))


if __name__ == '__main__':
    unittest.main()
