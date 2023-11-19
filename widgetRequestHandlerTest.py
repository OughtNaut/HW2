import unittest
import WidgetRequestHandler

class WidgetRequestHandlerTest(unittest.TestCase):


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


if __name__ == '__main__':
    unittest.main()
