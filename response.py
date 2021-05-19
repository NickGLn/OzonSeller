import requests

class Response:

    def __init__(self, raw_response, body_key):
        self._body = None
        self.content = None
        self.code = None
        self.parse_response(raw_response, body_key)

    def parse_response(self, raw_response, body_key):
        response_json = raw_response.json()
        if 'result' in response_json:
            self._body = response_json[body_key]
            self.content = raw_response.content
            self.code = raw_response.status_code

    def get_results(self):
        return self._body

    def get_status_code(self):
        return self.code

    def get_response_content(self):
        return self.content