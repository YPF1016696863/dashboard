from redash.query_runner import BaseHTTPQueryRunner, register
from redash.utils import json_loads


class Oslc(BaseHTTPQueryRunner):
    requires_authentication = False
    requires_url = True
    url_title = 'OSLC Adapter ROOT URL'

    def __init__(self, configuration):
        super(Oslc, self).__init__(configuration)
        self.syntax = 'json'

    @classmethod
    def annotate_query(cls):
        return False

    def __get_base_url(self):
        base_url = self.configuration.get("url", None)
        if base_url is None:
            raise Exception("Not provided ROOT URL!")

        base_url = base_url.strip()
        if base_url == "":
            raise Exception("Empty ROOT URL!")

        if not base_url.endswith("/"):
            base_url = base_url + "/"

        if base_url.find("://") < 0:
            raise Exception("Not qualified URL!")

        return base_url

    def test_connection(self):
        base_url = self.__get_base_url()
        response, error = self.get_response(base_url)
        if error is not None:
            raise Exception(error)

    def run_query(self, query, user):
        base_url = self.__get_base_url()

        try:
            query = json_loads(query)
            query_endpoint = query["endpoint"]
            query_type = query["type"]

            url = (base_url + query_endpoint).strip()

            response, error = self.get_response(url)
            if error is not None:
                return None, error

            json_data = response.content.strip()

            if json_data:
                return json_data, None
            else:
                return None, "Got empty response from '{}'.".format(url)
        except KeyboardInterrupt:
            return None, "Query cancelled by user."


register(Oslc)
