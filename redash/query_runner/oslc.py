from redash.utils import json_loads, json_dumps
from redash.query_runner import *

TYPES_MAP = {
    "REF##String": TYPE_STRING,
    "REF##XMLLiteral": TYPE_STRING,
    "REF##Integer": TYPE_INTEGER,
    "REF##Double": TYPE_FLOAT,
    "REF##Decimal": TYPE_FLOAT,
    "REF##Float": TYPE_FLOAT,
    "REF##Boolean": TYPE_BOOLEAN,
}


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
            raise Exception("Not qualified ROOT URL!")

        return base_url

    def test_connection(self):
        base_url = self.__get_base_url()
        adapter, error = self.get_definition(base_url)
        if error is not None:
            raise Exception(error)

    def __get_json_response(self, url):
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

        return self.get_response(url=url, headers=headers)

    def get_definition(self, base_url):
        def_url = base_url + "adapter.json"

        response, error = self.__get_json_response(def_url)
        if error is not None:
            return None, error

        json_data = response.content.strip()
        if json_data:
            json_data_obj = json_loads(json_data)
            return json_data_obj, None
        else:
            return None, "Got empty response from '{}'.".format(base_url)

    def convert_oslc_type(self, oslc_type):
        if oslc_type in TYPES_MAP:
            return TYPES_MAP[oslc_type]
        else:
            return None

    def get_columns(self, resource_name, adapter):
        columns = []
        complex_fields = set()

        for resource in adapter["resources"]:
            if resource["name"] == resource_name:
                for prop in resource["properties"]:
                    type_in_dashboard = self.convert_oslc_type(prop["type"])
                    if type_in_dashboard is None:
                        complex_fields.add(prop["name"])
                        type_in_dashboard = TYPE_STRING

                    columns.append({
                        "name": prop["name"],
                        "friendly_name": prop["title"],
                        "type": type_in_dashboard
                    })
                break

        if not columns:
            raise Exception("Cannot find definition for " + resource_name)

        for default_column in ["about", "name", "instanceId"]:
            columns.append({
                "name": default_column,
                "friendly_name": default_column.capitalize(),
                "type": TYPE_STRING
            })

        return columns, complex_fields

    def run_query(self, query, user):
        base_url = self.__get_base_url()

        adapter, error = self.get_definition(base_url)
        if error is not None:
            return None, error

        try:
            query = json_loads(query)
            query_endpoint = ""

            if "endpoint" in query:
                query_endpoint = query["endpoint"]

            url = (base_url + query_endpoint).strip()

            if query_endpoint != "":
                response, error = self.__get_json_response(url)
                if error is not None:
                    return None, error

                json_data = response.content.strip()

                if json_data:
                    json_data_arr = json_loads(json_data)

                    if not isinstance(json_data_arr, list):
                        json_data_arr = [json_data_arr]

                    resource_name = ""
                    for item in json_data_arr:
                        if "name" in item:
                            resource_name = item["name"]
                            break

                    if resource_name == "":
                        return None, "Unknown resource name in the response!"

                    columns, complex_fields = self.get_columns(resource_name, adapter)

                    for item in json_data_arr:
                        for key in item:
                            if key in complex_fields:
                                item[key] = json_dumps(item[key])

                    data = {
                        "columns": columns,
                        "rows": json_data_arr
                    }

                    return json_dumps(data), None
                else:
                    return None, "Got empty response from '{}'.".format(url)

        except KeyboardInterrupt:
            return None, "Query cancelled by user."


register(Oslc)
