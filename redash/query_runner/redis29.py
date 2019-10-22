import urllib

from redash.query_runner import *
from redash.utils import json_loads, json_dumps


def __extract_columns(data_resp):
    ncols = len(data_resp["column"])

    columns = []
    mapping_idx_to_name = {}

    for idx in range(ncols):
        column_name = data_resp["column"][idx]["columnname"]
        friendly_name = data_resp["column"][idx]["anothername"]

        if not column_name:
            column_name = 'column_{}'.format(idx)

        if not friendly_name:
            friendly_name = column_name

        mapping_idx_to_name[idx] = column_name

        columns.append({
            'name': column_name,
            'friendly_name': friendly_name,
            'type': TYPE_STRING
        })

    return columns, mapping_idx_to_name


def __extract_data(data_resp, columns, mapping_idx_to_name):
    ncols = len(data_resp["column"])
    nrows = len(data_resp["data"])

    rows = []
    col_data_type_flags = []

    for col_idx in range(0, ncols):
        col_data_type_flags.append(set())

    for row_idx in range(1, nrows):
        row = {}
        for col_idx in range(0, ncols):
            cell_v = data_resp["data"][row_idx][col_idx]
            row[mapping_idx_to_name[col_idx]] = cell_v
            cell_type = guess_type(cell_v)
            col_data_type_flags[col_idx].add(cell_type)

        rows.append(row)

    for col_idx in range(0, ncols):
        if len(col_data_type_flags[col_idx]) == 1:
            columns[col_idx]['type'] = col_data_type_flags[col_idx].pop()
        elif len(col_data_type_flags[col_idx]) == 2 and TYPE_FLOAT in col_data_type_flags[
            col_idx] and TYPE_INTEGER in col_data_type_flags[col_idx]:
            columns[col_idx]['type'] = TYPE_FLOAT

    data = {'columns': columns, 'rows': rows}

    return data


def parse_resp(data_resp):
    columns, mapping_idx_to_name = __extract_columns(data_resp)
    data = __extract_data(data_resp, columns, mapping_idx_to_name)

    return data


class Redis29(BaseHTTPQueryRunner):
    requires_authentication = False
    requires_url = True

    def __init__(self, configuration):
        super(Redis29, self).__init__(configuration)
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
        self.__get_base_url()

    def __get_json_response(self, url):
        headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

        return self.get_response(url=url, headers=headers)

    def run_query(self, query, user):
        base_url = self.__get_base_url()
        api = base_url + "api/v1/getdata"

        try:
            query = json_loads(query)

            if "id" not in query or query["id"] is None:
                return None, "id is not provided!"

            if not isinstance(query["id"], int):
                return None, "id is not an int!"

            if "type" not in query or query["type"] is None:
                query["type"] = "redis"

            if not isinstance(query["type"], str):
                return None, "type is not an string!"

            query["type"] = query["type"].strip()

            full = api + "?" + urllib.urlencode(query)

            response, error = self.__get_json_response(full)
            if error is not None:
                return None, error

            json_data = response.content.strip()
            if not json_data:
                return None, "Got empty response!"

            content = json_loads(json_data)

            if content is None or "status" not in content or content["status"] != "ok":
                return None, "Not OK response!"

            if "data" not in content or content["data"] is None:
                return None, "Empty data!"

            data_resp = content["data"]

            if "data" not in data_resp or "column" not in data_resp:
                return None, "Malformat data!"

            data = parse_resp(data_resp)

            return json_dumps(data), None

        except KeyboardInterrupt:
            return None, "Query cancelled by user."


register(Redis29)
