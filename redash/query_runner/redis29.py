import json
from collections import OrderedDict

from redash.query_runner import *


class Redis29(BaseQueryRunner):
    MAX_SCHEMA_COUNT = 100

    def __init__(self, configuration):
        super(Redis29, self).__init__(configuration)

    @classmethod
    def annotate_query(cls):
        return False

    @classmethod
    def configuration_schema(cls):
        schema = {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string',
                    'title': 'Host'
                },
                'port': {
                    'type': 'number',
                    'default': 6379,
                    'title': 'Port'
                },
                'passwd': {
                    'type': 'string',
                    'title': 'Password'
                },
                'db': {
                    'type': 'number',
                    'default': 0,
                    'title': 'Database 0 to 15'
                }
            },
            "order": ['host', 'port', 'db', 'passwd'],
            "required": ['host'],
            'secret': ['passwd']
        }

        return schema

    @classmethod
    def name(cls):
        return "Redis29"

    @classmethod
    def enabled(cls):
        try:
            import redis
        except ImportError:
            return False

        return True

    def __get_connection(self):
        import redis

        host = self.configuration['host'] if 'host' in self.configuration else 'localhost'
        password = self.configuration['passwd'] if 'passwd' in self.configuration else None
        db = self.configuration['db'] if 'db' in self.configuration else 0
        port = self.configuration['port'] if 'port' in self.configuration else 6379

        connection = redis.StrictRedis(host=host,
                                       password=password,
                                       db=db,
                                       port=port)

        return connection

    def __get_data(self, key):
        conn = self.__get_connection()
        ret = conn.get(key)

        if ret is not None:
            try:
                ret_obj = json.loads(ret, object_pairs_hook=OrderedDict)
                if ret_obj is not None and isinstance(ret_obj, OrderedDict) and "data" in ret_obj:
                    data_arr = ret_obj["data"]
                    if data_arr is not None and isinstance(data_arr, list):
                        return data_arr
            except Exception:
                return None

        return None

    def __get_column_names(self, data_obj, filter_columns=None):
        column_names = []
        column_name_set = set()

        for obj in data_obj:
            if isinstance(obj, dict) or isinstance(obj, OrderedDict):
                for k, v in obj.items():
                    if k not in column_name_set and (filter_columns is None or k in filter_columns):
                        column_name_set.add(k)
                        column_names.append(k)

        return column_names

    def __extract_data(self, data_obj, column_names):
        columns = []
        col_data_type_flags = {}

        for c_name in column_names:
            columns.append({'name': c_name, 'friendly_name': c_name, 'type': TYPE_STRING})
            col_data_type_flags[c_name] = set()

        rows = []

        for obj in data_obj:
            row = OrderedDict()
            for c_name in column_names:
                if c_name in obj:
                    col_data_type_flags[c_name].add(guess_type(obj[c_name]))
                    row[c_name] = obj[c_name]

            rows.append(row)

        for c in columns:
            c_name = c['name']

            if len(col_data_type_flags[c_name]) == 1:
                c['type'] = col_data_type_flags[c_name].pop()
            elif len(col_data_type_flags[c_name]) == 2 \
                    and TYPE_FLOAT in col_data_type_flags[c_name] \
                    and TYPE_INTEGER in col_data_type_flags[c_name]:
                c['type'] = TYPE_FLOAT
        data = {'columns': columns, 'rows': rows}

        return data

    def test_connection(self):
        conn = self.__get_connection()
        conn.ping()

    def get_schema(self, prefix=None):
        conn = self.__get_connection()
        pattern = "*" if prefix is None else prefix
        if '*' not in pattern:
            pattern = pattern + "*"

        schema_dict = OrderedDict()

        for key in conn.scan_iter(pattern, self.MAX_SCHEMA_COUNT):
            schema_dict[key] = {'name': key, 'columns': []}

        ret = []

        for k, v in schema_dict.items():
            data_obj = self.__get_data(k)
            if data_obj is None:
                continue

            column_names = self.__get_column_names(data_obj)
            v['columns'] = column_names

            if column_names:
                ret.append(v)

        return ret

    def run_query(self, query, user):
        try:
            if query is None or query.strip() == '':
                return None, "Empty query!"

            query_obj = None

            try:
                query_obj = json.loads(query, object_pairs_hook=OrderedDict)
                if query_obj is None or type(query_obj) is not OrderedDict:
                    query_obj = {"key": query}
            except Exception:
                query_obj = {"key": query}

            if 'key' not in query_obj:
                return None, "No key provided!"

            filter_columns = None

            if 'columns' in query_obj:
                filter_columns = set()
                for c in query_obj['columns']:
                    filter_columns.add(c)

            data_obj = self.__get_data(query_obj['key'])
            if data_obj is None:
                return None, "Empty data or not supported data format!"

            column_names = self.__get_column_names(data_obj, filter_columns)
            data = self.__extract_data(data_obj, column_names)

            return json.dumps(data), None

        except KeyboardInterrupt:
            return None, "Query cancelled by user."


register(Redis29)