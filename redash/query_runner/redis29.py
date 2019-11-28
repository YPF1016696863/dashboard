import json
from collections import OrderedDict

from redash.query_runner import *


####
# QUERY E#XAMPLE
# {
#   "key": "XYZ",
#   "extra": [
#     {
#       "expr": "(a+bc)/5*ext",
#       "name": "add"
#     },
#     {
#       "expr": "add -3",
#       "name": "more"
#     }
#   ],
#   "select": [
#     "ab"
#   ],
#   "order": [
#     "ab",
#     "more"
#   ],
#   "alias": {
#     "more": "more nice column name"
#   }
# }


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

    def __get_column_names(self, data_obj):
        column_names = []
        column_name_set = set()

        for obj in data_obj:
            if isinstance(obj, dict) or isinstance(obj, OrderedDict):
                for k, v in obj.items():
                    if k not in column_name_set:
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
                    val_type, val = guess_type_and_decode(obj[c_name])

                    col_data_type_flags[c_name].add(val_type)
                    row[c_name] = val

            rows.append(row)

        for c in columns:
            c['type'] = get_column_type_from_set(col_data_type_flags[c['name']])

        for row in rows:
            for c in columns:
                c_name = c['name']
                if c_name not in row:
                    row[c_name] = default_value_for_type(c['type'])

        return {'columns': columns, 'rows': rows}

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

            data_obj = self.__get_data(query_obj['key'])
            if data_obj is None:
                return None, "Empty data or not supported data format!"

            column_names = self.__get_column_names(data_obj)
            data = self.__extract_data(data_obj, column_names)

            extra_columns = []
            if 'extra' in query_obj and query_obj['extra'] is not None:
                extra_columns = query_obj['extra']

                if isinstance(extra_columns, list):
                    err, data = handle_extra_columns(data, extra_columns)

                    if err is not None:
                        return None, err
                else:
                    return None, "extra field is not a list!"

            selected_columns_set = set()
            if 'select' in query_obj and query_obj['select'] is not None:
                selected_columns_list = query_obj['select']

                if isinstance(selected_columns_list, list):
                    selected_columns_set.update(selected_columns_list)

                    if "*" in selected_columns_set:
                        selected_columns_set = set()
                    elif extra_columns is not None:
                        for extra_column_def in extra_columns:
                            selected_columns_set.add(extra_column_def['name'])
                else:
                    return None, "select field is not a list!"

            ordered_columns_list = []
            if 'order' in query_obj and query_obj['order'] is not None:
                ordered_columns_list = query_obj['order']

                if not isinstance(ordered_columns_list, list):
                    return None, "order field is not a list!"

            err, data = handle_select_and_ordering(data, selected_columns_set, ordered_columns_list)
            if err is not None:
                return None, err

            if 'alias' in query_obj and query_obj['alias'] is not None:
                alias_mapping = query_obj['alias']

                if isinstance(alias_mapping, OrderedDict):
                    data = handle_alias(data, alias_mapping)
                else:
                    return None, "alias field is not a dictionary!"

            return json.dumps(data), None

        except KeyboardInterrupt:
            return None, "Query cancelled by user."


#register(Redis29)

redis = Redis29({'host': '127.0.0.1'})
print(redis.run_query('{"key":"XYZ","extra":[{"expr":"(a+bc)/5","name":"add"},{"expr":"add -3","name":"more"}],"select":["*"],"order":["bc","more"],"alias":{"more":"more nice column name"}}', None))
