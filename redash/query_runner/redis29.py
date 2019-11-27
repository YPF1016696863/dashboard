import json
from collections import OrderedDict
from sympy import sympify
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

    def __get_column_type_from_set(self, type_set):
        if len(type_set) == 1:
            return type_set.pop()
        elif len(type_set) == 2 \
                and TYPE_FLOAT in type_set \
                and TYPE_INTEGER in type_set:
            return TYPE_FLOAT
        else:
            return TYPE_STRING

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
            c['type'] = self.__get_column_type_from_set(col_data_type_flags[c['name']])

        return {'columns': columns, 'rows': rows}

    def __add_extra_columns(self, data, extra_columns):
        if extra_columns is None:
            extra_columns = []

        columns = data['columns']
        rows = data['rows']

        column_name_set = set()
        for column in columns:
            column_name_set.add(column['name'])

        for extra_column_def in extra_columns:
            if type(extra_column_def) is OrderedDict and "expr" in extra_column_def and "name" in extra_column_def:
                expr = extra_column_def["expr"]
                name = extra_column_def["name"]

                if name in column_name_set:
                    return "Duplicated columns in extra column definition: " + name, None

                column_name_set.add(name)
                column_data_type = set()

                for row in rows:
                    data = None

                    try:
                        data = str(sympify(expr).subs(row).evalf())
                    except:
                        data = expr
                    val_type, val = guess_type_and_decode(data)

                    column_data_type.add(val_type)
                    row[name] = val

                columns.append(
                    {'name': name, 'friendly_name': name, 'type': self.__get_column_type_from_set(column_data_type)})
            else:
                return "Unexpected extra column definition!", None

        return None, {'columns': columns, 'rows': rows}

    def __handle_select_and_ordering(self, data, selected_columns_set, ordered_columns_list):
        columns = data['columns']
        rows = data['rows']

        column_name_set = set()
        column_name_mapping = {}
        for column in columns:
            column_name_set.add(column['name'])
            column_name_mapping[column['name']] = column

        if len(selected_columns_set) == 0:
            selected_columns_set = column_name_set

        if not selected_columns_set.issubset(column_name_set):
            return "Selected more columns than actually exist!", None

        ordered_columns_set = set(ordered_columns_list)
        if not ordered_columns_set.issubset(selected_columns_set):
            return "Ordering not selected columns!", None

        new_columns = []
        for ordered_name in ordered_columns_list:
            new_columns.append(column_name_mapping[ordered_name])

        for column in columns:
            regular_column_name = column['name']
            if regular_column_name not in ordered_columns_set and regular_column_name in selected_columns_set:
                new_columns.append(column)

        new_rows = []
        for row in rows:
            new_row = OrderedDict()
            for column in new_columns:
                if column['name'] in row:
                    new_row[column['name']] = row[column['name']]

            new_rows.append(new_row)

        return None, {'columns': new_columns, 'rows': new_rows}

    def __handle_alias(self, data, alias_mapping):
        columns = data['columns']
        rows = data['rows']

        for column in columns:
            if column['name'] in alias_mapping:
                column['friendly_name'] = alias_mapping[column['name']]

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
                    err, data = self.__add_extra_columns(data, extra_columns)

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

            err, data = self.__handle_select_and_ordering(data, selected_columns_set, ordered_columns_list)
            if err is not None:
                return None, err

            if 'alias' in query_obj and query_obj['alias'] is not None:
                alias_mapping = query_obj['alias']

                if isinstance(alias_mapping, OrderedDict):
                    data = self.__handle_alias(data, alias_mapping)
                else:
                    return None, "alias field is not a dictionary!"

            return json.dumps(data), None

        except KeyboardInterrupt:
            return None, "Query cancelled by user."


register(Redis29)

#redis = Redis29({'host': '127.0.0.1'})
#print(redis.run_query('{"key":"XYZ","extra":[{"expr":"(a+bc)/5*ext","name":"add"},{"expr":"add -3","name":"more"}],"select":["bc"],"order":["bc","more"],"alias":{"more":"more nice column name"}}', None))