import logging
from collections import OrderedDict

import requests
from dateutil import parser
from sympy import sympify

from redash.utils import json_loads

logger = logging.getLogger(__name__)

__all__ = [
    'BaseQueryRunner',
    'BaseHTTPQueryRunner',
    'InterruptException',
    'BaseSQLQueryRunner',
    'TYPE_DATETIME',
    'TYPE_BOOLEAN',
    'TYPE_INTEGER',
    'TYPE_STRING',
    'TYPE_DATE',
    'TYPE_FLOAT',
    'SUPPORTED_COLUMN_TYPES',
    'register',
    'get_query_runner',
    'import_query_runners',
    'guess_type',
    'guess_type_and_decode',
    'default_value_for_type',
    'handle_extra_columns',
    'handle_alias',
    'handle_select_and_ordering',
    'get_column_type_from_set'
]

# Valid types of columns returned in results:
TYPE_INTEGER = 'integer'
TYPE_FLOAT = 'float'
TYPE_BOOLEAN = 'boolean'
TYPE_STRING = 'string'
TYPE_DATETIME = 'datetime'
TYPE_DATE = 'date'

SUPPORTED_COLUMN_TYPES = set([
    TYPE_INTEGER,
    TYPE_FLOAT,
    TYPE_BOOLEAN,
    TYPE_STRING,
    TYPE_DATETIME,
    TYPE_DATE
])


class InterruptException(Exception):
    pass


class NotSupported(Exception):
    pass


class BaseQueryRunner(object):
    noop_query = None

    def __init__(self, configuration):
        self.syntax = 'sql'
        self.configuration = configuration

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def type(cls):
        return cls.__name__.lower()

    @classmethod
    def enabled(cls):
        return True

    @classmethod
    def annotate_query(cls):
        return True

    @classmethod
    def configuration_schema(cls):
        return {}

    def test_connection(self):
        if self.noop_query is None:
            raise NotImplementedError()
        data, error = self.run_query(self.noop_query, None)

        if error is not None:
            raise Exception(error)

    def run_query(self, query, user):
        raise NotImplementedError()

    def fetch_columns(self, columns):
        column_names = []
        duplicates_counter = 1
        new_columns = []

        for col in columns:
            column_name = col[0]
            if column_name in column_names:
                column_name = "{}{}".format(column_name, duplicates_counter)
                duplicates_counter += 1

            column_names.append(column_name)
            new_columns.append({'name': column_name,
                                'friendly_name': column_name,
                                'type': col[1]})

        return new_columns

    def get_schema(self, prefix=None):
        raise NotSupported()

    def _run_query_internal(self, query):
        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed running query [%s]." % query)
        return json_loads(results)['rows']

    @classmethod
    def to_dict(cls):
        return {
            'name': cls.name(),
            'type': cls.type(),
            'configuration_schema': cls.configuration_schema()
        }


class BaseSQLQueryRunner(BaseQueryRunner):

    def get_schema(self, prefix=None):
        schema_dict = {}
        self._get_tables(schema_dict)
        return schema_dict.values()

    def _get_tables(self, schema_dict):
        return []


class BaseHTTPQueryRunner(BaseQueryRunner):
    response_error = "Endpoint returned unexpected status code"
    requires_authentication = False
    requires_url = True
    url_title = 'URL base path'
    username_title = 'HTTP Basic Auth Username'
    password_title = 'HTTP Basic Auth Password'

    @classmethod
    def configuration_schema(cls):
        schema = {
            'type': 'object',
            'properties': {
                'url': {
                    'type': 'string',
                    'title': cls.url_title,
                },
                'username': {
                    'type': 'string',
                    'title': cls.username_title,
                },
                'password': {
                    'type': 'string',
                    'title': cls.password_title,
                },
            },
            'secret': ['password'],
            'order': ['url', 'username', 'password']
        }

        if cls.requires_url or cls.requires_authentication:
            schema['required'] = []

        if cls.requires_url:
            schema['required'] += ['url']

        if cls.requires_authentication:
            schema['required'] += ['username', 'password']
        return schema

    def get_auth(self):
        username = self.configuration.get('username')
        password = self.configuration.get('password')
        if username and password:
            return (username, password)
        if self.requires_authentication:
            raise ValueError("Username and Password required")
        else:
            return None

    def get_response(self, url, auth=None, http_method='get', **kwargs):
        # Get authentication values if not given
        if auth is None:
            auth = self.get_auth()

        # Then call requests to get the response from the given endpoint
        # URL optionally, with the additional requests parameters.
        error = None
        response = None
        try:
            response = requests.request(http_method, url, auth=auth, **kwargs)
            # Raise a requests HTTP exception with the appropriate reason
            # for 4xx and 5xx response status codes which is later caught
            # and passed back.
            response.raise_for_status()

            # Any other responses (e.g. 2xx and 3xx):
            if response.status_code != 200:
                error = '{} ({}).'.format(
                    self.response_error,
                    response.status_code,
                )

        except requests.HTTPError as exc:
            logger.exception(exc)
            error = (
                "Failed to execute query. "
                "Return Code: {} Reason: {}".format(
                    response.status_code,
                    response.text
                )
            )
        except requests.RequestException as exc:
            # Catch all other requests exceptions and return the error.
            logger.exception(exc)
            error = str(exc)

        # Return response and error.
        return response, error


query_runners = {}


def register(query_runner_class):
    global query_runners
    if query_runner_class.enabled():
        logger.debug("Registering %s (%s) query runner.", query_runner_class.name(), query_runner_class.type())
        query_runners[query_runner_class.type()] = query_runner_class
    else:
        logger.debug("%s query runner enabled but not supported, not registering. Either disable or install missing "
                     "dependencies.", query_runner_class.name())


def get_query_runner(query_runner_type, configuration):
    query_runner_class = query_runners.get(query_runner_type, None)
    if query_runner_class is None:
        return None

    return query_runner_class(configuration)


def get_configuration_schema_for_query_runner_type(query_runner_type):
    query_runner_class = query_runners.get(query_runner_type, None)
    if query_runner_class is None:
        return None

    return query_runner_class.configuration_schema()


def import_query_runners(query_runner_imports):
    for runner_import in query_runner_imports:
        __import__(runner_import)


def guess_type(string_value):
    val_type, val = guess_type_and_decode(string_value)

    return val_type


def guess_type_and_decode(string_value):
    if string_value == '' or string_value is None:
        return TYPE_STRING, string_value

    if type(string_value) != str:
        string_value = str(string_value)

    try:
        val = float(string_value)
        if val.is_integer():
            return TYPE_INTEGER, int(val)

        return TYPE_FLOAT, val
    except (ValueError, OverflowError):
        pass

    if unicode(string_value).lower() in ('true', 'false'):
        return TYPE_BOOLEAN, unicode(string_value).lower() == 'true'

    try:
        val = parser.parse(string_value)
        return TYPE_DATETIME, val
    except (ValueError, OverflowError):
        pass

    return TYPE_STRING, string_value


def default_value_for_type(tp):
    if tp == TYPE_FLOAT or tp == TYPE_INTEGER:
        return 0

    if tp == TYPE_BOOLEAN:
        return 'false'

    return ""


def get_column_type_from_set(type_set):
    if len(type_set) == 1:
        return type_set.pop()
    elif len(type_set) == 2 \
            and TYPE_FLOAT in type_set \
            and TYPE_INTEGER in type_set:
        return TYPE_FLOAT
    else:
        return TYPE_STRING


def handle_extra_columns(data, extra_columns):
    if extra_columns is None:
        extra_columns = []

    columns = data['columns']
    rows = data['rows']

    column_name_set = set()
    for column in columns:
        column_name_set.add(column['name'])

    for extra_column_def in extra_columns:
        if (type(extra_column_def) is OrderedDict or type(extra_column_def) is dict) \
                and "expr" in extra_column_def \
                and "name" in extra_column_def:
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
                {'name': name, 'friendly_name': name, 'type': get_column_type_from_set(column_data_type)})
        else:
            return "Unexpected extra column definition!", None

    return None, {'columns': columns, 'rows': rows}


def handle_select_and_ordering(data, selected_columns_set, ordered_columns_list):
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


def handle_alias(data, alias_mapping):
    columns = data['columns']
    rows = data['rows']

    for column in columns:
        if column['name'] in alias_mapping:
            column['friendly_name'] = alias_mapping[column['name']]

    return {'columns': columns, 'rows': rows}
