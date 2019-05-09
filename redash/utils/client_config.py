from redash import settings
from redash.utils.org_resolving import current_org


def date_time_format_config():
    date_format = current_org.get_setting('date_format')
    date_format_list = set(["DD/MM/YY", "MM/DD/YY", "YYYY-MM-DD", settings.DATE_FORMAT])
    time_format = current_org.get_setting('time_format')
    time_format_list = set(["HH:mm", "MM:mm:ss", "HH:mm:ss.SSS", settings.TIME_FORMAT])
    return {
        'dateFormat': date_format,
        'dateFormatList': list(date_format_list),
        'timeFormatList': list(time_format_list),
        'dateTimeFormat': "{0} {1}".format(date_format, time_format),
    }


def number_format_config():
    return {
        'integerFormat': current_org.get_setting('integer_format'),
        'floatFormat': current_org.get_setting('float_format'),
    }


def client_config():
    client_config = {}

    defaults = {
        'allowScriptsInUserInput': settings.ALLOW_SCRIPTS_IN_USER_INPUT,
        'allowCustomJSVisualizations': settings.FEATURE_ALLOW_CUSTOM_JS_VISUALIZATIONS,
        'autoPublishNamedQueries': settings.FEATURE_AUTO_PUBLISH_NAMED_QUERIES,
        'mailSettingsMissing': not settings.email_server_is_configured(),
        'dashboardRefreshIntervals': settings.DASHBOARD_REFRESH_INTERVALS,
        'queryRefreshIntervals': settings.QUERY_REFRESH_INTERVALS,
        'pageSize': settings.PAGE_SIZE,
        'pageSizeOptions': settings.PAGE_SIZE_OPTIONS,
        'tableCellMaxJSONSize': settings.TABLE_CELL_MAX_JSON_SIZE,
    }

    client_config.update(defaults)
    client_config.update(date_time_format_config())
    client_config.update(number_format_config())

    return client_config
