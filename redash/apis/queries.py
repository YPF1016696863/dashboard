import sqlparse
from flask import jsonify, request
from flask_login import login_required

from redash import settings
from redash.handlers.base import (org_scoped_rule, routes)


@routes.route(org_scoped_rule('/api/queries/format'), methods=['POST'])
@login_required
def format_sql_query(org_slug=None):
    """
    Formats an SQL query using the Python ``sqlparse`` formatter.

    :<json string query: The SQL text to format
    :>json string query: Formatted SQL text
    """
    arguments = request.get_json(force=True)
    query = arguments.get("query", "")

    return jsonify({'query': sqlparse.format(query, **settings.SQLPARSE_FORMAT_OPTIONS)})