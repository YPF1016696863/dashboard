import logging

from flask_login import login_required

from redash import models, redis_connection
from redash.apis import routes, json_response
from redash.monitor import celery_tasks, get_status
from redash.permissions import require_super_admin
from redash.serializers import QuerySerializer
from redash.utils import json_loads

logger = logging.getLogger(__name__)


@routes.route('/api/admin/queries/outdated', methods=['GET'])
@require_super_admin
@login_required
def outdated_queries():
    manager_status = redis_connection.hgetall('redash:status')
    query_ids = json_loads(manager_status.get('query_ids', '[]'))
    if query_ids:
        queries = (
            models.Query.query.outerjoin(models.QueryResult)
                .filter(models.Query.id.in_(query_ids))
                .order_by(models.Query.created_at.desc())
        )
    else:
        queries = []

    response = {
        'queries': QuerySerializer(queries, with_stats=True, with_last_modified_by=False).serialize(),
        'updated_at': manager_status.get("last_refresh_at", None),
    }
    return json_response(response)


@routes.route('/api/admin/queries/tasks', methods=['GET'])
@require_super_admin
@login_required
def queries_tasks():
    response = {
        'tasks': celery_tasks(),
    }

    return json_response(response)


@routes.route('/api/status')
@login_required
@require_super_admin
def status_api():
    status = get_status()
    return json_response(status)