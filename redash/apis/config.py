import logging

from redash.handlers import routes
from redash.handlers.base import json_response
from redash.utils.org_resolving import current_org
from redash.utils.client_config import client_config

logger = logging.getLogger(__name__)


@routes.route('/api/config', methods=['GET'])
def config():
    return json_response({
        'org_slug': current_org.slug,
        'client_config': client_config()
    })