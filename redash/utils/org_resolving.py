import logging

from flask import request, g
from redash.models import Organization

logger = logging.getLogger(__name__)
from werkzeug.local import LocalProxy


def _get_current_org():
    if 'org' in g and g.org is not None:
        return g.org

    slug = "default"

    g.org = Organization.get_by_slug(slug)
    logging.debug("Current organization: %s (slug: %s)", g.org, slug)
    return g.org


current_org = LocalProxy(_get_current_org)
