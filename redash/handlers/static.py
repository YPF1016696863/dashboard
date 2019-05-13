from flask import safe_join, send_file
from flask_login import login_required

from redash import settings
from redash.handlers.base import org_scoped_rule, routes


def render_index():
    full_path = safe_join(settings.STATIC_ASSETS_PATH, 'index.html')
    response = send_file(full_path, **dict(cache_timeout=0, conditional=True))

    return response


@routes.route(org_scoped_rule('/<path:path>'))
@routes.route(org_scoped_rule('/'))
@login_required
def index(**kwargs):
    return render_index()
