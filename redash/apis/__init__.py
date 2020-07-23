from flask import Blueprint
from flask import current_app

from redash import settings
from redash.apis.api import api
from redash.utils import json_dumps

routes = Blueprint('redash', __name__)


def json_response(response):
    return current_app.response_class(json_dumps(response), mimetype='application/json')


def json_response_with_status(response, status):
    return current_app.response_class(json_dumps(response), status=status, mimetype='application/json')


def init_app(app):
    from redash.apis import admin, authentication, config, queries, organization, file_upload, image_upload
    app.config.setdefault('MAX_CONTENT_LENGTH', settings.FILE_UPLOAD_MAX_CONTENT_LENGTH)

    app.register_blueprint(routes)
    api.init_app(app)
    authentication.init_app(app)
