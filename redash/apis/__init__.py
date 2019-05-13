from flask import Blueprint

from redash.apis.api import api

routes = Blueprint('redash', __name__)


def init_app(app):
    from redash.apis import admin, authentication, config, queries, static, organization
    app.register_blueprint(routes)
    api.init_app(app)
    authentication.init_app(app)