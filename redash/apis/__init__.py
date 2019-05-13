from flask import Blueprint

from redash.apis.api import api

routes = Blueprint('redash', __name__)


def init_app(app):
    from redash.apis import admin, authentication, config, queries, static, organization, embed
    app.register_blueprint(routes)
    api.init_app(app)