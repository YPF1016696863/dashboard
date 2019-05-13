from redash.apis.api import api
from redash.handlers.base import routes


def init_app(app):
    from redash.apis import admin, authentication, config, queries
    from redash.handlers import embed, queries, static, organization
    app.register_blueprint(routes)
    api.init_app(app)