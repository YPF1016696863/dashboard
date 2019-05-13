from flask import Flask
from werkzeug.contrib.fixers import ProxyFix

from . import settings


class Redash(Flask):
    """A custom Flask app for Redash"""

    def __init__(self, *args, **kwargs):
        kwargs.update({
            'template_folder': settings.STATIC_ASSETS_PATH,
            'static_folder': settings.STATIC_ASSETS_PATH,
            'static_path': '/static',
        })
        super(Redash, self).__init__(__name__, *args, **kwargs)
        # Make sure we get the right referral address even behind proxies like nginx.
        self.wsgi_app = ProxyFix(self.wsgi_app, settings.PROXIES_COUNT)
        # Configure Redash using our settings
        self.config.from_object('redash.settings')


def create_app():
    from . import extensions, apis, limiter, mail, migrate, security
    from .destinations import import_destinations
    from .metrics import request as request_metrics
    from .models import db, users
    from .query_runner import import_query_runners

    app = Redash()

    # Load query runners and destinations
    import_query_runners(settings.QUERY_RUNNERS)
    import_destinations(settings.DESTINATIONS)

    security.init_app(app)
    request_metrics.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)
    mail.init_app(app)
    limiter.init_app(app)
    apis.init_app(app)
    extensions.init_app(app)
    users.init_app(app)

    return app
