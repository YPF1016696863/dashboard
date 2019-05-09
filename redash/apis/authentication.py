import logging

from flask import request
from flask_login import LoginManager, current_user, login_required, login_user, logout_user
from redash import limiter, models, settings
from redash.handlers import routes
from redash.handlers.base import json_response, json_response_with_status
from redash.utils.org_resolving import current_org
from redash.utils.client_config import client_config

login_manager = LoginManager()

logger = logging.getLogger(__name__)


@routes.route('/api/login', methods=['POST'])
@limiter.limit(settings.THROTTLE_LOGIN_PATTERN)
def login():
    login_info = request.get_json(True)

    user = models.User.get_by_email(login_info["email"])
    if user and not user.is_disabled and user.verify_password(login_info['password']):
        login_user(user, remember=("remember" in login_info and login_info["remember"]))
        return json_response({})
    else:
        return json_response_with_status({
            'error': 'LOGIN_FAILED'
        }, 401)


@routes.route('/api/logout')
def logout():
    logout_user()
    return json_response({})


def __messages():
    messages = []

    if not current_user.is_email_verified:
        messages.append('email-not-verified')

    return messages


@routes.route('/api/session', methods=['GET'])
@login_required
def session():
    user = {
        'profile_image_url': current_user.profile_image_url,
        'id': current_user.id,
        'name': current_user.name,
        'email': current_user.email,
        'groups': current_user.group_ids,
        'permissions': current_user.permissions
    }

    return json_response({
        'user': user,
        'messages': __messages(),
        'org_slug': current_org.slug,
        'client_config': client_config()
    })


@login_manager.user_loader
def load_user(user_id_with_identity):
    org = current_org._get_current_object()
    user_id, _ = user_id_with_identity.split("-")

    try:
        user = models.User.get_by_id_and_org(user_id, org)
        if user.is_disabled:
            return None

        if user.get_id() != user_id_with_identity:
            return None

        return user
    except models.NoResultFound:
        return None


@login_manager.unauthorized_handler
def handle_unauthorized():
    return json_response_with_status({
        'error': 'UNAUTHORIZED'
    }, 401)


def init_app(app):
    login_manager.init_app(app)
    login_manager.anonymous_user = models.AnonymousUser