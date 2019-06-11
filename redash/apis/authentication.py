import logging

from flask import g, request
from flask_login import LoginManager, current_user, login_required, login_user, logout_user
from sqlalchemy.orm.exc import NoResultFound

from redash import limiter, models, settings
from redash.apis import routes, json_response, json_response_with_status
from redash.models import Group, Organization, User, db
from redash.utils.client_config import client_config
from redash.utils.org_resolving import current_org

login_manager = LoginManager()

logger = logging.getLogger(__name__)


def create_org(org_name, user_name, email, password):
    default_org = Organization(name=org_name, slug='default', settings={})
    admin_group = Group(name='admin', permissions=['admin', 'super_admin'], org=default_org, type=Group.BUILTIN_GROUP)
    default_group = Group(name='default', permissions=Group.DEFAULT_PERMISSIONS, org=default_org,
                          type=Group.BUILTIN_GROUP)

    db.session.add_all([default_org, admin_group, default_group])
    db.session.commit()

    user = User(org=default_org,
                name=user_name,
                email=email,
                group_ids=[admin_group.id, default_group.id])
    user.hash_password(password)

    db.session.add(user)
    db.session.commit()

    return default_org, user


@routes.route('/api/setup', methods=['POST'])
def setup():
    # local proxy needs to use != to check None
    if current_org != None:
        return json_response_with_status({
            'error': 'SETUP_NOT_ALLOWED'
        }, 403)

    setup_info = request.get_json(True)
    default_org, user = create_org(setup_info["org_name"], setup_info["name"], setup_info["email"],
                                   setup_info["password"])
    g.org = default_org
    login_user(user)

    return json_response({
        'status': 'OK'
    })


@routes.route('/api/login', methods=['POST'])
@limiter.limit(settings.THROTTLE_LOGIN_PATTERN)
def login():
    login_info = request.get_json(True)

    user = models.User.get_by_email(login_info["email"])
    if user and not user.is_disabled and user.verify_password(login_info['password']):
        login_user(user, remember=("remember" in login_info and login_info["remember"]))
        return json_response({
            'status': 'OK'
        })
    else:
        return json_response_with_status({
            'error': 'LOGIN_FAILED'
        }, 401)


@routes.route('/api/logout', methods=['POST'])
def logout():
    logout_user()
    return json_response({
        'status': 'OK'
    })


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
    except NoResultFound:
        return None


@login_manager.unauthorized_handler
def handle_unauthorized():
    return json_response_with_status({
        'error': 'UNAUTHORIZED'
    }, 401)


def init_app(app):
    login_manager.init_app(app)
    login_manager.anonymous_user = models.AnonymousUser
