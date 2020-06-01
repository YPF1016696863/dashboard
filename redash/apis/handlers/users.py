from disposable_email_domains import blacklist
from flask import request
from flask_login import current_user, login_user
from flask_restful import abort
from funcy import partial
from funcy import project
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound

from redash import models, limiter
from redash import settings
from redash.apis.handlers.base import BaseResource, require_fields, get_object_or_404, paginate, \
    order_results as _order_results
from redash.permissions import require_permission, require_admin_or_owner, is_admin_or_owner, \
    require_permission_or_owner, require_admin
from redash.settings import parse_boolean
from redash.utils.account import invite_link_for_user, reset_link_for_user, verify_link_for_user, \
    send_invite_email, send_password_reset_email, send_verify_email, validate_token, SignatureExpired, BadSignature

# Ordering map for relationships
order_map = {
    'name': 'name',
    '-name': '-name',
    'active_at': 'active_at',
    '-active_at': '-active_at',
    'created_at': 'created_at',
    '-created_at': '-created_at',
    'groups': 'group_ids',
    '-groups': '-group_ids',
}

order_results = partial(
    _order_results,
    default_order='-created_at',
    allowed_orders=order_map,
)


def invite_user(inviter, user, send_email=True):
    d = user.to_dict()

    # invite_url = invite_link_for_user(user)
    #if settings.email_server_is_configured() and send_email:
    #    send_invite_email(inviter, user, invite_url)
    #else:
    #    d['invite_link'] = invite_url

    return d


def verify_token(token):
    try:
        user_id = validate_token(token)
        user = models.User.get_by_id(user_id)
    except (SignatureExpired, BadSignature, NoResultFound):
        abort(403, message="Bad token or user id not found in token!")

    return user


def reset_password_with_token(token, invite_flag=False):
    user = verify_token(token)

    if invite_flag and not user.details.get('is_invitation_pending'):
        abort(400, message='Invitation already accepted!')

    req = request.get_json(force=True)
    require_fields(req, ['password'])
    if len(req['password']) < 6:
        abort(400, message='Bad password.')

    if invite_flag:
        user.is_invitation_pending = False
        user.is_email_verified = True

    user.hash_password(req['password'])
    models.db.session.add(user)
    models.db.session.commit()


class UserListResource(BaseResource):
    decorators = BaseResource.decorators + \
                 [limiter.limit('200/day;50/hour', methods=['POST'])]

    def get_users(self, disabled, pending, search_term):
        if disabled:
            users = models.User.all_disabled(self.current_org)
        else:
            users = models.User.all(self.current_org)

        if pending is not None:
            users = models.User.pending(users, pending)

        if search_term:
            users = models.User.search(users, search_term)
            self.record_event({
                'action': 'search',
                'object_type': 'user',
                'term': search_term,
                'pending': pending,
            })
        else:
            self.record_event({
                'action': 'list',
                'object_type': 'user',
                'pending': pending,
            })

        # order results according to passed order parameter,
        # special-casing search queries where the database
        # provides an order by search rank
        return order_results(users, fallback=not bool(search_term))

    @require_permission('list_users')
    def get(self):
        page = request.args.get('page', 1, type=int)
        page_size = request.args.get('page_size', 25, type=int)

        groups = {group.id: group for group in models.Group.all(self.current_org)}

        def serialize_user(user):
            d = user.to_dict()
            user_groups = []
            for group_id in set(d['groups']):
                group = groups.get(group_id)

                if group:
                    user_groups.append({'id': group.id, 'name': group.name})

            d['groups'] = user_groups

            return d

        search_term = request.args.get('q', '')

        disabled = request.args.get('disabled', 'false')  # get enabled users by default
        disabled = parse_boolean(disabled)

        pending = request.args.get('pending', None)  # get both active and pending by default
        if pending is not None:
            pending = parse_boolean(pending)

        users = self.get_users(disabled, pending, search_term)

        return paginate(users, page, page_size, serialize_user)

    @require_admin
    def post(self):
        req = request.get_json(force=True)
        require_fields(req, ('name', 'email'))

        user = models.User(org=self.current_org,
                           name=req['name'],
                           email=req['email'],
                           is_invitation_pending=False,
                           group_ids=[])
        
        user.hash_password('Abcd1234')

        try:
            models.db.session.add(user)
            models.db.session.commit()
        except IntegrityError as e:
            if "email" in e.message:
                abort(400, message='Username already taken.')
            abort(500)

        self.record_event({
            'action': 'create',
            'object_id': user.id,
            'object_type': 'user'
        })

        should_send_invitation = 'no_invite' not in request.args
        return invite_user(self.current_user, user, send_email=should_send_invitation)


class UserInviteResource(BaseResource):
    @require_admin
    def post(self, user_id):
        user = models.User.get_by_id(user_id)
        return invite_user(self.current_user, user)


class UserResetPasswordResource(BaseResource):
    @require_admin
    def post(self, user_id):
        user = models.User.get_by_id(user_id)

        req = request.get_json(True)

        params = project(req, ('password'))

        if user.is_disabled:
            abort(404, message='Not found')

        if 'password' not in params:
            user.hash_password('Abcd1234')
        else:
            user.hash_password(params.pop('password'))

        return {
            'status': 'ok'
        }


class UserVerifyEmailResource(BaseResource):
    def post(self, user_id):
        user = models.User.get_by_id(user_id)
        if user.is_disabled:
            abort(404, message='Not found')

        verify_link = verify_link_for_user(user)
        email_sent = False

        if settings.email_server_is_configured():
            send_verify_email(user, verify_link)
            email_sent = True

        return {
            'verify_link': verify_link,
            'email_sent': email_sent
        }


class UserVerifyTokenResource(BaseResource):
    def post(self, token):
        user = verify_token(token)
        return {
            'status': 'OK',
            'user_id': user.id
        }


class UserAcceptInvitationWithTokenResource(BaseResource):
    def post(self, token):
        reset_password_with_token(token, True)

        return {
            'status': 'OK',
        }


class UserResetPasswordWithTokenResource(BaseResource):
    def post(self, token):
        reset_password_with_token(token, False)

        return {
            'status': 'OK',
        }


class UserVerifyEmailWithTokenResource(BaseResource):
    def post(self, token):
        user = verify_token(token)

        user.is_email_verified = True
        models.db.session.add(user)
        models.db.session.commit()

        return {
            'status': 'OK',
        }


class UserRegenerateApiKeyResource(BaseResource):
    def post(self, user_id):
        user = models.User.get_by_id(user_id)
        if user.is_disabled:
            abort(404, message='Not found')
        if not is_admin_or_owner(user_id):
            abort(403)

        user.regenerate_api_key()
        models.db.session.commit()

        self.record_event({
            'action': 'regnerate_api_key',
            'object_id': user.id,
            'object_type': 'user'
        })

        return user.to_dict(with_api_key=True)


class UserResource(BaseResource):
    decorators = BaseResource.decorators + \
                 [limiter.limit('50/hour', methods=['POST'])]

    def get(self, user_id):
        require_permission_or_owner('list_users', user_id)

        user = models.User.get_by_id(user_id)
        if user is None:
            abort(404, "User not found")

        self.record_event({
            'action': 'view',
            'object_id': user_id,
            'object_type': 'user',
        })

        return user.to_dict(with_api_key=is_admin_or_owner(user_id))

    def post(self, user_id):
        require_admin_or_owner(user_id)
        user = models.User.get_by_id(user_id)

        req = request.get_json(True)

        params = project(req, ('email', 'name', 'password', 'old_password', 'group_ids'))

        if 'password' in params and 'old_password' not in params:
            abort(403, message="Must provide current password to update password.")

        if 'old_password' in params and not user.verify_password(params['old_password']):
            abort(403, message="Incorrect current password.")

        if 'password' in params:
            user.hash_password(params.pop('password'))
            params.pop('old_password')

        if 'group_ids' in params:
            if not self.current_user.has_permission('admin'):
                abort(403, message="Must be admin to change groups membership.")

            for group_id in params['group_ids']:
                try:
                    models.Group.get_by_id_and_org(group_id, self.current_org)
                except NoResultFound:
                    abort(400, message="Group id {} is invalid.".format(group_id))

            #if len(params['group_ids']) == 0:
            #    params.pop('group_ids')

        try:
            self.update_model(user, params)
            models.db.session.commit()

            # The user has updated their email or password. This should invalidate all _other_ sessions,
            # forcing them to log in again. Since we don't want to force _this_ session to have to go
            # through login again, we call `login_user` in order to update the session with the new identity details.
            if current_user.id == user.id:
                login_user(user, remember=True)
        except IntegrityError as e:
            if "email" in e.message:
                message = "Username already taken."
            else:
                message = "Error updating record"

            abort(400, message=message)

        self.record_event({
            'action': 'edit',
            'object_id': user.id,
            'object_type': 'user',
            'updated_fields': params.keys()
        })

        return user.to_dict(with_api_key=is_admin_or_owner(user_id))

    @require_admin
    def delete(self, user_id):
        user = models.User.get_by_id(user_id)
        # admin cannot delete self; current user is an admin (`@require_admin`)
        # so just check user id
        if user.id == current_user.id:
            abort(403, message="You cannot delete your own account. "
                               "Please ask another admin to do this for you.")
        elif not user.is_invitation_pending:
            abort(403, message="You cannot delete activated users. "
                               "Please disable the user instead.")
        models.db.session.delete(user)
        models.db.session.commit()

        return user.to_dict(with_api_key=is_admin_or_owner(user_id))


class UserDisableResource(BaseResource):
    @require_admin
    def post(self, user_id):
        user = models.User.get_by_id(user_id)
        # admin cannot disable self; current user is an admin (`@require_admin`)
        # so just check user id
        if user.id == current_user.id:
            abort(403, message="You cannot disable your own account. "
                               "Please ask another admin to do this for you.")
        user.disable()
        models.db.session.commit()

        return user.to_dict(with_api_key=is_admin_or_owner(user_id))

    @require_admin
    def delete(self, user_id):
        user = models.User.get_by_id(user_id)
        user.enable()
        models.db.session.commit()

        return user.to_dict(with_api_key=is_admin_or_owner(user_id))
