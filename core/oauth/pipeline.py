import base64
import json
import logging
from django.contrib.auth import get_user_model
from django.db import transaction
from social_django.models import UserSocialAuth
from core.oauth.models import Group, BPToken

_logger = logging.getLogger('social')


def merge_social_users(details, *args, **kwargs):
    """
    Merge duplicate Django users that share the same email across different
    social-auth providers, so that all social accounts are linked to a single
    canonical application user.
    Args:
        details (dict): user details extracted from the provider; must contain
            the email field.
    Returns:
        dict | None:
            - {"user": primary_user} if a user with the given email exists
              (after merging duplicates if needed).
            - None if no email is provided or no matching users exist.
    """
    email = details.get("email", None)
    if not email:
        return None

    auth_user_model = get_user_model()
    users = list(auth_user_model.objects.filter(email__iexact=email).values('id'))
    if len(users) == 0:
        return None

    user_ids = sorted([u['id'] for u in users])
    primary_user_id = user_ids[0]
    duplicates = user_ids[1:]
    if len(duplicates) > 0:
        with transaction.atomic():
            # update associated user for all other providers
            UserSocialAuth.objects.filter(user_id__in=duplicates).update(user_id=primary_user_id)
            # mark duplicated users as inactive to clean up later
            auth_user_model.objects.filter(id__in=duplicates).update(is_active=0)
        _logger.info(f"Found {len(duplicates)} social user duplicates -> merged them with {primary_user_id} user_id")

    # pass primary user object into pipeline
    primary_user = auth_user_model.objects.get(id=primary_user_id)
    return {"user": primary_user}


def sync_user_groups(backend, user, social,  *args, **kwargs):
    """
    Update user groups provided in access token. Because there are several supported backends,
        we need to make sure the groups data is not interfere with each other.
        Roles from CERN SSO - does not have / in their names. Groups from IAM start with / .
    Args:
        backend: authentication backend (provider) instance.
        user: Django user instance.
        social: UserSocialAuth instance with extra_data field containing access token.
    Returns:
        None - pipeline will continue
    """
    user_groups = []
    token_dict = {}
    roles_key = None
    groups_user_registered = []
    if backend.name == 'cernoidc':
        roles_key = 'cern_roles'
        groups_user_registered = [g.name for g in user.groups.exclude(name__contains='/')]
    elif backend.name == 'indigoiam':
        roles_key = 'wlcg.groups'
        groups_user_registered = [g.name for g in user.groups.filter(name__contains='/')]
    try:
        token_dict = json.loads(base64.b64decode(social.extra_data['access_token'].split('.')[1] + '==').decode('utf-8'))
    except Exception as ex:
        _logger.warning(f"Failed to decode access token for user {user} from {backend.name}: {ex}")
    if roles_key is not None and roles_key in token_dict and len(token_dict[roles_key]) >= 0:
        user_groups = token_dict[roles_key]
    _logger.info(f"User {user} groups from {backend.name} token: {user_groups}, registered groups: {groups_user_registered}")
    if set(user_groups) == set(groups_user_registered):
        return None

    # if any new groups - add them and register user in them
    groups_all = []
    if backend.name == 'cernoidc':
        groups_all = [g.name for g in Group.objects.exclude(name__contains='/')]
    elif backend.name == 'indigoiam':
        groups_all = [g.name for g in Group.objects.filter(name__contains='/')]
    groups_new = list(set(user_groups) - set(groups_all))
    if len(groups_new) > 0:
        try:
            Group.objects.bulk_create([Group(name=g) for g in groups_new])
        except Exception as ex:
            _logger.warning(f"Failed to save new groups to DB: {ex}")
    groups_user_joined_recently = list(set(user_groups) - set(groups_user_registered))
    if len(groups_user_joined_recently) > 0:
        for g in groups_user_joined_recently:
            user.groups.add(Group.objects.get(name=g))
            user.save()
    _logger.info(f"Group membership for {user}, new groups: {groups_user_joined_recently}, joined groups: {groups_user_joined_recently}")

    # if the existing groups does not correspond with ones from token -> update
    groups_user_left = list(set(groups_user_registered) - set(user_groups))
    if len(groups_user_left) > 0:
        for g in groups_user_left:
            user.groups.remove(Group.objects.get(name=g))
            user.save()
    _logger.info(f"Group membership for {user}, left groups: {groups_user_left}")

    return None


def issue_user_token(strategy, backend, user=None, *args, **kwargs):
    """
    Issue a DRF Token for the authenticated user. This token can be used for subsequent API requests to authenticate the user.
    Args:
        strategy: the current social-auth strategy instance.
        backend: the authentication backend (provider) instance.
        user: a Django user instance.
        *args:
        **kwargs:
    Returns:
        None - the pipeline will continue.
    """
    if user:
        token, created = BPToken.objects.get_or_create(user=user)
        if created:
            _logger.info(f"Created new token for user {user.username}")
        else:
            _logger.info(f"Using existing token for user {user.username}")
        strategy.session_set("bp_token", token.key)
        strategy.session_set("auth_social_backend", backend.name)
    return None