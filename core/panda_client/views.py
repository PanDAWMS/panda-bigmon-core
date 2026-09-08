import json
import logging
import markdown
from django.apps import apps
from django.http import HttpResponse, JsonResponse

from core.oauth.decorators import login_required
from core.panda_client.utils import get_auth_indigoiam, kill_task, finish_task, set_debug_mode, to_bool, get_user_groups
from core.panda_client.ask_panda import AskPanda
from core.utils import error_response
from core.views import initRequest

_logger = logging.getLogger('panda.client')


@login_required
def client(request, task_id=None):
    valid, response = initRequest(request)
    if not valid:
        return response

    info = {'redirect': 'false'}

    data = request.session.get('requestParams') or {}

    if not data:
        info['text'] = 'Request body is empty'
        return HttpResponse(json.dumps(info), content_type='text/html')

    jeditaskid = data.get('taskID') or data.get('taskid')

    action = data.get('action')

    try:
        if action == 'finishtask' and jeditaskid:
            info['text'] = finish_task(request=request, jeditaskid=jeditaskid)

        elif action == 'killtask' and jeditaskid:
            info['text'] = kill_task(request=request, jeditaskid=jeditaskid)

        elif action == 'setdebugmode' and data.get('pandaid') is not None:
            auth = get_auth_indigoiam(request) or {}
            bearer = auth.get('Authorization')

            modeOn = False
            if data.get('params'):
                params = json.loads(data['params'])
                if params.get('modeOn') is not None:
                    modeOn = to_bool(params['modeOn'])

            groups = get_user_groups(bearer) if bearer else []

            info['text'] = set_debug_mode(
                request=request,
                job_id=data['pandaid'],
                mode=modeOn,
                user_id=getattr(request.user, 'id', None),
                groups=groups,
            )
            if 'Succeeded' in (info['text'] or '') and modeOn:
                info['redirect'] = 'true'
            else:
                info['redirect'] = 'false'

        else:
            if not jeditaskid and action in ('finishtask', 'killtask'):
                info['text'] = 'Error! JeditaskID is none'
            else:
                info['text'] = 'Operation error'

    except Exception as e:
        _logger.exception("Error in client view")
        info['text'] = f'Operation failed: {e}'

    return HttpResponse(json.dumps(info), content_type='text/html')


@login_required
def job_error_analysis(request, analysis_id:str|None=None):
    """Handles job error analysis done by AskPanda"""
    valid, response = initRequest(request)
    if not valid:
        return response

    if not analysis_id:
        return error_response(request, "No job id provided", 400)

    authz = apps.get_app_config("oauth").authz
    if not authz.enforce(list(request.user.groups.values_list('name', flat=True)), 'error_analysis', 'read', {}, {}):
        return error_response(request, "You are not authorized to access this resource", 403)

    try:
        ask_panda = AskPanda(request.user.username)
    except ValueError as e:
        _logger.error(f"AskPanda initialization failed: {e}")
        return error_response(request, "AskPanda service is misconfigured on the server", 500)

    if analysis_id == '-1':
        request_params = request.session.get('requestParams', {})
        pandaid = request_params.get('pandaid')
        if not pandaid:
            return error_response(request, "No pandaid provided", 400)
        res = ask_panda.start_job_error_analysis(pandaid)
    else:
        res = ask_panda.get_analysis_result(analysis_id)

    # convert markdown to HTML
    if res.get("success") and res.get("data"):
        raw_markdown = res["data"].get("answer_markdown")
        if raw_markdown:
            # Enable extra extensions like tables, code blocks, etc.
            res["data"]["answer_html"] = markdown.markdown(
                raw_markdown,
                extensions=['fenced_code', 'tables', 'nl2br']
            )
            del res["data"]["answer_markdown"]

    status_code = res.get("status_code", 200) if res.get("success") else (res.get("status_code") or 502)
    return JsonResponse({'result': res}, status=status_code)


@login_required
def submit_job_error_rating(request, analysis_id:str):
    """Submits user rating for a completed analysis."""
    valid, response = initRequest(request)
    if not valid:
        return response
    if request.method != "POST":
        return error_response(request, "Method not allowed", 405)

    # validate rating
    if 'rating' in request.session['requestParams']:
        rating = request.session['requestParams']['rating']
    else:
        return error_response(request, "Rating is required", 400)
    try:
        rating = int(rating)
    except (ValueError, TypeError, json.JSONDecodeError):
        return error_response(request, "Invalid JSON payload", 400)
    if rating < 0 or rating > 5:
        return error_response(request, "Invalid rating value, it must be between 1 and 5", 400)

    # submit rating to AskPanda
    try:
        ask_panda = AskPanda(username=request.user.username)
        res = ask_panda.submit_rating(analysis_id=analysis_id, rating=rating)
    except ValueError as e:
        return error_response(request, str(e), 500)

    status_code = res.get("status_code") if res.get("success") else 502
    return JsonResponse({'result': res}, status=status_code)