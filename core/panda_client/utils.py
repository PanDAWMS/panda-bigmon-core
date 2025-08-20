import time
import logging
import jwt
from typing import Any, Dict, Tuple, Optional

from requests import post
from core.oauth.utils import get_auth_provider
from django.conf import settings

from .http_client import HttpClient, api_url_ssl

_logger = logging.getLogger("panda.client")


def to_bool(value: Any) -> bool:
    """Convert a string to a boolean value."""
    s = str(value).strip().lower()
    if s == "true":
        return True
    if s == "false":
        return False
    raise ValueError(f"Invalid value for boolean conversion: {value!r}")


def _get_full_url(command: str) -> str:
    """Build a legacy Panda server URL (non-Task-API endpoints)."""
    if hasattr(settings, "PANDA_SERVER_URL") and settings.PANDA_SERVER_URL:
        return f"{settings.PANDA_SERVER_URL}/{command}"
    raise Exception("PANDA_SERVER_URL attribute does not exist in settings")

def get_auth_indigoiam(request) -> Dict[str, str]:
    """
    Get authentication headers from the IndigoIAM token stored in the user session.

    Returns:
        { "Authorization": "Bearer <id_token>", "Origin": "atlas" }
    or
        { "detail": "..." } with a human-readable error reason.
    """
    header: Dict[str, str] = {}
    organisation = "atlas"

    auth_provider = get_auth_provider(request)
    if not auth_provider:
        return {"detail": "Authentication is required. Please sign in with IAM (green button)."}

    try:
        social = request.user.social_auth.get(provider=auth_provider)
    except Exception:
        return {"detail": "Authentication provider not found for user session. Please re-login with IAM."}

    if auth_provider != "indigoiam":
        return {
            "detail": "This action is only available for token-based authentication. "
                      "Please re-login with the green 'Sign in with IAM' option."
        }

    # Token expiration check
    try:
        auth_time = int(social.extra_data["auth_time"])
        expires_in = int(social.extra_data["expires_in"])
        if (auth_time + expires_in - 10) <= int(time.time()):
            return {"detail": "ID token is expired. Please re-login with IAM."}
        id_token = social.extra_data["id_token"]
    except Exception:
        return {"detail": "Failed to read IAM token from session. Please re-login with IAM."}

    header["Authorization"] = f"Bearer {id_token}"
    header["Origin"] = organisation
    return header

def make_http_client_from_request(request) -> Tuple[Optional[HttpClient], Optional[str]]:
    """
    Returns (client, error).

    If error is not None — it contains a human-readable reason
    (as provided by get_auth_indigoiam).
    """
    auth = get_auth_indigoiam(request)
    if "Authorization" not in auth:
        return None, auth.get("detail", "Authentication required")

    token = auth["Authorization"].split(" ", 1)[1]
    vo = auth.get("Origin", "atlas")

    client = HttpClient()
    client.override_oidc(True, token, vo)
    return client, None


def _http_post(request, path: str, data: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """Perform an authenticated POST request to the Task API."""
    url = f"{api_url_ssl}{path}"
    client, err = make_http_client_from_request(request)
    if err:
        return 401, {"detail": err}
    try:
        return client.post(url, data)
    except Exception as ex:
        _logger.exception("POST %s failed: %s", url, str(ex))
        return 500, {"detail": f"Request failed: {str(ex)}"}


def _http_get(request, path: str, params: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """Perform an authenticated GET request to the Task API."""
    url = f"{api_url_ssl}{path}"
    client, err = make_http_client_from_request(request)
    if err:
        return 401, {"detail": err}
    try:
        return client.get(url, params)
    except Exception as ex:
        _logger.exception("GET %s failed: %s", url, ex)
        return 500, {"detail": f"Request failed: {ex}"}


def _extract_task_id(task_id: Optional[int] = None, jeditaskid: Optional[int] = None) -> int:
    """Normalize `task_id` and `jeditaskid` into a single integer."""
    tid = task_id if task_id is not None else jeditaskid
    if tid is None:
        raise ValueError("task_id (or jeditaskid) must be provided")
    return int(tid)


def pause_task(request, *, task_id: Optional[int] = None, jeditaskid: Optional[int] = None, **_kwargs):
    """Pause a JEDI task."""
    tid = _extract_task_id(task_id, jeditaskid)
    return _http_post(request, "/task/pause", {"task_id": tid})


def kill_task(request, *, task_id: Optional[int] = None, jeditaskid: Optional[int] = None, **_kwargs):
    """Kill a JEDI task."""
    tid = _extract_task_id(task_id, jeditaskid)
    return _http_post(request, "/task/kill", {"task_id": tid})


def finish_task(
    request,
    *,
    task_id: Optional[int] = None,
    jeditaskid: Optional[int] = None,
    soft: bool = False,
    broadcast: bool = False,
    **_kwargs,
):
    """Finish a JEDI task."""
    tid = _extract_task_id(task_id, jeditaskid)
    payload = {"task_id": tid, "soft": bool(soft), "broadcast": bool(broadcast)}
    return _http_post(request, "/task/finish", payload)


def set_debug_mode(auth: Dict[str, str], **kwargs) -> str:
    """
    Legacy Panda server endpoint: set debug mode for a job.

    Kwargs:
        pandaid (int): ID of the job to debug (required)
        modeOn (bool): True/False to enable/disable debug (required)
        user_id (int): For logging only (optional)
        groups (Iterable[str]): User groups, may affect Origin (optional)
    """
    pandaid = kwargs.get("pandaid")
    mode_on = kwargs.get("modeOn")

    if pandaid is None:
        return "PandaID is not defined"
    if mode_on is None:
        return "ModeOn is not defined"

    data = {"pandaID": pandaid, "modeOn": bool(mode_on)}

    groups = set(kwargs.get("groups") or [])
    if groups:
        auth = dict(auth)  # copy to avoid mutating caller's dict
        auth["Origin"] = "atlas.production" if "atlas/production" in groups else "atlas"

    url = _get_full_url("setDebugMode")

    try:
        resp = post(url, headers=auth, data=data, timeout=30)
        text = resp.text
        status = resp.status_code
    except Exception as ex:
        text = f"ERROR to set debug mode: {ex}"
        status = -1

    try:
        _logger.debug(
            "SetDebugMode | URL: %s | HTTP: %s | Response: %s | user_id: %s | Origin: %s | PandaID: %s | ModeOn: %s | Groups: %s",
            url,
            status,
            (text[:500] + "...") if len(text) > 500 else text,
            kwargs.get("user_id"),
            auth.get("Origin"),
            data["pandaID"],
            data["modeOn"],
            sorted(groups) if groups else [],
        )
    except Exception:
        pass
    return text

def get_user_groups(idtoken: str):
    """Extract the 'groups' claim from an IAM JWT without verifying the signature."""
    token = idtoken.split(" ", 1)[1] if " " in idtoken else idtoken
    decoded = jwt.decode(token, verify=False, options={"verify_signature": False})
    return decoded.get("groups", [])