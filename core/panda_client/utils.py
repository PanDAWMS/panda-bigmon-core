import os
import time
import logging
import jwt
from typing import Any, Dict, Tuple, Optional, List, Union

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

def _ok(status, output) -> bool:
    return status == 0 and isinstance(output, dict) and output.get("success") is True

def _msg(output) -> str:
    if isinstance(output, dict):
        return str(output.get("message") or output.get("data") or "")
    return str(output)


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


def _guess_proxy_path() -> Optional[str]:
    """
    Try to find a usable x509 proxy path for certificate auth.
    Preference order:
      1) settings.X509_USER_PROXY
      2) env X509_USER_PROXY
      3) /tmp/x509up_u<uid>
    """
    candidates = [
        getattr(settings, "X509_USER_PROXY", None),
        os.environ.get("X509_USER_PROXY"),
        f"/tmp/x509up_u{os.getuid()}",
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return None


def _configure_cert_auth(client: HttpClient) -> None:
    """
    Configure client for certificate-based auth if the client exposes
    a conventional method/attribute. If not, do nothing (client may
    already pick up system/env defaults).
    """
    proxy_path = _guess_proxy_path()
    if not proxy_path:
        _logger.warning("x509 proxy not found; falling back to HttpClient defaults")

    # Common patterns we try conservatively:
    try:
        if hasattr(client, "set_cert") and callable(getattr(client, "set_cert")):
            client.set_cert(proxy_path)  # type: ignore[attr-defined]
            return
        if hasattr(client, "cert"):
            setattr(client, "cert", proxy_path)  # type: ignore[attr-defined]
            return
        if hasattr(client, "proxy_cert_path"):
            setattr(client, "proxy_cert_path", proxy_path)  # type: ignore[attr-defined]
            return
    except Exception as e:
        _logger.debug("Failed to set cert on HttpClient via reflective methods: %s", e)
    # If nothing matched, rely on HttpClient's internal defaults.


def make_http_client_from_request(request) -> Tuple[Optional[HttpClient], Optional[str]]:
    """
    Returns (client, error).

    If user logged in via IndigoIAM — use OIDC token.
    Otherwise — use certificate (x509 proxy).
    """
    auth_provider = get_auth_provider(request)

    client = HttpClient()

    if auth_provider == "indigoiam":
        # Token (OIDC) path
        auth = get_auth_indigoiam(request)

        client.ssl_key = None
        client.ssl_certificate = None

        if "Authorization" not in auth:
            # Keep prior behavior: explicit error if IAM flow is broken/expired
            return None, auth.get("detail", "Authentication required")
        token = auth["Authorization"].split(" ", 1)[1]
        vo = auth.get("Origin", "atlas")
        try:
            # Explicitly enable OIDC override for the Task API
            client.override_oidc(True, token, vo)
        except Exception as e:
            _logger.exception("Failed to enable OIDC on HttpClient: %s", e)
            return None, "Internal error: unable to configure OIDC client"
        return client, None

    # Certificate (x509) path
    try:
        _configure_cert_auth(client)
    except Exception as e:
        _logger.exception("Failed to configure certificate auth: %s", e)
        # For non-IAM users we still return the client — many HttpClient
        # implementations can auto-detect certs; we just log the problem.
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


def kill_task(
    request,
    *,
    task_id: Optional[int] = None,
    jeditaskid: Optional[int] = None,
    **_kwargs,
) -> str:
    """Kill a JEDI task and return a human-friendly message."""
    tid = _extract_task_id(task_id, jeditaskid)
    status, output = _http_post(request, "/task/kill", {"task_id": tid})
    if _ok(status, output):
        return f"Succeeded: task {tid} killed"
    return f"Failed: task {tid} kill — {_msg(output)}"


def finish_task(
    request,
    *,
    task_id: Optional[int] = None,
    jeditaskid: Optional[int] = None,
    soft: bool = False,
    broadcast: bool = False,
    **_kwargs,
) -> str:
    """Finish a JEDI task and return a human-friendly message."""
    tid = _extract_task_id(task_id, jeditaskid)
    payload = {"task_id": tid, "soft": bool(soft), "broadcast": bool(broadcast)}
    status, output = _http_post(request, "/task/finish", payload)
    if _ok(status, output):
        return f"Succeeded: task {tid} finished"
    return f"Failed: task {tid} finish — {_msg(output)}"


def set_debug_mode(request, **kwargs) -> str:
    """
    PanDA API (v1): POST /job/set_debug_mode
    Toggle debug mode for a job. Keeps string return for backward-compat with views.py.

    Kwargs:
        job_id (int): required
        mode (bool): required
        user_id (int), groups (Iterable[str]) — optional
    """
    job_id = kwargs.get("job_id")
    mode = kwargs.get("mode")

    if job_id is None:
        return "job_id is not defined"
    if mode is None:
        return "mode is not defined"

    data = {"job_id": int(job_id), "mode": bool(mode)}

    try:
        status, output = _http_post(request, "/job/set_debug_mode", data)
    except Exception as ex:
        return f"ERROR to set debug mode: {ex}"

    message = output.get("message") or output.get("detail") or str(output)
    return f"Status: {status}, message: {message}"


def get_worker_stats(auth: Optional[Dict[str, str]] = None, **params) -> Tuple[int, Dict[str, Any]]:
    return _http_get(auth, "/harvester/get_worker_statistics", params)

def kill_jobs(
    auth: Optional[Dict[str, str]] = None,
    job_ids: Union[int, str, List[Union[int, str]], None] = None,
    code: Optional[int] = None,
    use_email_as_id: bool = False,
    kill_options: Optional[List[str]] = None,
) -> Tuple[int, Dict[str, Any]]:
    if job_ids is None:
        ids: List[int] = []
    elif isinstance(job_ids, (list, tuple, set)):
        ids = [int(x) for x in job_ids]
    else:
        ids = [int(job_ids)]

    payload: Dict[str, Any] = {
        "job_ids": ids,
        "use_email_as_id": bool(use_email_as_id),
        "kill_options": list(kill_options or []),
    }
    if code is not None:
        payload["code"] = int(code)

    return _http_post(auth, "/job/kill", payload)


def get_job_status(auth: Optional[Dict[str, str]] = None, pandaid: Union[int, str] = 0) -> Tuple[int, Dict[str, Any]]:
    params = {"panda_id": int(pandaid)}
    return _http_get(auth, "/job/get_description", params)


def get_script_offline_running(auth: Optional[Dict[str, str]] = None, pandaid: Union[int, str] = 0) -> Tuple[int, Dict[str, Any]]:
    params = {"panda_id": int(pandaid)}
    return _http_get(auth, "/job/get_offline_execution_script", params)


def get_user_groups(idtoken: str):
    """Extract the 'groups' claim from an IAM JWT without verifying the signature."""
    token = idtoken.split(" ", 1)[1] if " " in idtoken else idtoken
    decoded = jwt.decode(token, verify=False, options={"verify_signature": False})
    return decoded.get("groups", [])