import os
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

    headers = auth if auth and (auth.get("Authorization") or auth.get("Origin")) else None

    try:
        resp = post(url, headers=headers, data=data, timeout=30)
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
            (auth or {}).get("Origin") if headers else None,
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