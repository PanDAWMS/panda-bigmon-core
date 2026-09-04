import logging
import os
import requests

_logger = logging.getLogger('panda_client')


class AskPanda:
    """Base class for communicating with AskPanda and processing the output for representation in web pages"""
    def __init__(self):
        self.base_url = os.environ.get("BASE_URL_ASK_PANDA")

    def _prepare_headers(self):
        """Prepare request headers"""
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _prepare_request_params_str(self, get_params=None) -> str:
        """Prepare request params string"""
        if get_params is None:
            get_params = {}
        return "&".join(f"{k}={v}" for k, v in get_params.items())

    def post(self, req_params=None, data=None):
        """HTTP client for ask panda"""
        if not self.base_url:
            _logger.error("[AskPanda] Base URL for AskPanda not set")
            return {"success": False, "error": "AskPanda service is misconfigured (missing Base URL)."}
        if data is None:
            data = {}
        if req_params:
            params_str = self._prepare_request_params_str(req_params)
        else:
            params_str = ''
        full_url = f"{self.base_url}?{params_str}"

        headers = self._prepare_headers()
        try:
            response = requests.post(full_url, headers=headers, timeout=600, json=data)
            response.raise_for_status()
            return {"success": True, "data": response.json()}
        except requests.RequestException as e:
            _logger.error("[AskPanda] request failed: %s", str(e))
            return {"success": False, "error": f"Failed to communicate with AskPanda: {str(e)}"}

    def job_error_analysis(self, job:dict):
        """Send request to AskPanda"""
        out = {"success": False, "error": "", "data": {}}
        data = {
            'prompt': "Analyze job error",
            'job_id': job['pandaid'],
            'username': job['produsername'],
        }
        res = self.post(data=data)

        # processing of response
        if res['success']:
            out["success"] = True
            out["data"] = res['data']

        return out
