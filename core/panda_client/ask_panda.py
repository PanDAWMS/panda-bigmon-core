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

    def post(self, data=None):
        """HTTP client for ask panda"""
        if not self.base_url:
            _logger.error("Base URL for AskPanda not set")
            return {"success": False, "error": "AskPanda service is misconfigured (missing Base URL)."}

        headers = self._prepare_headers()

        try:
            response = requests.post(self.base_url, headers=headers, json=data, timeout=600)
            response.raise_for_status()
            return {"success": True, "data": response.json()}
        except requests.RequestException as e:
            _logger.error("AskPanda request failed: %s", str(e))
            return {"success": False, "error": f"Failed to communicate with AskPanda: {str(e)}"}


    def job_error_analysis(self, pandaid):
        """Send request to AskPanda"""
        out = {"success": False, "error": "", "data": {}}
        data = {
            'prompt': "Analyze job error",
            'context': f"pandaid={pandaid}",
        }
        res = self.post(data)

        # processing of response
        if res['success']:
            out["success"] = True
            out["data"] = res['data']

        return out
