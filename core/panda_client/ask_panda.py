import logging
import markdown
import os
import requests

_logger = logging.getLogger('panda_client')


class AskPanda:
    """Base class for communicating with AskPanda REST API."""
    def __init__(self, username:str):
        self.username = username
        self.base_url = os.environ.get("BASE_URL_ASK_PANDA")
        self.token = os.environ.get("TOKEN_ASK_PANDA")
        if not self.base_url or not self.token:
            _logger.error("[AskPanda] Base URL not set")
            raise ValueError("[AskPanda] Init failed due to missing base URL or Token")
        self.base_url = self.base_url.rstrip('/')

    def _prepare_headers(self):
        """Prepare request headers"""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        if self.username:
            headers["X-Bamboo-User"] = self.username
        return headers


    def _request(self, method: str, endpoint: str, data=None) -> dict:
        """Internal HTTP runner supporting GET/POST and standardized response handling."""
        url = f"{self.base_url}{endpoint}"
        headers = self._prepare_headers()

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                timeout=600
            )
            response.raise_for_status()

            # Handle 204 No Content responses (e.g. rating submission)
            if response.status_code == 204:
                return {"success": True, "status_code": 204, "data": None}
            _logger.debug("[AskPanda] %s %s %s", method, endpoint, response.json())
            return {
                "success": True,
                "status_code": response.status_code,
                "data": response.json()
            }

        except requests.RequestException as e:
            _logger.error("[AskPanda] Request failed for %s %s: %s", method, endpoint, str(e))
            error_details = str(e)
            if e.response is not None:
                try:
                    error_details = e.response.json()
                except ValueError:
                    error_details = {'error': e.response.text or str(e)}

            return {
                "success": False,
                "status_code": getattr(e.response, 'status_code', None),
                "error": error_details['error'] if 'error' in error_details else "No error details",
            }

    def start_job_error_analysis(self, job_id: int, mode: str = "failure") -> dict:
        """
        Start job error analysis.
        Endpoint: POST /api/v1/analysis
        """
        payload = {
            "job_id": int(job_id),
            "mode": mode,
            "user": self.username
        }
        return self._request("POST", "/analysis", data=payload)

    def get_analysis_result(self, analysis_id: str) -> dict:
        """
        Poll or fetch analysis result.
        Endpoint: GET /api/v1/analysis/{id}
        """
        return self._request("GET", f"/analysis/{analysis_id}")


    def submit_rating(self, analysis_id: str, rating: int) -> dict:
        """
        Submit user rating (1..5) for an analysis.
        Endpoint: POST /api/v1/analysis/{id}/rating
        """
        if not (1 <= rating <= 5):
            return {"success": False, "error": "Rating must be an integer between 1 and 5"}

        payload = {"rating": rating}
        return self._request("POST", f"/analysis/{analysis_id}/rating", data=payload)