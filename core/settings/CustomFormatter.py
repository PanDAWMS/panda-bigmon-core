import logging

from django.http import HttpRequest
class CustomFormatter(logging.Formatter):
    def format(self, record):
        url = "N/A"
        method = "-"
        remote = "-"

        req = getattr(record, "request", None)
        if isinstance(req, HttpRequest):
            try:
                url = req.build_absolute_uri(req.get_full_path())
                method = req.method
                remote = req.META.get("REMOTE_ADDR", "-")
            except Exception:
                pass

        record.full_url = url
        record.http_method = method
        record.remote_addr = remote

        return super().format(record)