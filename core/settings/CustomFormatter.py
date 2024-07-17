import logging

class CustomFormatter(logging.Formatter):
    def format(self, record):
        if hasattr(record, 'request') and record.request:
            request = record.request
            full_url = request.build_absolute_uri(request.get_full_path())
        else:
            full_url = 'N/A'

        message = super().format(record)

        if record.exc_info:
            #error_traceback = self.formatException(record.exc_info)
            return f"{message}\n|Full URL: {full_url}"
        else:
            return f"{message}\n|Full URL: {full_url}"