import logging
class CustomFormatter(logging.Formatter):
    def format(self, record):
        if hasattr(record, 'request'):
            request = record.request
            full_url = request.build_absolute_uri(request.get_full_path())
            record.message = f"Internal Server Error: {full_url}\n{record.getMessage()}"
        else:
            record.message = f"Internal Server Error: N/A\n{record.getMessage()}"
        return super().format(record)