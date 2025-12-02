
import logging
from datetime import datetime
from django.conf import settings
from pathlib import Path

# Ensure logs go to project root (BASE_DIR)
LOG_PATH = Path(settings.BASE_DIR) / "requests.log"

# Configure logging once (module import)
logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(message)s"
)

class RequestLoggingMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        logging.basicConfig(
            filename='requests.log',
            level=logging.INFO,
            format='%(message)s'
        )

    def __call__(self, request):
        user = request.user if request.user.is_authenticated else "Anonymous"
        log_message = f"{datetime.now()} - User: {user} - Path: {request.path}"

        logging.info(log_message)

        response = self.get_response(request)
        return response
