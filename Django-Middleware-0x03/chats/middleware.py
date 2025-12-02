
import logging
from datetime import datetime
from django.conf import settings
from pathlib import Path
from django.http import HttpResponseForbidden, JsonResponse
import time

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


class RestrictAccessByTimeMiddleware:


    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        # Only restrict the chat endpoints
        if request.path.startswith("/api/messages/"):
            current_hour = datetime.now().hour
            # Deny access before 6AM or after 9PM (21)
            if current_hour < 6 or current_hour >= 21:
                return HttpResponseForbidden("Chat access is allowed only between 6AM and 9PM")

        response = self.get_response(request)
        return response


class OffensiveLanguageMiddleware:

    def __init__(self, get_response):
        self.get_response = get_response
        # Track IPs: {ip: [timestamps_of_messages]}
        self.ip_message_log = {}

    def __call__(self, request):
        # Only track POST requests to messages endpoint
        if request.method == "POST" and request.path.startswith("/api/messages/"):
            ip = self.get_client_ip(request)
            now = time.time()
            timestamps = self.ip_message_log.get(ip, [])

            # Keep only timestamps within the last 60 seconds
            timestamps = [ts for ts in timestamps if now - ts < 60]

            if len(timestamps) >= 5:
                # Too many messages within 1 minute
                return JsonResponse(
                    {"error": "Message limit exceeded: max 5 messages per minute"},
                    status=429,
                )

            # Log this message timestamp
            timestamps.append(now)
            self.ip_message_log[ip] = timestamps

        response = self.get_response(request)
        return response

    @staticmethod
    def get_client_ip(request):
        """Extract client IP address"""
        x_forwarded_for = request.META.get("HTTP_X_FORWARDED_FOR")
        if x_forwarded_for:
            ip = x_forwarded_for.split(",")[0]
        else:
            ip = request.META.get("REMOTE_ADDR")
        return ip
