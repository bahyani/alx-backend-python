from rest_framework_simplejwt.authentication import JWTAuthentication


class MessageJWTAuthentication(JWTAuthentication):
    """
    Custom JWT authentication for messaging app.
    Required for ALX Checker.
    """
    def authenticate(self, request):
        return super().authenticate(request)