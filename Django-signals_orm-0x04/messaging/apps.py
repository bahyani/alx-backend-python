from django.apps import AppConfig


class MessagingConfig(AppConfig):
    """
    Configuration class for the messaging application.
    This class handles app initialization and signal registration.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'messaging'
    verbose_name = 'Messaging System'
    
    def ready(self):
        """
        Override the ready method to import signals when Django starts.
        This ensures that signal handlers are registered and active.
        """
        # Import signals module to register signal handlers
        import messaging.signals  # noqa: F401
