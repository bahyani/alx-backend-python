from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.contrib.auth.models import User
from .models import Message, Notification


@receiver(post_save, sender=Message)
def create_message_notification(sender, instance, created, **kwargs):
    """
    Signal handler that creates a notification when a new message is created.
    
    Args:
        sender: The Message model class
        instance: The actual Message instance being saved
        created: Boolean indicating if this is a new record
        **kwargs: Additional keyword arguments
    """
    # Only create notification for new messages, not updates
    if created:
        # Create notification for the receiver
        Notification.objects.create(
            user=instance.receiver,
            message=instance,
            notification_type='message',
            title=f"New message from {instance.sender.username}",
            content=f"{instance.sender.username} sent you a message: {instance.content[:50]}{'...' if len(instance.content) > 50 else ''}"
        )


@receiver(post_save, sender=Message)
def mark_notification_read_on_message_read(sender, instance, created, **kwargs):
    """
    Signal handler that marks related notifications as read when a message is read.
    
    Args:
        sender: The Message model class
        instance: The actual Message instance being saved
        created: Boolean indicating if this is a new record
        **kwargs: Additional keyword arguments
    """
    # Only process if message was marked as read (not on creation)
    if not created and instance.is_read:
        # Mark all related notifications as read
        Notification.objects.filter(
            message=instance,
            is_read=False
        ).update(is_read=True)


@receiver(post_delete, sender=Message)
def delete_related_notifications(sender, instance, **kwargs):
    """
    Signal handler that deletes related notifications when a message is deleted.
    This maintains data integrity and prevents orphaned notifications.
    
    Args:
        sender: The Message model class
        instance: The Message instance being deleted
        **kwargs: Additional keyword arguments
    """
    # Delete all notifications related to this message
    Notification.objects.filter(message=instance).delete()


# Optional: Create notification when user joins
@receiver(post_save, sender=User)
def create_welcome_notification(sender, instance, created, **kwargs):
    """
    Signal handler that creates a welcome notification for new users.
    
    Args:
        sender: The User model class
        instance: The actual User instance being saved
        created: Boolean indicating if this is a new record
        **kwargs: Additional keyword arguments
    """
    if created:
        Notification.objects.create(
            user=instance,
            notification_type='system',
            title="Welcome to our messaging platform!",
            content=f"Hello {instance.username}, welcome to our platform! Start sending messages to connect with others."
        )
