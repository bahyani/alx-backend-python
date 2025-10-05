from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
import uuid


class Message(models.Model):
    """
    Model representing a message between users.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    sender = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='sent_messages',
        help_text="User who sent the message"
    )
    receiver = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='received_messages',
        help_text="User who receives the message"
    )
    content = models.TextField(help_text="Message content")
    timestamp = models.DateTimeField(default=timezone.now, help_text="When the message was sent")
    is_read = models.BooleanField(default=False, help_text="Whether the message has been read")
    
    class Meta:
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['sender', 'timestamp']),
            models.Index(fields=['receiver', 'timestamp']),
            models.Index(fields=['receiver', 'is_read']),
        ]
    
    def __str__(self):
        return f"Message from {self.sender.username} to {self.receiver.username} at {self.timestamp}"
    
    def mark_as_read(self):
        """Mark the message as read"""
        if not self.is_read:
            self.is_read = True
            self.save(update_fields=['is_read'])


class Notification(models.Model):
    """
    Model representing notifications for users.
    """
    NOTIFICATION_TYPES = (
        ('message', 'New Message'),
        ('system', 'System Notification'),
        ('alert', 'Alert'),
    )
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name='notifications',
        help_text="User who receives the notification"
    )
    message = models.ForeignKey(
        Message,
        on_delete=models.CASCADE,
        related_name='notifications',
        null=True,
        blank=True,
        help_text="Related message if applicable"
    )
    notification_type = models.CharField(
        max_length=20,
        choices=NOTIFICATION_TYPES,
        default='message',
        help_text="Type of notification"
    )
    title = models.CharField(max_length=200, help_text="Notification title")
    content = models.TextField(help_text="Notification content")
    is_read = models.BooleanField(default=False, help_text="Whether the notification has been read")
    created_at = models.DateTimeField(auto_now_add=True, help_text="When the notification was created")
    read_at = models.DateTimeField(null=True, blank=True, help_text="When the notification was read")
    
    class Meta:
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', 'is_read']),
            models.Index(fields=['user', 'created_at']),
            models.Index(fields=['notification_type']),
        ]
    
    def __str__(self):
        return f"Notification for {self.user.username}: {self.title}"
    
    def mark_as_read(self):
        """Mark the notification as read"""
        if not self.is_read:
            self.is_read = True
            self.read_at = timezone.now()
            self.save(update_fields=['is_read', 'read_at'])
    
    @classmethod
    def unread_count(cls, user):
        """Get count of unread notifications for a user"""
        return cls.objects.filter(user=user, is_read=False).count()
    
    @classmethod
    def get_recent_notifications(cls, user, limit=10):
        """Get recent notifications for a user"""
        return cls.objects.filter(user=user).order_by('-created_at')[:limit]
