# from django.db import models
# from django.contrib.auth.models import User
# from django.utils import timezone
# import uuid


# class Message(models.Model):
#     """
#     Model representing a message between users.
#     """
#     id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
#     sender = models.ForeignKey(
#         User,
#         on_delete=models.CASCADE,
#         related_name='sent_messages',
#         help_text="User who sent the message"
#     )
#     receiver = models.ForeignKey(
#         User,
#         on_delete=models.CASCADE,
#         related_name='received_messages',
#         help_text="User who receives the message"
#     )
#     content = models.TextField(help_text="Message content")
#     timestamp = models.DateTimeField(default=timezone.now, help_text="When the message was sent")
#     is_read = models.BooleanField(default=False, help_text="Whether the message has been read")
    
#     class Meta:
#         ordering = ['-timestamp']
#         indexes = [
#             models.Index(fields=['sender', 'timestamp']),
#             models.Index(fields=['receiver', 'timestamp']),
#             models.Index(fields=['receiver', 'is_read']),
#         ]
    
#     def __str__(self):
#         return f"Message from {self.sender.username} to {self.receiver.username} at {self.timestamp}"
    
#     def mark_as_read(self):
#         """Mark the message as read"""
#         if not self.is_read:
#             self.is_read = True
#             self.save(update_fields=['is_read'])


# class Notification(models.Model):
#     """
#     Model representing notifications for users.
#     """
#     NOTIFICATION_TYPES = (
#         ('message', 'New Message'),
#         ('system', 'System Notification'),
#         ('alert', 'Alert'),
#     )
    
#     id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
#     user = models.ForeignKey(
#         User,
#         on_delete=models.CASCADE,
#         related_name='notifications',
#         help_text="User who receives the notification"
#     )
#     message = models.ForeignKey(
#         Message,
#         on_delete=models.CASCADE,
#         related_name='notifications',
#         null=True,
#         blank=True,
#         help_text="Related message if applicable"
#     )
#     notification_type = models.CharField(
#         max_length=20,
#         choices=NOTIFICATION_TYPES,
#         default='message',
#         help_text="Type of notification"
#     )
#     title = models.CharField(max_length=200, help_text="Notification title")
#     content = models.TextField(help_text="Notification content")
#     is_read = models.BooleanField(default=False, help_text="Whether the notification has been read")
#     created_at = models.DateTimeField(auto_now_add=True, help_text="When the notification was created")
#     read_at = models.DateTimeField(null=True, blank=True, help_text="When the notification was read")
    
#     class Meta:
#         ordering = ['-created_at']
#         indexes = [
#             models.Index(fields=['user', 'is_read']),
#             models.Index(fields=['user', 'created_at']),
#             models.Index(fields=['notification_type']),
#         ]
    
#     def __str__(self):
#         return f"Notification for {self.user.username}: {self.title}"
    
#     def mark_as_read(self):
#         """Mark the notification as read"""
#         if not self.is_read:
#             self.is_read = True
#             self.read_at = timezone.now()
#             self.save(update_fields=['is_read', 'read_at'])
    
#     @classmethod
#     def unread_count(cls, user):
#         """Get count of unread notifications for a user"""
#         return cls.objects.filter(user=user, is_read=False).count()
    
#     @classmethod
#     def get_recent_notifications(cls, user, limit=10):
#         """Get recent notifications for a user"""
#         return cls.objects.filter(user=user).order_by('-created_at')[:limit]

from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
import uuid


class Message(models.Model):
    """
    Model representing a message between users with threading support.
    Supports threaded conversations through parent_message self-referential foreign key.
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
    parent_message = models.ForeignKey(
        'self',
        on_delete=models.CASCADE,
        related_name='replies',
        null=True,
        blank=True,
        help_text="Parent message if this is a reply in a threaded conversation"
    )
    content = models.TextField(help_text="Message content")
    timestamp = models.DateTimeField(default=timezone.now, help_text="When the message was sent")
    is_read = models.BooleanField(default=False, help_text="Whether the message has been read")
    edited = models.BooleanField(default=False, help_text="Whether the message has been edited")
    last_edited_at = models.DateTimeField(null=True, blank=True, help_text="When the message was last edited")
    
    class Meta:
        ordering = ['-timestamp']
        indexes = [
            models.Index(fields=['sender', 'timestamp']),
            models.Index(fields=['receiver', 'timestamp']),
            models.Index(fields=['receiver', 'is_read']),
            models.Index(fields=['edited']),
            models.Index(fields=['parent_message', 'timestamp']),
        ]
    
    def __str__(self):
        edited_marker = " (edited)" if self.edited else ""
        reply_marker = " (reply)" if self.parent_message else ""
        return f"Message from {self.sender.username} to {self.receiver.username} at {self.timestamp}{edited_marker}{reply_marker}"
    
    def mark_as_read(self):
        """Mark the message as read"""
        if not self.is_read:
            self.is_read = True
            self.save(update_fields=['is_read'])
    
    def mark_as_edited(self):
        """Mark the message as edited and update timestamp"""
        self.edited = True
        self.last_edited_at = timezone.now()
        self.save(update_fields=['edited', 'last_edited_at'])
    
    @property
    def edit_count(self):
        """Get the number of times this message has been edited"""
        return self.history.count()
    
    def get_edit_history(self):
        """Get all edit history for this message"""
        return self.history.all().order_by('-edited_at')
    
    @property
    def is_thread_root(self):
        """Check if this message is the root of a thread"""
        return self.parent_message is None
    
    @property
    def reply_count(self):
        """Get the total number of direct replies to this message"""
        return self.replies.count()
    
    @property
    def thread_depth(self):
        """Calculate the depth of this message in the thread (0 for root messages)"""
        depth = 0
        current = self
        while current.parent_message:
            depth += 1
            current = current.parent_message
        return depth
    
    def get_thread_root(self):
        """Get the root message of this thread"""
        current = self
        while current.parent_message:
            current = current.parent_message
        return current
    
    def get_all_replies(self):
        """
        Get all replies (direct and nested) to this message using efficient ORM queries.
        Uses select_related and prefetch_related for optimization.
        """
        return Message.objects.filter(
            parent_message=self
        ).select_related(
            'sender', 'receiver', 'parent_message'
        ).prefetch_related(
            'replies__sender',
            'replies__receiver'
        ).order_by('timestamp')
    
    def get_thread_messages(self):
        """
        Get all messages in this thread (root message and all nested replies).
        Optimized query using select_related and prefetch_related.
        """
        root = self.get_thread_root()
        
        # Get all messages that belong to this thread
        messages = Message.objects.filter(
            models.Q(id=root.id) | models.Q(parent_message__isnull=False)
        ).select_related(
            'sender', 'receiver', 'parent_message'
        ).prefetch_related(
            'replies'
        )
        
        # Filter to only include messages in this thread
        thread_messages = []
        checked_messages = {root.id}
        to_check = [root]
        
        while to_check:
            current = to_check.pop(0)
            thread_messages.append(current)
            for reply in current.replies.all():
                if reply.id not in checked_messages:
                    checked_messages.add(reply.id)
                    to_check.append(reply)
        
        return thread_messages
    
    def get_conversation_participants(self):
        """Get all unique users participating in this thread"""
        thread_messages = self.get_thread_messages()
        participants = set()
        for msg in thread_messages:
            participants.add(msg.sender)
            participants.add(msg.receiver)
        return list(participants)
    
    @classmethod
    def get_threaded_conversation(cls, message_id):
        """
        Class method to retrieve a complete threaded conversation efficiently.
        Uses advanced ORM techniques to minimize database queries.
        
        Args:
            message_id: The ID of any message in the thread
            
        Returns:
            Dictionary with root message and organized replies
        """
        try:
            message = cls.objects.select_related(
                'sender', 'receiver', 'parent_message'
            ).prefetch_related(
                'replies__sender',
                'replies__receiver',
                'replies__replies'
            ).get(id=message_id)
            
            root = message.get_thread_root()
            
            # Recursively build the thread structure
            def build_thread(msg):
                return {
                    'message': msg,
                    'replies': [build_thread(reply) for reply in msg.replies.all()]
                }
            
            return build_thread(root)
            
        except cls.DoesNotExist:
            return None
    
    @classmethod
    def get_user_conversations(cls, user, use_prefetch=True):
        """
        Get all conversation threads for a user with optimized queries.
        
        Args:
            user: The User object
            use_prefetch: Whether to use prefetch_related (default True)
            
        Returns:
            QuerySet of root messages (threads) involving the user
        """
        base_query = cls.objects.filter(
            models.Q(sender=user) | models.Q(receiver=user),
            parent_message__isnull=True  # Only root messages
        )
        
        if use_prefetch:
            return base_query.select_related(
                'sender', 'receiver'
            ).prefetch_related(
                'replies__sender',
                'replies__receiver',
                'replies__replies'
            ).order_by('-timestamp')
        
        return base_query.order_by('-timestamp')


class MessageHistory(models.Model):
    """
    Model to track the edit history of messages.
    Stores previous versions of message content when edited.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    message = models.ForeignKey(
        Message,
        on_delete=models.CASCADE,
        related_name='history',
        help_text="The message this history entry belongs to"
    )
    old_content = models.TextField(help_text="Previous content before edit")
    edited_at = models.DateTimeField(auto_now_add=True, help_text="When this edit was made")
    edited_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='message_edits',
        help_text="User who made the edit"
    )
    
    class Meta:
        ordering = ['-edited_at']
        verbose_name_plural = "Message histories"
        indexes = [
            models.Index(fields=['message', 'edited_at']),
            models.Index(fields=['edited_by']),
        ]
    
    def __str__(self):
        return f"Edit history for message {self.message.id} at {self.edited_at}"


class Notification(models.Model):
    """
    Model representing notifications for users.
    """
    NOTIFICATION_TYPES = (
        ('message', 'New Message'),
        ('system', 'System Notification'),
        ('alert', 'Alert'),
        ('message_edit', 'Message Edited'),
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