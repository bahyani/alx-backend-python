from django.test import TestCase
from django.contrib.auth.models import User
from django.utils import timezone
from .models import Message, Notification


class MessageModelTest(TestCase):
    """Test cases for the Message model"""
    
    def setUp(self):
        """Set up test users"""
        self.sender = User.objects.create_user(
            username='sender',
            email='sender@test.com',
            password='testpass123'
        )
        self.receiver = User.objects.create_user(
            username='receiver',
            email='receiver@test.com',
            password='testpass123'
        )
    
    def test_message_creation(self):
        """Test that a message can be created successfully"""
        message = Message.objects.create(
            sender=self.sender,
            receiver=self.receiver,
            content="Test message content"
        )
        self.assertIsNotNone(message.id)
        self.assertEqual(message.sender, self.sender)
        self.assertEqual(message.receiver, self.receiver)
        self.assertEqual(message.content, "Test message content")
        self.assertFalse(message.is_read)
    
    def test_message_str_representation(self):
        """Test the string representation of a message"""
        message = Message.objects.create(
            sender=self.sender,
            receiver=self.receiver,
            content="Test message"
        )
        expected_str = f"Message from {self.sender.username} to {self.receiver.username} at {message.timestamp}"
        self.assertEqual(str(message), expected_str)
    
    def test_mark_as_read(self):
        """Test marking a message as read"""
        message = Message.objects.create(
            sender=self.sender,
            receiver=self.receiver,
            content="Test message"
        )
        self.assertFalse(message.is_read)
        
        message.mark_as_read()
        self.assertTrue(message.is_read)
        
        # Test that calling mark_as_read again doesn't cause issues
        message.mark_as_read()
        self.assertTrue(message.is_read)


class NotificationModelTest(TestCase):
    """Test cases for the Notification model"""
    
    def setUp(self):
        """Set up test data"""
        self.user = User.objects.create_user(
            username='testuser',
            email='test@test.com',
            password='testpass123'
        )
        self.sender = User.objects.create_user(
            username='sender',
            email='sender@test.com',
            password='testpass123'
        )
        self.message = Message.objects.create(
            sender=self.sender,
            receiver=self.user,
            content="Test message"
        )
    
    def test_notification_creation(self):
        """Test that a notification can be created successfully"""
        notification = Notification.objects.create(
            user=self.user,
            message=self.message,
            notification_type='message',
            title='New message',
            content='You have a new message'
        )
        self.assertIsNotNone(notification.id)
        self.assertEqual(notification.user, self.user)
        self.assertEqual(notification.message, self.message)
        self.assertFalse(notification.is_read)
    
    def test_notification_str_representation(self):
        """Test the string representation of a notification"""
        notification = Notification.objects.create(
            user=self.user,
            notification_type='message',
            title='Test notification',
            content='Test content'
        )
        expected_str = f"Notification for {self.user.username}: Test notification"
        self.assertEqual(str(notification), expected_str)
    
    def test_mark_as_read(self):
        """Test marking a notification as read"""
        notification = Notification.objects.create(
            user=self.user,
            notification_type='message',
            title='Test notification',
            content='Test content'
        )
        self.assertFalse(notification.is_read)
        self.assertIsNone(notification.read_at)
        
        notification.mark_as_read()
        self.assertTrue(notification.is_read)
        self.assertIsNotNone(notification.read_at)
    
    def test_unread_count(self):
        """Test counting unread notifications for a user"""
        # Create multiple notifications
        Notification.objects.create(
            user=self.user,
            notification_type='message',
            title='Notification 1',
            content='Content 1'
        )
        Notification.objects.create(
            user=self.user,
            notification_type='message',
            title='Notification 2',
            content='Content 2'
        )
        notification3 = Notification.objects.create(
            user=self.user,
            notification_type='message',
            title='Notification 3',
            content='Content 3'
        )
        
        # All should be unread
        self.assertEqual(Notification.unread_count(self.user), 3)
        
        # Mark one as read
        notification3.mark_as_read()
        self.assertEqual(Notification.unread_count(self.user), 2)
    
    def test_get_recent_notifications(self):
        """Test retrieving recent notifications"""
        # Create multiple notifications
        for i in range(15):
            Notification.objects.create(
                user=self.user,
                notification_type='message',
                title=f'Notification {i}',
                content=f'Content {i}'
            )
        
        # Get recent notifications with default limit
        recent = Notification.get_recent_notifications(self.user)
        self.assertEqual(len(recent), 10)
        
        # Get recent notifications with custom limit
        recent_5 = Notification.get_recent_notifications(self.user, limit=5)
        self.assertEqual(len(recent_5), 5)


class MessageSignalTest(TestCase):
    """Test cases for message-related signals"""
    
    def setUp(self):
        """Set up test users"""
        self.sender = User.objects.create_user(
            username='sender',
            email='sender@test.com',
            password='testpass123'
        )
        self.receiver = User.objects.create_user(
            username='receiver',
            email='receiver@test.com',
            password='testpass123'
        )
    
    def test_notification_created_on_message_save(self):
        """Test that a notification is automatically created when a message is sent"""
        # Count notifications before
        initial_count = Notification.objects.filter(user=self.receiver).count()
        
        # Create a message
        message = Message.objects.create(
            sender=self.sender,
            receiver=self.receiver,
            content="Test message for signal"
        )
        
        # Check that a notification was created
        final_count = Notification.objects.filter(user=self.receiver).count()
        self.assertEqual(final_count, initial_count + 1)
        
        # Verify notification details
        notification = Notification.objects.filter(
            user=self.receiver,
            message=message
        ).first()
        self.assertIsNotNone(notification)
        self.assertEqual(notification.notification_type, 'message')
        self.assertIn(self.sender.username, notification.title)
        self.assertFalse(notification.is_read)
    
    def test_notification_not_created_on_message_update(self):
        """Test that updating a message doesn't create a new notification"""
        # Create a message
        message = Message.objects.create(
            sender=self.sender,
            receiver=self.receiver,
            content="Original content"
        )
        
        # Count notifications
        initial_count = Notification.objects.filter(user=self.receiver).count()
        
        # Update the message
        message.content = "Updated content"
        message.save()
        
        # Check that no new notification was created
        final_count = Notification.objects.filter(user=self.receiver).count()
        self.assertEqual(final_count, initial_count)
    
    def test_notification_deleted_when_message_deleted(self):
        """Test that notifications are deleted when the related message is deleted"""
        # Create a message (this triggers notification creation)
        message = Message.objects.create(
            sender=self.sender,
            receiver=self.receiver,
            content="Test message"
        )
        
        # Verify notification exists
        self.assertTrue(
            Notification.objects.filter(message=message).exists()
        )
        
        # Delete the message
        message.delete()
        
        # Verify notification is also deleted
        self.assertFalse(
            Notification.objects.filter(message=message).exists()
        )
    
    def test_welcome_notification_created_for_new_user(self):
        """Test that a welcome notification is created for new users"""
        # Create a new user
        new_user = User.objects.create_user(
            username='newuser',
            email='newuser@test.com',
            password='testpass123'
        )
        
        # Check that a welcome notification was created
        welcome_notification = Notification.objects.filter(
            user=new_user,
            notification_type='system'
        ).first()
        
        self.assertIsNotNone(welcome_notification)
        self.assertIn('Welcome', welcome_notification.title)
        self.assertIn(new_user.username, welcome_notification.content)


class NotificationSignalTest(TestCase):
    """Test cases for notification-related signals"""
    
    def setUp(self):
        """Set up test data"""
        self.sender = User.objects.create_user(
            username='sender',
            email='sender@test.com',
            password='testpass123'
        )
        self.receiver = User.objects.create_user(
            username='receiver',
            email='receiver@test.com',
            password='testpass123'
        )
    
    def test_notification_marked_read_when_message_read(self):
        """Test that notifications are marked as read when message is read"""
        # Create a message (triggers notification)
        message = Message.objects.create(
            sender=self.sender,
            receiver=self.receiver,
            content="Test message"
        )
        
        # Get the notification
        notification = Notification.objects.get(message=message)
        self.assertFalse(notification.is_read)
        
        # Mark message as read
        message.is_read = True
        message.save()
        
        # Refresh notification from database
        notification.refresh_from_db()
        self.assertTrue(notification.is_read)
