from django.contrib import admin
from django.utils.html import format_html
from .models import Message, Notification


@admin.register(Message)
class MessageAdmin(admin.ModelAdmin):
    """
    Admin interface configuration for the Message model.
    Provides comprehensive management of messages in the Django admin.
    """
    list_display = ['id', 'sender_link', 'receiver_link', 'content_preview', 'timestamp', 'read_status']
    list_filter = ['is_read', 'timestamp', 'sender', 'receiver']
    search_fields = ['sender__username', 'receiver__username', 'content']
    readonly_fields = ['id', 'timestamp']
    date_hierarchy = 'timestamp'
    ordering = ['-timestamp']
    
    fieldsets = (
        ('Message Information', {
            'fields': ('id', 'sender', 'receiver', 'content')
        }),
        ('Status', {
            'fields': ('is_read', 'timestamp')
        })
    )
    
    def sender_link(self, obj):
        """Display sender as a clickable link to user's change page"""
        from django.urls import reverse
        from django.utils.safestring import mark_safe
        url = reverse('admin:auth_user_change', args=[obj.sender.id])
        return mark_safe(f'<a href="{url}">{obj.sender.username}</a>')
    sender_link.short_description = 'Sender'
    
    def receiver_link(self, obj):
        """Display receiver as a clickable link to user's change page"""
        from django.urls import reverse
        from django.utils.safestring import mark_safe
        url = reverse('admin:auth_user_change', args=[obj.receiver.id])
        return mark_safe(f'<a href="{url}">{obj.receiver.username}</a>')
    receiver_link.short_description = 'Receiver'
    
    def content_preview(self, obj):
        """Display a preview of the message content"""
        max_length = 50
        if len(obj.content) > max_length:
            return f"{obj.content[:max_length]}..."
        return obj.content
    content_preview.short_description = 'Content'
    
    def read_status(self, obj):
        """Display read status with colored indicator"""
        if obj.is_read:
            return format_html(
                '<span style="color: green; font-weight: bold;">✓ Read</span>'
            )
        return format_html(
            '<span style="color: orange; font-weight: bold;">⚠ Unread</span>'
        )
    read_status.short_description = 'Status'
    
    actions = ['mark_as_read', 'mark_as_unread']
    
    def mark_as_read(self, request, queryset):
        """Admin action to mark selected messages as read"""
        updated = queryset.update(is_read=True)
        self.message_user(request, f'{updated} message(s) marked as read.')
    mark_as_read.short_description = 'Mark selected messages as read'
    
    def mark_as_unread(self, request, queryset):
        """Admin action to mark selected messages as unread"""
        updated = queryset.update(is_read=False)
        self.message_user(request, f'{updated} message(s) marked as unread.')
    mark_as_unread.short_description = 'Mark selected messages as unread'


@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    """
    Admin interface configuration for the Notification model.
    Provides comprehensive management of notifications in the Django admin.
    """
    list_display = ['id', 'user_link', 'notification_type', 'title_preview', 'read_status', 'created_at']
    list_filter = ['notification_type', 'is_read', 'created_at']
    search_fields = ['user__username', 'title', 'content']
    readonly_fields = ['id', 'created_at', 'read_at']
    date_hierarchy = 'created_at'
    ordering = ['-created_at']
    
    fieldsets = (
        ('Notification Information', {
            'fields': ('id', 'user', 'notification_type', 'title', 'content')
        }),
        ('Related Data', {
            'fields': ('message',),
            'classes': ('collapse',)
        }),
        ('Status', {
            'fields': ('is_read', 'created_at', 'read_at')
        })
    )
    
    def user_link(self, obj):
        """Display user as a clickable link to user's change page"""
        from django.urls import reverse
        from django.utils.safestring import mark_safe
        url = reverse('admin:auth_user_change', args=[obj.user.id])
        return mark_safe(f'<a href="{url}">{obj.user.username}</a>')
    user_link.short_description = 'User'
    
    def title_preview(self, obj):
        """Display a preview of the notification title"""
        max_length = 40
        if len(obj.title) > max_length:
            return f"{obj.title[:max_length]}..."
        return obj.title
    title_preview.short_description = 'Title'
    
    def read_status(self, obj):
        """Display read status with colored indicator"""
        if obj.is_read:
            return format_html(
                '<span style="color: green; font-weight: bold;">✓ Read</span>'
            )
        return format_html(
            '<span style="color: red; font-weight: bold;">✗ Unread</span>'
        )
    read_status.short_description = 'Status'
    
    actions = ['mark_as_read', 'mark_as_unread', 'delete_selected_notifications']
    
    def mark_as_read(self, request, queryset):
        """Admin action to mark selected notifications as read"""
        from django.utils import timezone
        updated = queryset.update(is_read=True, read_at=timezone.now())
        self.message_user(request, f'{updated} notification(s) marked as read.')
    mark_as_read.short_description = 'Mark selected notifications as read'
    
    def mark_as_unread(self, request, queryset):
        """Admin action to mark selected notifications as unread"""
        updated = queryset.update(is_read=False, read_at=None)
        self.message_user(request, f'{updated} notification(s) marked as unread.')
    mark_as_unread.short_description = 'Mark selected notifications as unread'
    
    def delete_selected_notifications(self, request, queryset):
        """Admin action to delete selected notifications"""
        count = queryset.count()
        queryset.delete()
        self.message_user(request, f'{count} notification(s) deleted successfully.')
    delete_selected_notifications.short_description = 'Delete selected notifications'
