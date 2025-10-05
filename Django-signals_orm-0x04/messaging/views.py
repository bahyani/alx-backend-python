
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import logout
from django.contrib import messages
from django.views.decorators.http import require_http_methods
from django.db import transaction
from django.http import JsonResponse
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from .models import Message, Notification


@login_required
@require_http_methods(["GET", "POST"])
def delete_user(request):
    """
    View to handle user account deletion.
    
    GET: Display confirmation page
    POST: Process the deletion
    
    This view allows authenticated users to delete their own accounts.
    All related data (messages, notifications) will be automatically cleaned up
    via signals and CASCADE constraints.
    """
    if request.method == 'GET':
        # Display confirmation page with statistics
        context = {
            'user': request.user,
            'sent_messages_count': Message.objects.filter(sender=request.user).count(),
            'received_messages_count': Message.objects.filter(receiver=request.user).count(),
            'notifications_count': Notification.objects.filter(user=request.user).count(),
        }
        return render(request, 'messaging/delete_user_confirm.html', context)
    
    elif request.method == 'POST':
        # Verify password or confirmation
        confirmation = request.POST.get('confirmation', '')
        
        if confirmation.lower() != 'delete':
            messages.error(request, 'You must type "DELETE" to confirm account deletion.')
            return redirect('messaging:delete_user')
        
        try:
            # Store username before deletion
            username = request.user.username
            user = request.user
            
            # Use transaction to ensure all deletions happen atomically
            with transaction.atomic():
                # Logout the user first
                logout(request)
                
                # Delete the user (signals will handle related data cleanup)
                user.delete()
            
            # Success message
            messages.success(
                request, 
                f'Account for {username} has been successfully deleted. All your data has been removed.'
            )
            return redirect('home')  # Redirect to home or login page
            
        except Exception as e:
            messages.error(request, f'An error occurred while deleting your account: {str(e)}')
            return redirect('messaging:delete_user')


@api_view(['DELETE'])
@permission_classes([IsAuthenticated])
def delete_user_api(request):
    """
    API endpoint to delete authenticated user's account.
    
    DELETE: Deletes the user account and all related data
    
    Request body:
    {
        "confirmation": "DELETE"
    }
    
    Returns:
        200: Account successfully deleted
        400: Invalid confirmation
        500: Server error
    """
    try:
        confirmation = request.data.get('confirmation', '')
        
        if confirmation != 'DELETE':
            return Response(
                {'error': 'You must provide "DELETE" as confirmation to delete your account.'},
                status=status.HTTP_400_BAD_REQUEST
            )
        
        # Get user statistics before deletion
        user_data = {
            'username': request.user.username,
            'email': request.user.email,
            'sent_messages': Message.objects.filter(sender=request.user).count(),
            'received_messages': Message.objects.filter(receiver=request.user).count(),
            'notifications': Notification.objects.filter(user=request.user).count(),
        }
        
        # Store user object
        user = request.user
        
        # Delete user within transaction
        with transaction.atomic():
            user.delete()
        
        return Response(
            {
                'message': f'Account for {user_data["username"]} has been successfully deleted.',
                'deleted_data': user_data
            },
            status=status.HTTP_200_OK
        )
        
    except Exception as e:
        return Response(
            {'error': f'An error occurred while deleting your account: {str(e)}'},
            status=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@login_required
def user_dashboard(request):
    """
    User dashboard showing messages and notifications.
    """
    sent_messages = Message.objects.filter(sender=request.user).order_by('-timestamp')[:10]
    received_messages = Message.objects.filter(receiver=request.user).order_by('-timestamp')[:10]
    notifications = Notification.objects.filter(user=request.user).order_by('-created_at')[:10]
    unread_notifications = Notification.objects.filter(user=request.user, is_read=False).count()
    
    context = {
        'sent_messages': sent_messages,
        'received_messages': received_messages,
        'notifications': notifications,
        'unread_notifications': unread_notifications,
        'total_sent': Message.objects.filter(sender=request.user).count(),
        'total_received': Message.objects.filter(receiver=request.user).count(),
    }
    
    return render(request, 'messaging/dashboard.html', context)


@login_required
@require_http_methods(["POST"])
def mark_notification_read(request, notification_id):
    """
    Mark a specific notification as read.
    """
    try:
        notification = Notification.objects.get(id=notification_id, user=request.user)
        notification.mark_as_read()
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({'success': True, 'message': 'Notification marked as read'})
        
        messages.success(request, 'Notification marked as read.')
        return redirect('messaging:dashboard')
        
    except Notification.DoesNotExist:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({'success': False, 'error': 'Notification not found'}, status=404)
        
        messages.error(request, 'Notification not found.')
        return redirect('messaging:dashboard')


@login_required
@require_http_methods(["POST"])
def mark_all_notifications_read(request):
    """
    Mark all user's notifications as read.
    """
    try:
        from django.utils import timezone
        
        updated_count = Notification.objects.filter(
            user=request.user,
            is_read=False
        ).update(is_read=True, read_at=timezone.now())
        
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'success': True,
                'message': f'{updated_count} notifications marked as read',
                'count': updated_count
            })
        
        messages.success(request, f'{updated_count} notifications marked as read.')
        return redirect('messaging:dashboard')
        
    except Exception as e:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({'success': False, 'error': str(e)}, status=500)
        
        messages.error(request, f'An error occurred: {str(e)}')
        return redirect('messaging:dashboard')


@login_required
@require_http_methods(["POST"])
def send_message(request):
    """
    Send a message to another user.
    """
    try:
        from django.contrib.auth.models import User
        
        receiver_username = request.POST.get('receiver')
        content = request.POST.get('content')
        
        if not receiver_username or not content:
            messages.error(request, 'Receiver and content are required.')
            return redirect('messaging:dashboard')
        
        try:
            receiver = User.objects.get(username=receiver_username)
        except User.DoesNotExist:
            messages.error(request, f'User {receiver_username} not found.')
            return redirect('messaging:dashboard')
        
        if receiver == request.user:
            messages.error(request, 'You cannot send a message to yourself.')
            return redirect('messaging:dashboard')
        
        # Create message (signal will automatically create notification)
        message = Message.objects.create(
            sender=request.user,
            receiver=receiver,
            content=content
        )
        
        messages.success(request, f'Message sent to {receiver.username}.')
        return redirect('messaging:dashboard')
        
    except Exception as e:
        messages.error(request, f'An error occurred: {str(e)}')
        return redirect('messaging:dashboard')


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def get_user_stats(request):
    """
    API endpoint to get user statistics.
    
    Returns:
        User's message and notification counts
    """
    stats = {
        'username': request.user.username,
        'email': request.user.email,
        'sent_messages': Message.objects.filter(sender=request.user).count(),
        'received_messages': Message.objects.filter(receiver=request.user).count(),
        'unread_messages': Message.objects.filter(receiver=request.user, is_read=False).count(),
        'total_notifications': Notification.objects.filter(user=request.user).count(),
        'unread_notifications': Notification.objects.filter(user=request.user, is_read=False).count(),
    }
    
    return Response(stats, status=status.HTTP_200_OK)