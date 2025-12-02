from rest_framework.permissions import BasePermission
from rest_framework import permissions

class IsOwner(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        return obj.user == request.user


class IsParticipantOfConversation(permissions.BasePermission):
    """
    Custom permission: Only participants can send, view, update, delete messages.
    """

    def has_permission(self, request, view):
        # Only authenticated users can access any API endpoint
        return request.user and request.user.is_authenticated

    def has_object_permission(self, request, view, obj):
        # Explicitly check participant for all common HTTP methods
        if request.method in ["GET", "POST", "PUT", "PATCH", "DELETE"]:
            # If the object has 'participants' (group chat)
            if hasattr(obj, "participants"):
                return request.user in obj.participants.all()

            # If the object has 'user1' and 'user2' (1:1 conversation)
            if hasattr(obj, "user1") and hasattr(obj, "user2"):
                return request.user == obj.user1 or request.user == obj.user2

            # If the object has a 'user' field (message sender)
            if hasattr(obj, "user"):
                return request.user == obj.user

            return False
        return False
