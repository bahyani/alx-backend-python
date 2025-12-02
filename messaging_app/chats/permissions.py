from rest_framework.permissions import BasePermission
from rest_framework import permissions

class IsOwner(permissions.BasePermission):
    def has_object_permission(self, request, view, obj):
        return obj.user == request.user


class IsParticipantOfConversation(permissions.BasePermission):
    """
    Allow access only to authenticated users who are participants of a conversation.
    """

    def has_permission(self, request, view):
        # Ensure user is authenticated globally
        return request.user and request.user.is_authenticated

    def has_object_permission(self, request, view, obj):
        """
        obj can be a Conversation or a Message
        Only allow access if the user is part of the conversation
        """
        # If the object has 'participants' (many-to-many)
        if hasattr(obj, "participants"):
            return request.user in obj.participants.all()


        if hasattr(obj, "user1") and hasattr(obj, "user2"):
            return request.user == obj.user1 or request.user == obj.user2


        if hasattr(obj, "user"):
            return request.user == obj.user

        return False
