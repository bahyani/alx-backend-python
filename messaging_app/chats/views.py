from django.shortcuts import render
from django_filters.rest_framework import DjangoFilterBackend

from rest_framework import generics, viewsets
from rest_framework.exceptions import PermissionDenied
from rest_framework.permissions import IsAuthenticated
from .models import Message, Conversation
from .permissions import IsOwner, IsParticipantOfConversation
from .serializers import ConversationSerializer, MessageSerializer
from .pagination import MessagePagination
from .filters import MessageFilter

# Create your views here.

class UserMessageListCreate(generics.ListCreateAPIView):
    serializer_class = MessageSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        return Message.objects.filter(user=self.request.user)

    def perform_create(self, serializer):
        serializer.save(user=self.request.user)


class UserMessageDetail(generics.RetrieveDestroyAPIView):
    queryset = Message.objects.all()
    serializer_class = MessageSerializer
    permission_classes = [IsAuthenticated, IsOwner]


class ConversationViewSet(viewsets.ModelViewSet):
    queryset = Conversation.objects.all()
    serializer_class = ConversationSerializer
    permission_classes = [IsParticipantOfConversation]

    def get_queryset(self):
        # Only return conversations where request.user is a participant
        return Conversation.objects.filter(user1=self.request.user) | Conversation.objects.filter(user2=self.request.user)



class MessageViewSet(viewsets.ModelViewSet):
        serializer_class = MessageSerializer
        permission_classes = [IsAuthenticated, IsParticipantOfConversation]
        pagination_class = MessagePagination
        filter_backends = [DjangoFilterBackend]
        filterset_class = MessageFilter

    def get_queryset(self):
        # Filter messages by conversation_id query param
        conversation_id = self.request.query_params.get("conversation_id")
        if conversation_id:
            qs = Message.objects.filter(conversation_id=conversation_id)
        else:
            qs = Message.objects.all()
        # Only include messages from conversations where user is a participant
        return qs.filter(conversation__user1=self.request.user) | qs.filter(conversation__user2=self.request.user)

    def perform_create(self, serializer):
        conversation = serializer.validated_data["conversation"]
        if self.request.user not in [conversation.user1, conversation.user2]:
            # Non-participants get HTTP_403_FORBIDDEN
            raise PermissionDenied(detail="You are not a participant of this conversation.")
        serializer.save(user=self.request.user)