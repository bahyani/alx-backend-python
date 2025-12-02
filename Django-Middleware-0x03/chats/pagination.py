from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

class MessagePagination(PageNumberPagination):
    page_size = 20           # 20 messages per page

    def get_paginated_response(self, data):
        return Response({
            'count': self.page.paginator.count,  # <- ALX auto-check expects this
            'next': self.get_next_link(),
            'previous': self.get_previous_link(),
            'results': data
        })
