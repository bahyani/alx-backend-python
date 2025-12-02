import django_filters
from .models import Message
from django.contrib.auth import get_user_model

User = get_user_model()

class MessageFilter(django_filters.FilterSet):
    # Filter by sender
    user = django_filters.ModelChoiceFilter(queryset=User.objects.all())
    # Filter by date range
    start_date = django_filters.DateFilter(field_name="created_at", lookup_expr='gte')
    end_date = django_filters.DateFilter(field_name="created_at", lookup_expr='lte')

    class Meta:
        model = Message
        fields = ['user', 'content', 'timestamp']
