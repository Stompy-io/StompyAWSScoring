from django.contrib import admin
from django.urls import path
from . import views as _v

urlpatterns = [
    path('get_spot_score/', _v.ScoringView.as_view(), name='get_spot_score'),
    path('get_instance_type/', _v.InstanceView.as_view(), name='get_spot_score'),
]
