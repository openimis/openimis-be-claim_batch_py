from django.urls import path
from . import report_views

urlpatterns = [
    path('report/', report_views.report, name='report')
]
