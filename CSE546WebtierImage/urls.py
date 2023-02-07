from django.urls import path

from . import imagepushcontroller

urlpatterns = [
    path('push', imagepushcontroller.pushcontroller)
]