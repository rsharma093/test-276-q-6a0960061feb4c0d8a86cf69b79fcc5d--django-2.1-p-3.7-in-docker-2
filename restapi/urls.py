# -*- coding: utf-8 -*-
from __future__ import unicode_literals, absolute_import

from restapi.views import AnalyzeLogFilesViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register("process-logs", AnalyzeLogFilesViewSet, basename="process_logs")
urlpatterns = router.urls
