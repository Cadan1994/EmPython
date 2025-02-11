from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('admin/', admin.site.urls),
    path('appetd/', include('appetd.urls')),
    path('appbi/', include('appbi.urls')),
    path('appierp/', include('appierp.urls')),
]
