from django.urls import path
from flexDash import views

urlpatterns = [
    path('', views.home, name='home'),
    path('home/', views.home, name='home_alt'),
    path('list-tables/', views.list_tables, name='list_tables'),
    path('load-table-data/', views.load_table_data, name='load_table_data'),
    path('upload-sheet/', views.upload_sheet, name='upload_sheet'),
    path('get-chart-data/', views.get_chart_data, name='get_chart_data'),
    path('save-to-db/', views.save_to_database, name='save_to_database'),
    path('clear-cache/', views.clear_cache, name='clear_cache'),
    path('check-cache/', views.check_cache, name='check_cache'),
]
