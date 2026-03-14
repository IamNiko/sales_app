import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'sales_dashboard_secret_key_change_in_production')
    # Add other config variables here
