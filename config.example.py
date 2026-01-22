"""
Application configuration - Copy this to config.py and update values
"""
import os


class Config:
    """Base configuration"""

    # Database configuration
    DATABASE = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': int(os.environ.get('DB_PORT', 3306)),
        'user': os.environ.get('DB_USER', 'root'),
        'password': os.environ.get('DB_PASSWORD', 'your_password_here'),
        'charset': 'utf8mb4',
        'cursorclass': None  # Will use default
    }

    # Flask configuration
    SECRET_KEY = os.environ.get('SECRET_KEY', 'change-this-in-production')
    DEBUG = os.environ.get('FLASK_DEBUG', 'True').lower() == 'true'
