import os.path
from pathlib import Path

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

# Quick-start development settings - unsuitable for production
# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'django-insecure-u8^lw0#ji(-g98&$h@opw4q=%m9c6zvj803qu=jy#ry!i3&n&-'

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

# A list of strings representing the host/domain names that this Django site can serve
ALLOWED_HOSTS = ['127.0.0.1', '172.16.157.3']

# Application definition
INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_apscheduler',
    'fontawesomefree',
    'appbi.apps.AppbiConfig',
    'appetd.apps.AppetdConfig',
    'appierp.apps.AppierpConfig',
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'configurations.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'configurations.wsgi.application'

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql_psycopg2',
        'OPTIONS': {'options': '-c search_path=webservice,public'},
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': 'cfb5ce8c49',
        'HOST': '172.16.157.3',
        'PORT': '2899',
    }
}

# Password validation
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# Internationalization
LANGUAGE_CODE = 'pt-BR'
TIME_ZONE = 'America/Sao_Paulo'
USE_I18N = True
USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'templates\static')]

# Default primary key field type
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'