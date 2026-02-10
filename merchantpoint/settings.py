BOT_NAME = 'merchantpoint'

SPIDER_MODULES = ['merchantpoint.spiders']
NEWSPIDER_MODULE = 'merchantpoint.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure a delay for requests for the same website
DOWNLOAD_DELAY = 2
RANDOMIZE_DOWNLOAD_DELAY = True

# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 1

# Disable cookies
COOKIES_ENABLED = False

# Configure pipelines
ITEM_PIPELINES = {
    'merchantpoint.pipelines.CleanDataPipeline': 300,
}

# User agent
USER_AGENT = 'merchantpoint (+http://www.yourdomain.com)'

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 1

# AutoThrottle для автоматической регулировки скорости
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 10
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = True

# Retry settings
RETRY_TIMES = 3
RETRY_HTTP_CODES = [500, 502, 503, 504, 408, 429]

# Настройки для CSV экспорта
FEED_EXPORT_ENCODING = 'utf-8'