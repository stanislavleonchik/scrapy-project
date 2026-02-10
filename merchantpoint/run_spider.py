# run_spider.py
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import sys
import os

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_spider(max_items=10000):
    # Импортируем паука
    from merchantpoint.spiders.merchant_spider_advanced import MerchantSpiderAdvanced

    # Настройки
    settings = get_project_settings()
    settings.set('FEED_FORMAT', 'csv')
    settings.set('FEED_URI', 'merchants_data.csv')
    settings.set('FEED_EXPORT_ENCODING', 'utf-8')

    # Создаем процесс
    process = CrawlerProcess(settings)

    # Запускаем паука с параметрами
    process.crawl(MerchantSpiderAdvanced, max_items=max_items)
    process.start()


if __name__ == '__main__':
    run_spider(max_items=100)