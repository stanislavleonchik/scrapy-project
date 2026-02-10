from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import os


def run_spider():
    """Функция для запуска паука"""
    # Устанавливаем настройки проекта
    os.environ.setdefault('SCRAPY_SETTINGS_MODULE', 'merchantpoint.settings')

    # Создаем процесс краулера
    process = CrawlerProcess(get_project_settings())

    # Запускаем паука
    process.crawl('merchant_advanced')
    process.start()


if __name__ == '__main__':
    run_spider()