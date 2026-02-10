# merchantpoint_crawler


Проект Scrapy для сбора брендов и торговых точек с merchantpoint.ru


Запуск в виртуальном окружении


1. Установить зависимости


pip install -r requirements.txt


2. Запустить паука


scrapy crawl merchantpoint -o sample.jsonl


Проект уважает robots.txt и использует задержку between requests.