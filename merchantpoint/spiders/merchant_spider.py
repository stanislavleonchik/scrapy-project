# merchantpoint/spiders/merchant_spider.py
import scrapy
from scrapy import Request
from merchantpoint.items import MerchantItem
import time
import re


class MerchantSpider(scrapy.Spider):
    name = 'merchant'
    allowed_domains = ['merchantpoint.ru']
    start_urls = ['https://merchantpoint.ru/brands']

    # Настройки для соблюдения robots.txt
    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 2,  # Задержка между запросами в секундах
        'CONCURRENT_REQUESTS': 1,  # Количество одновременных запросов
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'USER_AGENT': 'MerchantSpider (+http://example.com/bot)',
        'FEEDS': {
            'merchants_data.csv': {
                'format': 'csv',
                'encoding': 'utf8',
                'store_empty': False,
                'fields': ['merchant_name', 'mcc', 'address', 'geo_coordinates',
                           'org_name', 'org_description', 'source_url'],
            },
        },
    }

    def parse(self, response):
        """Парсинг страницы со списком брендов"""
        # XPath для ссылок на бренды
        brand_links = response.xpath('//table[@class="finance-table"]//tbody/tr/td[2]/a/@href').getall()

        for link in brand_links:
            full_url = response.urljoin(link)
            yield Request(
                url=full_url,
                callback=self.parse_brand,
                meta={'brand_url': full_url}
            )

        # Пагинация - переход на следующую страницу
        next_page = response.xpath('//a[contains(text(), "Далее")]/@href').get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse
            )

    def parse_brand(self, response):
        """Парсинг страницы бренда"""
        # Извлекаем информацию о бренде
        org_name = response.xpath('//h1[@class="text-3xl md:text-4xl font-bold mb-3"]/text()').get()
        if org_name:
            org_name = org_name.strip()

        # Описание организации из блока описания
        org_description = response.xpath('//div[@class="description_brand"]//text()').getall()
        org_description = ' '.join([text.strip() for text in org_description if text.strip()])

        # Ищем все ссылки на торговые точки в таблице
        merchant_rows = response.xpath('//section[@id="sms"]//table[@class="finance-table"]//tbody/tr')

        for row in merchant_rows:
            # Извлекаем ссылку на торговую точку
            merchant_link = row.xpath('.//td[2]/a/@href').get()
            if merchant_link:
                merchant_url = response.urljoin(merchant_link)
                yield Request(
                    url=merchant_url,
                    callback=self.parse_merchant,
                    meta={
                        'org_name': org_name,
                        'org_description': org_description,
                        'brand_url': response.meta.get('brand_url')
                    }
                )

        def parse_merchant(self, response):
            """Парсинг страницы торговой точки"""
            item = MerchantItem()

            # XPath выражения для извлечения данных

            # Название точки
            merchant_name_xpath = '//h1[@class="text-3xl md:text-4xl font-bold mb-3"]/text()'
            item['merchant_name'] = response.xpath(merchant_name_xpath).get()
            if item['merchant_name']:
                item['merchant_name'] = item['merchant_name'].strip()

            # MCC код
            mcc_xpath = '//p[contains(text(), "MCC код")]/a/text()'
            item['mcc'] = response.xpath(mcc_xpath).get()
            if not item['mcc']:
                # Альтернативный XPath
                mcc_xpath_alt = '//p[b[contains(text(), "MCC код")]]/following-sibling::text() | //p[b[contains(text(), "MCC код")]]/a/text()'
                mcc_text = response.xpath(mcc_xpath_alt).get()
                if mcc_text:
                    # Извлекаем только цифры MCC кода
                    mcc_match = re.search(r'\d{4}', mcc_text)
                    if mcc_match:
                        item['mcc'] = mcc_match.group()

            # Адрес
            address_xpath = '//p[b[contains(text(), "Адрес торговой точки")]]/text()[last()]'
            address = response.xpath(address_xpath).get()
            if address:
                item['address'] = address.strip().replace('—', '').strip()

            # Геокоординаты
            geo_xpath = '//p[b[contains(text(), "Геокоординаты")]]/text()[last()]'
            geo_coords = response.xpath(geo_xpath).get()
            if geo_coords:
                item['geo_coordinates'] = geo_coords.strip().replace(':', '').strip()

            # Данные организации из meta
            item['org_name'] = response.meta.get('org_name', '')
            item['org_description'] = response.meta.get('org_description', '')

            # URL источника
            item['source_url'] = response.url

            yield item