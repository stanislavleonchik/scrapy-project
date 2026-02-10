# merchantpoint/spiders/merchant_spider_advanced.py
import scrapy
from scrapy import Request
from merchantpoint.items import MerchantItem
import re
from scrapy.exceptions import DropItem


class MerchantSpiderAdvanced(scrapy.Spider):
    name = 'merchant_advanced'
    allowed_domains = ['merchantpoint.ru']
    start_urls = ['https://merchantpoint.ru/brands']

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'USER_AGENT': 'MerchantSpider/1.0',
        'FEEDS': {
            'merchants_data.csv': {
                'format': 'csv',
                'encoding': 'utf8',
                'store_empty': False,
            },
        },
                'LOG_LEVEL': 'INFO',
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 3600,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.items_count = 0
        self.max_items = 1000  # Ограничение на количество записей

    def parse(self, response):
        """Парсинг страницы со списком брендов"""
        self.logger.info(f"Parsing brands page: {response.url}")

        # XPath для извлечения ссылок на бренды
        brand_rows = response.xpath('//table[@class="finance-table"]//tbody/tr')

        for row in brand_rows:
            if self.items_count >= self.max_items:
                self.logger.info(f"Reached maximum items limit: {self.max_items}")
                return

            # Извлекаем ссылку и название бренда
            brand_link = row.xpath('.//td[2]/a/@href').get()
            brand_name = row.xpath('.//td[2]/a/text()').get()

            if brand_link:
                full_url = response.urljoin(brand_link)
                yield Request(
                    url=full_url,
                    callback=self.parse_brand,
                    meta={
                        'brand_url': full_url,
                        'brand_name': brand_name
                    },
                    errback=self.handle_error
                )

        # Пагинация
        if self.items_count < self.max_items:
            next_page = response.xpath('//a[contains(text(), "Далее")]/@href').get()
            if next_page:
                yield Request(
                    url=response.urljoin(next_page),
                    callback=self.parse,
                    errback=self.handle_error
                )

    def parse_brand(self, response):
        """Парсинг страницы бренда"""
        self.logger.info(f"Parsing brand page: {response.url}")

        # Извлекаем информацию о бренде
        org_name = response.xpath('//h1[contains(@class, "text-3xl") or contains(@class, "text-4xl")]/text()').get()
        if not org_name:
            # Альтернативный XPath
            org_name = response.meta.get('brand_name', '')
        else:
            org_name = org_name.strip()

        # Описание организации - улучшенный XPath
        org_description_xpath = '''
            //div[contains(@class, "description_brand")]//text() |
            //section[@id="description"]//div[contains(@class, "prose")]//text()
        '''
        org_description = response.xpath(org_description_xpath).getall()
        org_description = ' '.join([text.strip() for text in org_description if text.strip()])

        # Очистка описания от HTML тегов и лишних символов
        org_description = re.sub(r'<[^>]+>', '', org_description)
        org_description = re.sub(r'\s+', ' ', org_description).strip()

        # Ищем таблицу с торговыми точками
        merchant_table_xpath = '//section[@id="sms"]//table[@class="finance-table"]//tbody/tr'
        merchant_rows = response.xpath(merchant_table_xpath)

        if not merchant_rows:
            self.logger.warning(f"No merchant rows found on {response.url}")
            # Альтернативный путь поиска
            merchant_rows = response.xpath('//table[contains(@class, "finance-table")]//tbody/tr')

        for row in merchant_rows:
            if self.items_count >= self.max_items:
                return

                # Извлекаем данные из строки таблицы
            mcc = row.xpath('.//td[1]/text()').get()
            merchant_link = row.xpath('.//td[2]/a/@href').get()
            merchant_name = row.xpath('.//td[2]/a/text()').get()
            address = row.xpath('.//td[3]/text()').get()

            if merchant_link:
                merchant_url = response.urljoin(merchant_link)
                yield Request(
                    url=merchant_url,
                    callback=self.parse_merchant,
                    meta={
                        'org_name': org_name,
                        'org_description': org_description,
                        'brand_url': response.meta.get('brand_url'),
                        'mcc_from_table': mcc,
                        'merchant_name_from_table': merchant_name,
                        'address_from_table': address
                    },
                    errback=self.handle_error
                )
            elif merchant_name and mcc:
                # Если нет ссылки на детальную страницу, сохраняем данные из таблицы
                item = MerchantItem()
                item['merchant_name'] = merchant_name.strip() if merchant_name else ''
                item['mcc'] = mcc.strip() if mcc else ''
                item['address'] = address.strip() if address else ''
                item['geo_coordinates'] = ''
                item['org_name'] = org_name
                item['org_description'] = org_description
                item['source_url'] = response.url

                self.items_count += 1
                yield item

        def parse_merchant(self, response):
            """Парсинг страницы торговой точки"""
            self.logger.info(f"Parsing merchant page: {response.url}")

            item = MerchantItem()

            # Название точки - комбинированный подход
            merchant_name = response.xpath(
                '//h1[contains(@class, "text-3xl") or contains(@class, "text-4xl")]/text()').get()
            if not merchant_name:
                merchant_name = response.meta.get('merchant_name_from_table', '')
            item['merchant_name'] = merchant_name.strip() if merchant_name else ''

            # MCC код - несколько вариантов XPath
            mcc_xpaths = [
                '//p[contains(., "MCC код")]/a/text()',
                '//p[b[contains(text(), "MCC код")]]/a/text()',
                '//p[contains(text(), "MCC")]/following-sibling::text()[1]',
                '//td[contains(text(), "MCC")]/following-sibling::td/text()'
            ]

            mcc = None
            for xpath in mcc_xpaths:
                mcc = response.xpath(xpath).get()
                if mcc:
                    break

            if not mcc:
                # Используем MCC из таблицы на предыдущей странице
                mcc = response.meta.get('mcc_from_table', '')

            # Извлекаем только цифры MCC
            if mcc:
                mcc_match = re.search(r'\d{4}', str(mcc))
                item['mcc'] = mcc_match.group() if mcc_match else mcc.strip()
            else:
                item['mcc'] = ''

            # Адрес - улучшенные XPath выражения
            address_xpaths = [
                '//p[b[contains(text(), "Адрес")]]/text()[last()]',
                '//p[contains(text(), "Адрес")]/following-sibling::text()[1]',
                '//div[contains(text(), "Адрес")]/following-sibling::div/text()',
                 '//td[contains(text(), "Адрес")]/following-sibling::td/text()'
            ]

            address = None
            for xpath in address_xpaths:
                address = response.xpath(xpath).get()
                if address:
                    break

            if not address:
                address = response.meta.get('address_from_table', '')

            if address:
                # Очистка адреса
                address = address.strip().replace('—', '').replace('–', '').strip()
            item['address'] = address or ''

            # Геокоординаты - несколько вариантов извлечения
            geo_xpaths = [
                '//p[b[contains(text(), "Геокоординаты")]]/text()[last()]',
                '//p[contains(text(), "Геокоординаты")]/following-sibling::text()[1]',
                '//script[contains(text(), "ymaps.Placemark")]/text()'
            ]

            geo_coords = None
            for xpath in geo_xpaths:
                geo_text = response.xpath(xpath).get()
                if geo_text:
                    # Ищем координаты в формате XX.XXXXXX, YY.YYYYYY
                    coord_pattern = r'(\d{1,3}\.\d+)[,\s]+(\d{1,3}\.\d+)'
                    match = re.search(coord_pattern, geo_text)
                    if match:
                        geo_coords = f"{match.group(1)}, {match.group(2)}"
                        break

            item['geo_coordinates'] = geo_coords or ''

            # Данные организации
            item['org_name'] = response.meta.get('org_name', '')
            item['org_description'] = response.meta.get('org_description', '')

            # URL источника
            item['source_url'] = response.url

            self.items_count += 1
            self.logger.info(f"Scraped item #{self.items_count}: {item['merchant_name']}")

            yield item

        def handle_error(self, failure):
            """Обработка ошибок при запросах"""
            self.logger.error(f"Request failed: {failure.request.url}")
            self.logger.error(f"Error: {failure.value}")