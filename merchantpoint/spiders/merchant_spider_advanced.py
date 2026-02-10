# merchantpoint_spider/spiders/merchant_spider_advanced.py
import scrapy
from scrapy import Request
import re
from merchantpoint.items import MerchantItem


class MerchantSpiderAdvanced(scrapy.Spider):
    name = 'merchant_advanced'
    allowed_domains = ['merchantpoint.ru']
    start_urls = ['https://merchantpoint.ru/brands']

    custom_settings = {
        'ROBOTSTXT_OBEY': True,
        'DOWNLOAD_DELAY': 2,
        'CONCURRENT_REQUESTS': 1,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'LOG_LEVEL': 'INFO',
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 3600,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.items_count = 0
        self.max_items = kwargs.get('max_items', 10000)

    def parse(self, response):
        """Парсинг страницы со списком брендов"""
        self.logger.info(f"Parsing brands page: {response.url}")

        # Ищем таблицу с брендами
        brand_rows = response.xpath('//table[@class="finance-table"]//tbody/tr')

        if not brand_rows:
            self.logger.warning("No brand rows found with finance-table class, trying alternative selector")
            brand_rows = response.xpath('//table//tbody/tr')

        self.logger.info(f"Found {len(brand_rows)} brand rows")

        for row in brand_rows:
            if self.items_count >= self.max_items:
                self.logger.info(f"Reached max items limit: {self.max_items}")
                return

            # Извлекаем ссылку на бренд
            brand_link = row.xpath('.//td[2]/a/@href').get()
            brand_name = row.xpath('.//td[2]/a/text()').get()

            if brand_link:
                full_url = response.urljoin(brand_link)
                self.logger.info(f"Following brand: {brand_name} - {full_url}")
                yield Request(
                    url=full_url,
                    callback=self.parse_brand,
                    meta={'brand_name': brand_name, 'brand_url': full_url},
                    dont_filter=True
                )

        # Пагинация
        next_page = response.xpath('//a[contains(text(), "Далее")]/@href').get()
        if not next_page:
            next_page = response.xpath('//a[contains(@class, "next")]/@href').get()

        if next_page and self.items_count < self.max_items:
            next_url = response.urljoin(next_page)
            self.logger.info(f"Following next page: {next_url}")
            yield Request(url=next_url, callback=self.parse)

    def parse_brand(self, response):
        """Парсинг страницы бренда"""
        self.logger.info(f"Parsing brand page: {response.url}")

        # Название организации
        org_name = response.xpath('//h1[@class="text-3xl font-bold mb-4"]/text()').get()
        if not org_name:
            org_name = response.xpath('//h1/text()').get()
        if not org_name:
            org_name = response.meta.get('brand_name', 'Unknown')

        # Описание организации
        org_description = response.xpath('//div[@class="prose max-w-none mb-8"]//text()').getall()
        if not org_description:
            org_description = response.xpath('//div[contains(@class, "description")]//text()').getall()
        org_description = ' '.join([text.strip() for text in org_description if text.strip()])

        # ДОБАВИТЬ: Поиск ссылок на merchant страницы
        merchant_links = response.xpath('//a[contains(@href, "/merchant/")]')

        if merchant_links:
            self.logger.info(f"Found {len(merchant_links)} merchant links for {org_name}")

            for link in merchant_links:
                if self.items_count >= self.max_items:
                    return

                # Получаем строку таблицы
                row = link.xpath('./ancestor::tr')

                # Извлекаем данные
                mcc = row.xpath('.//td[1]/text()').get()
                merchant_name = link.xpath('./text()').get()
                merchant_link = link.xpath('./@href').get()
                address = row.xpath('.//td[3]/text()').get()

                if merchant_link:
                    detail_url = response.urljoin(merchant_link)
                    yield Request(
                        url=detail_url,
                        callback=self.parse_merchant_detail,  # Исправлено имя метода
                        meta={
                            'mcc': mcc.strip() if mcc else '',
                            'merchant_name': merchant_name.strip() if merchant_name else '',
                            'address_from_table': address.strip() if address else '',
                            'org_name': org_name,
                            'org_description': org_description
                        },
                        dont_filter=True
                    )
        else:
        # Если не нашли merchant ссылки, пробуем старый способ с таблицей
            merchant_rows = response.xpath('//section[@id="sms"]//table//tbody/tr')
            if not merchant_rows:
                merchant_rows = response.xpath('//table[contains(@class, "table")]//tbody/tr')

            self.logger.info(f"Found {len(merchant_rows)} merchant rows for {org_name}")

            if len(merchant_rows) == 0:
                self.logger.warning(f"No merchants found on page: {response.url}")
                return

            for row in merchant_rows:
                if self.items_count >= self.max_items:
                    return

                # MCC код
                mcc = row.xpath('.//td[1]/text()').get()
                if not mcc:
                    continue

                # Название торговой точки
                merchant_name = row.xpath('.//td[2]/a/text()').get()
                if not merchant_name:
                    merchant_name = row.xpath('.//td[2]/text()').get()

                # Ссылка на детальную страницу
                detail_link = row.xpath('.//td[2]/a/@href').get()

                # Адрес из таблицы
                address = row.xpath('.//td[3]/text()').get()

                if detail_link:
                    # Переходим на детальную страницу
                    detail_url = response.urljoin(detail_link)
                    yield Request(
                        url=detail_url,
                        callback=self.parse_merchant_detail,  # Исправлено имя метода
                        meta={
                            'mcc': mcc.strip() if mcc else '',
                            'merchant_name': merchant_name.strip() if merchant_name else '',
                            'address_from_table': address.strip() if address else '',
                            'org_name': org_name,
                            'org_description': org_description
                        },
                        dont_filter=True
                    )
                else:
                    # Если нет детальной страницы, сохраняем что есть
                    item = MerchantItem()
                    item['mcc'] = mcc.strip() if mcc else ''
                    item['merchant_name'] = merchant_name.strip() if merchant_name else ''
                    item['address'] = address.strip() if address else ''
                    item['geo_coordinates'] = ''
                    item['org_name'] = org_name
                    item['org_description'] = org_description
                    item['source_url'] = response.url

                    self.items_count += 1
                    yield item

    def parse_merchant_detail(self, response):
        """Парсинг детальной страницы торговой точки"""
        self.logger.info(f"Parsing merchant detail: {response.url}")

        item = MerchantItem()

        # Данные из meta
        item['mcc'] = response.meta.get('mcc', '')
        item['merchant_name'] = response.meta.get('merchant_name', '')

        # Адрес - несколько вариантов поиска
        address_xpaths = [
            '//p[b[contains(text(), "Адрес")]]/text()[last()]',
            '//p[contains(text(), "Адрес")]/following-sibling::text()[1]',
            '//div[contains(@class, "address")]//text()',
            '//td[contains(text(), "Адрес")]/following-sibling::td/text()'
        ]

        address = None
        for xpath in address_xpaths:
            address = response.xpath(xpath).get()
            if address:
                break

        if not address:
            address = response.meta.get('address_from_table', '')
        item['address'] = address.strip() if address else ''

        # Геокоординаты - ищем на странице
        geo_patterns = [
            r'coordinates:\s*\[([0-9.-]+),\s*([0-9.-]+)\]',
            r'lat[itude]*"?\s*:\s*([0-9.-]+).*?lng|lon[gitude]*"?\s*:\s*([0-9.-]+)',
            r'data-lat="([0-9.-]+)".*?data-lng="([0-9.-]+)"',
            r'ymaps\.Placemark\(\[([0-9.-]+),\s*([0-9.-]+)\]'
        ]

        geo_coordinates = ''
        page_content = response.text

        for pattern in geo_patterns:
            match = re.search(pattern, page_content, re.IGNORECASE | re.DOTALL)
            if match:
                lat, lng = match.groups()
                geo_coordinates = f"{lat},{lng}"
                break

        item['geo_coordinates'] = geo_coordinates

        # Данные организации
        item['org_name'] = response.meta.get('org_name', '')
        item['org_description'] = response.meta.get('org_description', '')
        item['source_url'] = response.url

        self.items_count += 1
        yield item
