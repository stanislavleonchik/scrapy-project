import scrapy


class MerchantItem(scrapy.Item):
    merchant_name = scrapy.Field()
    mcc = scrapy.Field()
    address = scrapy.Field()
    geo_coordinates = scrapy.Field()
    org_name = scrapy.Field()
    org_description = scrapy.Field()
    source_url = scrapy.Field()