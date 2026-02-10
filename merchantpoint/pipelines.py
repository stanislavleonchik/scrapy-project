# merchantpoint/pipelines.py
import re


class CleanDataPipeline:
    """Pipeline для очистки и валидации данных"""

    def process_item(self, item, spider):
        # Очистка названия торговой точки
        if item.get('merchant_name'):
            item['merchant_name'] = self.clean_text(item['merchant_name'])

        # Валидация MCC кода
        if item.get('mcc'):
            mcc = str(item['mcc']).strip()
            if re.match(r'^\d{4}$', mcc):
                item['mcc'] = mcc
            else:
                spider.logger.warning(f"Invalid MCC code: {mcc}")
                item['mcc'] = None

        # Очистка адреса
        if item.get('address'):
            item['address'] = self.clean_text(item['address'])

        # Очистка и форматирование координат
        if item.get('geo_coordinates'):
            coords = item['geo_coordinates']
            # Убираем лишние символы и пробелы
            coords = re.sub(r'[^\d.,\-\s]', '', coords)
            coords = coords.strip()
            item['geo_coordinates'] = coords

        # Очистка названия организации
        if item.get('org_name'):
            item['org_name'] = self.clean_text(item['org_name'])

        # Очистка описания организации
        if item.get('org_description'):
            # Убираем множественные пробелы и переносы строк
            desc = ' '.join(item['org_description'].split())
            # Ограничиваем длину описания
            if len(desc) > 500:
                desc = desc[:497] + '...'
            item['org_description'] = desc

        return item

    def clean_text(self, text):
        """Очистка текста от лишних символов"""
        if not text:
            return ''
        # Убираем множественные пробелы
        text = ' '.join(text.split())
        # Убираем специальные символы в начале и конце
        text = text.strip(' \t\n\r\f\v—-–')
        return text