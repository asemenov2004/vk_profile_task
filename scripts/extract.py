import os
import requests
import logging
import time
from datetime import datetime
from db_connector import DBConnector

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger('extract')


class APIDataLoader:
    def __init__(self):
        self.api_url = os.getenv('API_URL')
        self.db = DBConnector('extract')
        self.max_retries = 3
        self.retry_delay = 5

    def fetch_data_from_api(self):
        """Получение данных из API с повторными попытками"""
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Получение данных из API (попытка {attempt + 1})")
                response = requests.get(self.api_url, timeout=30)
                response.raise_for_status()

                data = response.json()
                logger.info(f"Успешно получено {len(data)} записей из API")
                return data

            except requests.exceptions.Timeout:
                logger.warning(
                    f"Таймаут запроса к API (попытка {attempt + 1})")
            except requests.exceptions.ConnectionError:
                logger.warning(
                    f"Ошибка соединения с API (попытка {attempt + 1})")
            except requests.exceptions.HTTPError as e:
                logger.warning(f"HTTP ошибка API: {e} (попытка {attempt + 1})")
            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Ошибка запроса к API: {e} (попытка {attempt + 1})")

            if attempt < self.max_retries - 1:
                logger.info(
                    f"Повторная попытка через {self.retry_delay} секунд...")
                time.sleep(self.retry_delay)
            else:
                logger.error(
                    "Все попытки получения данных из API завершились ошибкой")
                raise Exception(
                    "Не удалось получить данные из API после нескольких попыток")

    def save_to_database(self, data):
        def insert_records(cursor):
            inserted_count = 0
            for item in data:
                cursor.execute("""
                    INSERT INTO raw_users_by_posts (user_id, post_id, title, body)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, post_id) DO NOTHING
                """, (item['userId'], item['id'], item['title'], item['body']))

                if cursor.rowcount > 0:
                    inserted_count += 1
            return inserted_count

        inserted_count = self.db.execute_in_transaction(insert_records)
        logger.info(
            f"Успешно вставлено {inserted_count} новых записей из {len(data)} полученных")

    def run(self):
        """Основной метод извлечения данных"""
        try:
            logger.info("Запуск процесса извлечения данных")
            start_time = datetime.now()

            data = self.fetch_data_from_api()
            self.save_to_database(data)

            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"Извлечение данных успешно завершено за {execution_time:.2f} секунд")
            return True

        except Exception as e:
            logger.error(f"Процесс извлечения данных завершился ошибкой: {e}")
            return False


if __name__ == "__main__":
    extractor = APIDataLoader()
    success = extractor.run()
    exit(0 if success else 1)
