#!/usr/bin/env python3
import logging
from datetime import datetime
from db_connector import DBConnector

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
)
logger = logging.getLogger('transform')


class TopUsersTransformer:
    def __init__(self):
        self.db = DBConnector('transform')

    def refresh_top_users_mart(self):
        """Обновление витрины топ пользователей"""
        self.db.execute_and_close("""
            TRUNCATE TABLE top_users_by_posts;

            INSERT INTO top_users_by_posts (user_id, posts_cnt, calculated_at)
            SELECT
                user_id,
                COUNT(*) as posts_cnt,
                CURRENT_TIMESTAMP as calculated_at
            FROM raw_users_by_posts
            GROUP BY user_id
            ORDER BY posts_cnt DESC;
        """)
        logger.info("Витрина топ пользователей успешно обновлена")

    def get_top_users_stats(self):
        """Получение статистики по топ пользователям"""
        # ОДНО соединение для всех запросов
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            # Получение топ-10 пользователей
            cursor.execute("""
                SELECT user_id, posts_cnt, calculated_at
                FROM top_users_by_posts
                ORDER BY posts_cnt DESC
                LIMIT 10;
            """)
            top_users = cursor.fetchall()

            # Получение общей статистики
            cursor.execute("""
                SELECT
                    COUNT(*) as total_users,
                    SUM(posts_cnt) as total_posts,
                    MAX(posts_cnt) as max_posts
                FROM top_users_by_posts;
            """)
            stats = cursor.fetchone()

            return top_users, stats

        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def log_transformation_results(self):
        """Логирование результатов трансформации"""
        top_users, stats = self.get_top_users_stats()
        total_users, total_posts, max_posts = stats

        logger.info("Результаты трансформации:")
        logger.info(f"  - Обработано пользователей: {total_users}")
        logger.info(f"  - Всего постов: {total_posts}")
        logger.info(f"  - Максимум постов у пользователя: {max_posts}")

        logger.info("Топ-10 пользователей по количеству постов:")
        for rank, (user_id, posts_cnt, calculated_at) in enumerate(top_users, 1):
            logger.info(f"  {rank:2}. User {user_id}: {posts_cnt} постов")

    def run(self):
        """Основной метод выполнения трансформации"""
        try:
            logger.info("Запуск процесса трансформации данных")
            start_time = datetime.now()

            self.refresh_top_users_mart()
            self.log_transformation_results()

            execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"Трансформация данных завершена за {execution_time:.2f} секунд")
            return True

        except Exception as e:
            logger.error(
                f"Процесс трансформации данных завершился ошибкой: {e}")
            return False


if __name__ == "__main__":
    transformer = TopUsersTransformer()
    success = transformer.run()
    exit(0 if success else 1)
