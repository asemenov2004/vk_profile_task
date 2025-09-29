#!/usr/bin/env python3
import os
import psycopg2
import logging
import time

logger = logging.getLogger('db_connector')


class DBConnector:
    def __init__(self, logger_name=None):
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        self.max_retries = 3
        self.retry_delay = 5
        self.logger = logging.getLogger(logger_name) if logger_name else logger

    def get_connection(self):
        """Установка соединения с БД с повторными попытками"""
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Подключение к БД (попытка {attempt + 1})")
                conn = psycopg2.connect(**self.db_config)
                self.logger.info("Подключение к БД установлено")
                return conn
            except psycopg2.OperationalError as e:
                self.logger.warning(
                    f"Ошибка подключения к БД (попытка {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    self.logger.info(
                        f"Повторная попытка через {self.retry_delay} секунд...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error(
                        "Все попытки подключения к БД завершились ошибкой")
                    raise

    def execute_query(self, query, params=None, commit=True):
        """Выполнение SQL запроса с возвратом курсора"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(query, params)
            if commit:
                conn.commit()
            return cursor, conn
        except Exception as e:
            conn.rollback()
            cursor.close()
            conn.close()
            raise e

    def execute_and_close(self, query, params=None):
        """Выполнение SQL запроса с автоматическим закрытием соединения"""
        cursor, conn = self.execute_query(query, params)
        cursor.close()
        conn.close()

    def fetch_all(self, query, params=None):
        """Выполнение SELECT запроса и возврат всех результатов"""
        cursor, conn = self.execute_query(query, params, commit=False)
        try:
            return cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

    def fetch_one(self, query, params=None):
        """Выполнение SELECT запроса и возврат одной строки"""
        cursor, conn = self.execute_query(query, params, commit=False)
        try:
            return cursor.fetchone()
        finally:
            cursor.close()
            conn.close()

    def execute_in_transaction(self, callback):
        """Выполнение операций в одной транзакции"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            result = callback(cursor)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()

    def execute_and_close(self, query, params=None):
        """Выполнение SQL запроса с автоматическим закрытием соединения"""
        cursor, conn = self.execute_query(query, params)
        cursor.close()
        conn.close()
