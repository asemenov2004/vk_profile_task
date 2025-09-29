set -e  # Остановка при ошибках

echo "=== ETL процесс ==="
echo

# Сборка образа
echo "1. Строим Docker образ..."
docker-compose build

echo
echo "2. Запускаем сервисы..."
docker-compose up -d

echo
echo "3. Ждем запуск сервиса БД..."
sleep 10

echo
echo "4. Начальный ETL"
# Запускаем начальный ETL
docker-compose exec etl python ./scripts/extract.py
docker-compose exec etl python ./scripts/transform.py

# Пример выборки из итоговой таблицы
echo "5. Пример выборки из итоговой таблицы"
docker-compose exec db psql -U postgres -d postgres -c "
SELECT user_id, posts_cnt, calculated_at
FROM top_users_by_posts
ORDER BY posts_cnt DESC
LIMIT 10;"

echo
echo "To stop: docker-compose down"