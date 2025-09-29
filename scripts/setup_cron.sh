# Создаем crontab файл с переменными окружения
cat > /etc/cron.d/etl-cron << EOF
DB_HOST=${DB_HOST}
DB_PORT=${DB_PORT}
POSTGRES_DB=${POSTGRES_DB}
POSTGRES_USER=${POSTGRES_USER}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
API_URL=${API_URL}

${EXTRACT_SCHEDULE} cd /app && /usr/local/bin/python ./scripts/extract.py >> /var/log/cron/cron.log 2>&1
${TRANSFORM_SCHEDULE} cd /app && /usr/local/bin/python ./scripts/transform.py >> /var/log/cron/cron.log 2>&1
EOF

# Применяем crontab
chmod 0644 /etc/cron.d/etl-cron
crontab /etc/cron.d/etl-cron

echo "Cron настроен с переменными:"
echo "DB_HOST=$DB_HOST"
echo "API_URL=$API_URL"
echo "Extract schedule: $EXTRACT_SCHEDULE"
echo "Transform schedule: $TRANSFORM_SCHEDULE"