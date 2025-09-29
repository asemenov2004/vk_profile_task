FROM python:3.9-slim

RUN apt-get update && apt-get install -y \
    cron \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
COPY sql/ ./sql/

# Делаем скрипты исполняемыми
RUN chmod +x ./scripts/extract.py ./scripts/transform.py ./scripts/setup_cron.sh

# Создаем лог-файл
RUN mkdir /var/log/cron
RUN touch /var/log/cron/cron.log && chmod 666 /var/log/cron/cron.log

# Запускаем setup скрипт и cron
CMD bash -c "/app/scripts/setup_cron.sh && echo 'Cron настроен. Запуск...' && cron -f"