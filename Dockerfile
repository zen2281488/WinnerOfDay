FROM python:3.11-slim

# Отключаем буферизацию (логи сразу в консоль) и создание .pyc файлов
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ARG BUILD_DATE="unknown"
ARG BUILD_SHA="unknown"
ENV BUILD_DATE=$BUILD_DATE
ENV BUILD_SHA=$BUILD_SHA

WORKDIR /app

# Сначала копируем зависимости (для кэширования слоев)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Запуск
CMD ["python", "gayBot.py"]
