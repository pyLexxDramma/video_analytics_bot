# Telegram-бот для аналитики по видео

Telegram-бот для получения статистики по видео на основе запросов на естественном языке.

## Технологии

- Python 3.11+
- PostgreSQL
- aiogram 3.x (асинхронный Telegram бот)
- OpenAI API (GPT-4o-mini) для преобразования запросов в SQL
- asyncpg для работы с БД

## Установка

1. Клонируйте репозиторий:
```bash
git clone <repository_url>
cd video-analytics-bot
```

2. Установите зависимости:
```bash
pip install -r requirements.txt
```

3. Создайте файл `.env` на основе `.env.example`:
```bash
cp .env.example .env
```

4. Заполните переменные окружения в `.env`:
- `TELEGRAM_BOT_TOKEN` - токен вашего Telegram бота (получить у @BotFather)
- `OPENAI_API_KEY` - API ключ OpenAI
- `DATABASE_URL` - строка подключения к PostgreSQL (формат: `postgresql://user:password@host:port/database`)

## Настройка базы данных

1. Создайте базу данных PostgreSQL:
```sql
CREATE DATABASE video_analytics;
```

2. Примените миграции:
```bash
psql -d video_analytics -f migrations/001_create_tables.sql
```

Или через Python:
```python
import asyncio
from database import Database
from os import getenv
from dotenv import load_dotenv

load_dotenv()
db = Database(getenv('DATABASE_URL'))

async def migrate():
    await db.connect()
    await db.execute_migration('migrations/001_create_tables.sql')
    await db.close()

asyncio.run(migrate())
```

3. Загрузите данные из JSON файла:
```bash
python load_data.py path/to/videos.json
```

## Запуск бота

```bash
python bot.py
```

Бот будет отвечать на текстовые сообщения, преобразуя их в SQL запросы и возвращая числовые результаты.

## Архитектура

### Преобразование запросов в SQL

Бот использует OpenAI GPT-4o-mini для преобразования вопросов на русском языке в SQL запросы.

**Подход:**
1. Пользователь отправляет вопрос на русском языке
2. Бот отправляет запрос в LLM с описанием схемы БД и примером вопроса
3. LLM возвращает SQL запрос
4. Бот выполняет SQL запрос к PostgreSQL
5. Бот возвращает пользователю числовой результат

**Описание схемы данных:**
LLM получает подробное описание структуры таблиц `videos` и `video_snapshots`, включая:
- Названия таблиц и колонок
- Типы данных
- Связи между таблицами
- Особенности работы с датами и приращениями

**Промпт:**
Промпт содержит:
- Описание схемы БД
- Инструкции по обработке дат
- Правила формирования SQL запросов
- Требование возвращать только одно число

## Примеры запросов

- "Сколько всего видео есть в системе?"
- "Сколько видео у креатора с id ... вышло с 1 ноября 2025 по 5 ноября 2025 включительно?"
- "Сколько видео набрало больше 100 000 просмотров за всё время?"
- "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
- "Сколько разных видео получали новые просмотры 27 ноября 2025?"

## Структура проекта

```
video-analytics-bot/
├── bot.py                 # Основной файл бота
├── database.py            # Класс для работы с БД
├── llm_query.py           # Преобразование запросов в SQL через LLM
├── load_data.py           # Скрипт загрузки JSON в БД
├── requirements.txt       # Зависимости Python
├── .env.example          # Пример файла с переменными окружения
├── migrations/
│   └── 001_create_tables.sql  # SQL миграции
└── README.md             # Документация
```

## Docker (опционально)

Для запуска через Docker создайте `docker-compose.yml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: video_analytics
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  bot:
    build: .
    depends_on:
      - postgres
    environment:
      DATABASE_URL: postgresql://postgres:postgres@postgres:5432/video_analytics
      TELEGRAM_BOT_TOKEN: ${TELEGRAM_BOT_TOKEN}
      OPENAI_API_KEY: ${OPENAI_API_KEY}
    volumes:
      - ./videos.json:/app/videos.json

volumes:
  postgres_data:
```

И `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "bot.py"]
```
