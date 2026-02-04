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
  - Создайте бота через [@BotFather](https://t.me/BotFather) в Telegram
  - Используйте команду `/newbot` и следуйте инструкциям
  - Скопируйте полученный токен в `.env`
- `OPENAI_API_KEY` - API ключ OpenAI
  - Получите ключ на [platform.openai.com](https://platform.openai.com/api-keys)
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

### Технологический стек

Проект полностью асинхронный:
- **aiogram 3.x** - асинхронный фреймворк для Telegram ботов
- **asyncpg** - асинхронный драйвер для PostgreSQL
- **AsyncOpenAI** - асинхронный клиент для OpenAI API
- **asyncio** - для управления асинхронными операциями

Все операции (запросы к БД, запросы к LLM, обработка сообщений) выполняются асинхронно, что обеспечивает высокую производительность.

### Преобразование запросов в SQL

Бот использует OpenAI GPT-4o-mini для преобразования вопросов на русском языке в SQL запросы.

**Подход:**
1. Пользователь отправляет вопрос на русском языке
2. Бот асинхронно отправляет запрос в LLM с описанием схемы БД
3. LLM возвращает SQL запрос
4. Бот асинхронно выполняет SQL запрос к PostgreSQL через asyncpg
5. Бот возвращает пользователю одно число (результат запроса)

**Внутренняя логика:**
- Каждый запрос обрабатывается независимо
- Контекст диалога не хранится
- Один запрос → один числовой ответ

**Описание схемы данных:**
LLM получает подробное описание структуры таблиц `videos` и `video_snapshots`, включая:
- Названия таблиц и колонок
- Типы данных
- Связи между таблицами (FOREIGN KEY)
- Особенности работы с датами и приращениями (delta_* поля)
- Правила преобразования русских дат в SQL формат

**Промпт для LLM:**
Промпт содержит:
- Полное описание схемы БД (SCHEMA_DESCRIPTION)
- Инструкции по обработке русских дат
- Правила формирования SQL запросов
- Требование возвращать только одно число через SELECT
- Примеры преобразования дат ("28 ноября 2025" → '2025-11-28')

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

## Запуск через Docker

Проект включает готовые Docker файлы для удобного развертывания.

1. Создайте файл `.env` в корне проекта:
```bash
TELEGRAM_BOT_TOKEN=your_bot_token_here
OPENAI_API_KEY=your_openai_api_key_here
```

2. Поместите файл `videos.json` в корневую директорию проекта

3. Запустите через Docker Compose:
```bash
docker-compose up -d
```

4. Примените миграции (в отдельном терминале):
```bash
docker-compose exec postgres psql -U postgres -d video_analytics -f /docker-entrypoint-initdb.d/001_create_tables.sql
```

Или примените миграции вручную:
```bash
docker-compose exec postgres psql -U postgres -d video_analytics
# Затем выполните SQL из migrations/001_create_tables.sql
```

5. Загрузите данные:
```bash
docker-compose exec bot python load_data.py /app/videos.json
```

Бот будет автоматически запущен и готов к работе. Логи можно посмотреть командой:
```bash
docker-compose logs -f bot
```
