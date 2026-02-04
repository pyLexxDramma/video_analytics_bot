import httpx
from os import getenv

SCHEMA_DESCRIPTION = """
База данных PostgreSQL с двумя таблицами:

1. videos - итоговая статистика по видео:
   - id UUID PRIMARY KEY
   - creator_id VARCHAR (идентификатор креатора - ЕСТЬ ТОЛЬКО В ЭТОЙ ТАБЛИЦЕ!)
   - video_created_at TIMESTAMP WITH TIME ZONE (дата публикации видео)
   - views_count INTEGER (текущее количество просмотров)
   - likes_count INTEGER
   - comments_count INTEGER
   - reports_count INTEGER
   - created_at, updated_at TIMESTAMP

2. video_snapshots - почасовые замеры статистики:
   - id VARCHAR PRIMARY KEY
   - video_id UUID REFERENCES videos(id) (связь с таблицей videos)
   - views_count, likes_count, comments_count, reports_count INTEGER (значения на момент замера)
   - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count INTEGER (прирост за час)
   - created_at TIMESTAMP WITH TIME ZONE (дата и время замера)
   - updated_at TIMESTAMP
   - ВАЖНО: В этой таблице НЕТ поля creator_id! Для фильтрации по креатору используй JOIN с таблицей videos!

ВАЖНЫЕ ПРАВИЛА:
- COUNT(*) для подсчета количества видео
- SUM(views_count) для суммирования общего количества просмотров (суммарное количество просмотров)
- SUM(delta_views_count) для суммирования прироста просмотров за период
- COUNT(DISTINCT video_id) для подсчета уникальных видео
- Для фильтрации по конкретной ДАТЕ используй: DATE(created_at) = 'YYYY-MM-DD'
- Для фильтрации по диапазону ДАТ используй: DATE(created_at) BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
- Для фильтрации по дате публикации видео: DATE(video_created_at) = 'YYYY-MM-DD' или BETWEEN
- Преобразование русских дат: "28 ноября 2025" = '2025-11-28', "1 ноября 2025" = '2025-11-01', "5 ноября 2025" = '2025-11-05'
- Преобразование месяцев: "июнь 2025" = DATE(video_created_at) BETWEEN '2025-06-01' AND '2025-06-30', "ноябрь 2025" = DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-30'
- Если упоминается МЕСЯЦ (июнь, ноябрь и т.д.) → используй BETWEEN для диапазона с 1 по последний день месяца!
- Для фильтрации по дате публикации видео используй поле video_created_at (НЕ published_at!)
- ВСЕГДА используй DATE() при фильтрации по датам: DATE(video_created_at) BETWEEN, а НЕ video_created_at BETWEEN!

КРИТИЧЕСКИ ВАЖНО - ВЫБОР ТАБЛИЦЫ:
- Если вопрос про КРЕАТОРА (creator_id) или про ИТОГОВУЮ СТАТИСТИКУ → используй таблицу VIDEOS
- Если вопрос про ЗАМЕРЫ, СНАПШОТЫ, ПРИРОСТ за период → используй таблицу VIDEO_SNAPSHOTS
- Если вопрос про "по итоговой статистике", "всего", "набрали" → используй таблицу VIDEOS
- Если вопрос про "замеры статистики", "прирост", "выросли за дату" → используй таблицу VIDEO_SNAPSHOTS
- Для фильтрации по креатору используй: WHERE creator_id = 'id' (в таблице videos)
- НЕ путай creator_id (в таблице videos) с video_id (в таблице video_snapshots)!

ПРИМЕРЫ SQL ЗАПРОСОВ:

1. Подсчет общего количества видео:
   - "Сколько всего видео есть в системе?" → SELECT COUNT(*) FROM videos
   - "Сколько всего видео?" → SELECT COUNT(*) FROM videos

2. Подсчет видео по креатору и дате публикации:
   - "Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'
   - "Сколько видео у креатора с id X вышло с 1 по 5 ноября 2025?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'
   - "Сколько видео у креатора abc вышло с 1 по 5 ноября?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'abc' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'

2a. Подсчет видео по креатору и количеству просмотров (ИТОГОВАЯ СТАТИСТИКА):
   - "Сколько видео у креатора с id abc123 набрали больше 10000 просмотров по итоговой статистике?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND views_count > 10000
   - "Сколько видео у креатора с id X набрали больше 10 000 просмотров?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND views_count > 10000
   - "Сколько видео у креатора abc имеет более 5000 просмотров?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'abc' AND views_count > 5000

3. Подсчет видео по количеству просмотров:
   - "Сколько видео набрало больше 100000 просмотров за всё время?" → SELECT COUNT(*) FROM videos WHERE views_count > 100000
   - "Сколько видео набрало больше 100 000 просмотров за всё время?" → SELECT COUNT(*) FROM videos WHERE views_count > 100000
   - "Сколько видео имеет более 50000 просмотров?" → SELECT COUNT(*) FROM videos WHERE views_count > 50000

3a. Суммарное количество просмотров (сумма views_count из таблицы videos, НЕ количество видео!):
   - "Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?" → SELECT SUM(views_count) FROM videos WHERE DATE(video_created_at) BETWEEN '2025-06-01' AND '2025-06-30'
   - "Сколько всего просмотров набрали все видео?" → SELECT SUM(views_count) FROM videos
   - "Какое суммарное количество просмотров у всех видео?" → SELECT SUM(views_count) FROM videos
   - "Сколько просмотров суммарно набрали видео креатора X?" → SELECT SUM(views_count) FROM videos WHERE creator_id = 'X'
   - "Какое суммарное количество просмотров набрали все видео, опубликованные в июне?" → SELECT SUM(views_count) FROM videos WHERE DATE(video_created_at) >= '2025-06-01' AND DATE(video_created_at) <= '2025-06-30'
   
КРИТИЧЕСКИ ВАЖНО: 
- "суммарное количество просмотров" = SUM(views_count) из таблицы VIDEOS, а НЕ COUNT(*)!
- "набрали просмотры" или "опубликованные" = используй таблицу VIDEOS, а НЕ video_snapshots!
- Для фильтрации по дате публикации используй поле video_created_at из таблицы videos, а НЕ published_at!
- Преобразование месяцев: "июнь 2025" = с '2025-06-01' по '2025-06-30', "ноябрь 2025" = с '2025-11-01' по '2025-11-30'
- Суммируй views_count (INTEGER), а НЕ даты или другие поля!

4. Прирост просмотров за конкретную дату:
   - "На сколько просмотров в сумме выросли все видео 28 ноября 2025?" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28'
   - "На сколько просмотров в сумме выросли все видео 28 ноября 2025" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28'
   - "На сколько просмотров выросли все видео 27 ноября 2025?" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27'
   - "Сколько просмотров добавилось у всех видео 25 ноября?" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-25'

5. Уникальные видео с приростом за дату:
   - "Сколько разных видео получали новые просмотры 27 ноября 2025?" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0
   - "Сколько разных видео получали новые просмотры 27 ноября?" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0
   - "Сколько уникальных видео получили просмотры 28 ноября?" → SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28' AND delta_views_count > 0

6. Прирост других метрик за дату:
   - "На сколько лайков в сумме выросли все видео 28 ноября?" → SELECT SUM(delta_likes_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28'
   - "На сколько комментариев выросли все видео 27 ноября?" → SELECT SUM(delta_comments_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27'

7. Подсчет по диапазону дат:
   - "Сколько видео вышло с 1 по 5 ноября 2025?" → SELECT COUNT(*) FROM videos WHERE DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'
   - "На сколько просмотров выросли все видео с 1 по 5 ноября?" → SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) BETWEEN '2025-11-01' AND '2025-11-05'

7a. Прирост просмотров по креатору с фильтрацией по дате и времени:
   - "На сколько просмотров суммарно выросли все видео креатора с id abc123 в промежутке с 10:00 до 15:00 28 ноября 2025?" → SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'abc123' AND DATE(vs.created_at) = '2025-11-28' AND EXTRACT(HOUR FROM vs.created_at) >= 10 AND EXTRACT(HOUR FROM vs.created_at) < 15
   - "На сколько просмотров выросли видео креатора X с 10 до 15 часов 27 ноября?" → SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'X' AND DATE(vs.created_at) = '2025-11-27' AND EXTRACT(HOUR FROM vs.created_at) >= 10 AND EXTRACT(HOUR FROM vs.created_at) < 15

ВАЖНО для временных интервалов:
- Для фильтрации по ЧАСАМ используй EXTRACT(HOUR FROM created_at) >= час1 AND EXTRACT(HOUR FROM created_at) < час2
- НЕ используй BETWEEN с временем типа '10:00:00' - это вызовет ошибку!
- Для фильтрации по креатору в video_snapshots используй JOIN: JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'id'

8. Подсчет замеров с отрицательными значениями (без фильтрации по дате):
   - "Сколько всего есть замеров статистики, в которых число просмотров за час оказалось отрицательным?" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
   - "Сколько замеров с отрицательным приростом просмотров?" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
   - "Сколько замеров, где просмотры уменьшились?" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
   - "Сколько замеров с отрицательным приростом лайков?" → SELECT COUNT(*) FROM video_snapshots WHERE delta_likes_count < 0

9. Прирост просмотров по креатору за дату и время:
   - "На сколько просмотров суммарно выросли все видео креатора с id abc123 в промежутке с 10:00 до 15:00 28 ноября 2025?" → SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'abc123' AND DATE(vs.created_at) = '2025-11-28' AND EXTRACT(HOUR FROM vs.created_at) >= 10 AND EXTRACT(HOUR FROM vs.created_at) < 15
   - "На сколько просмотров выросли видео креатора X с 10 до 15 часов 27 ноября?" → SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'X' AND DATE(vs.created_at) = '2025-11-27' AND EXTRACT(HOUR FROM vs.created_at) >= 10 AND EXTRACT(HOUR FROM vs.created_at) < 15
   - "На сколько просмотров суммарно выросли все видео креатора с id cd87be38b50b4fdd8342bb3c383f3c7d в промежутке с 10:00 до 15:00 28 ноября 2025?" → SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d' AND DATE(vs.created_at) = '2025-11-28' AND EXTRACT(HOUR FROM vs.created_at) >= 10 AND EXTRACT(HOUR FROM vs.created_at) < 15

КРИТИЧЕСКИ ВАЖНО:
- Если в вопросе упоминается КРЕАТОР и ВРЕМЯ/ЗАМЕРЫ → ОБЯЗАТЕЛЬНО используй JOIN между video_snapshots и videos!
- В таблице video_snapshots НЕТ поля creator_id! Для фильтрации по креатору используй: JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'id'
- Для фильтрации по ВРЕМЕНИ (часам) используй: EXTRACT(HOUR FROM created_at) >= час1 AND EXTRACT(HOUR FROM created_at) < час2
- НЕ используй BETWEEN для EXTRACT(HOUR)! Используй >= и <
- НЕ используй creator_id напрямую в video_snapshots - этого поля там нет!
- Если в вопросе НЕ упоминается конкретная дата или период, НЕ добавляй фильтрацию по дате!
"""

PROMPT_TEMPLATE = f"""{SCHEMA_DESCRIPTION}

Вопрос пользователя: "{{user_query}}"

Напиши SQL запрос для PostgreSQL, который вернет одно число.

ПЕРВОЕ ПРАВИЛО - ВЫБОР ТАБЛИЦЫ И ФУНКЦИИ:
- Если вопрос про "суммарное количество просмотров", "набрали просмотры", "опубликованные" → используй таблицу VIDEOS и SUM(views_count)
- Если вопрос про "прирост", "выросли", "замеры", "снапшоты" → используй таблицу VIDEO_SNAPSHOTS и SUM(delta_views_count)
- "суммарное количество просмотров" = SUM(views_count) из videos, а НЕ COUNT(*)!
- Для фильтрации по дате публикации используй video_created_at из таблицы videos
- ВСЕГДА используй DATE(video_created_at) при фильтрации по датам, НЕ используй BETWEEN без DATE()!

КРИТИЧЕСКИ ВАЖНО:
- Только SQL код, без markdown разметки (```sql), без объяснений, без текста до/после
- Запрос должен начинаться с SELECT и возвращать одно число
- Для фильтрации по конкретной дате ОБЯЗАТЕЛЬНО используй DATE(created_at) = 'YYYY-MM-DD'
- Для фильтрации по диапазону дат используй DATE(created_at) BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
- Правильно преобразуй русские даты: "28 ноября 2025" = '2025-11-28', "27 ноября 2025" = '2025-11-27'
- Если вопрос про прирост просмотров за дату - используй SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = 'дата'
- Если вопрос про количество видео - используй COUNT(*) FROM videos
- Если вопрос про уникальные видео - используй COUNT(DISTINCT video_id) FROM video_snapshots

САМОЕ ВАЖНО - ФИЛЬТРАЦИЯ ПО КРЕАТОРУ В VIDEO_SNAPSHOTS:
- Если вопрос про креатора И про замеры/время/прирост → ОБЯЗАТЕЛЬНО используй JOIN!
- Формат: SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'id'
- НИКОГДА не используй creator_id напрямую в video_snapshots - этого поля там НЕТ!
- Для фильтрации по времени используй: EXTRACT(HOUR FROM vs.created_at) >= час1 AND EXTRACT(HOUR FROM vs.created_at) < час2

КРИТИЧЕСКИЕ ПРАВИЛА ДЛЯ КАЖДОГО ТИПА ЗАПРОСА:

0. Если вопрос про СУММАРНОЕ КОЛИЧЕСТВО ПРОСМОТРОВ (сумма всех просмотров):
   - "суммарное количество просмотров" = SUM(views_count) FROM videos
   - "набрали просмотры" = SUM(views_count) FROM videos
   - "опубликованные в [месяц]" = фильтруй по DATE(video_created_at) BETWEEN 'YYYY-MM-01' AND 'YYYY-MM-30' (или 31 для некоторых месяцев)
   - Если упоминается МЕСЯЦ (июнь, ноябрь) → используй BETWEEN для диапазона дат!
   - НЕ используй video_snapshots для суммирования итоговых просмотров!
   - НЕ используй COUNT(*) - это количество видео, а не сумма просмотров!
   - ВСЕГДА используй DATE(video_created_at) при фильтрации по датам!

1. Если вопрос про ОБЩЕЕ КОЛИЧЕСТВО видео → SELECT COUNT(*) FROM videos

2. Если вопрос про количество видео у КРЕАТОРА за ПЕРИОД → 
   SELECT COUNT(*) FROM videos WHERE creator_id = 'id' AND DATE(video_created_at) BETWEEN 'дата1' AND 'дата2'

2a. Если вопрос про количество видео у КРЕАТОРА с определенным количеством ПРОСМОТРОВ (по итоговой статистике) → 
   SELECT COUNT(*) FROM videos WHERE creator_id = 'id' AND views_count > число
   ВАЖНО: используй таблицу VIDEOS, а не video_snapshots! Используй creator_id, а не video_id!

3. Если вопрос про количество видео с определенным количеством ПРОСМОТРОВ (без упоминания креатора) → 
   SELECT COUNT(*) FROM videos WHERE views_count > число

4. Если вопрос про ПРИРОСТ просмотров за КОНКРЕТНУЮ ДАТУ → 
   SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = 'дата'

5. Если вопрос про УНИКАЛЬНЫЕ видео с приростом за дату → 
   SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = 'дата' AND delta_views_count > 0

6. Если вопрос про прирост ЛАЙКОВ/КОММЕНТАРИЕВ за дату → 
   SELECT SUM(delta_likes_count) или SUM(delta_comments_count) FROM video_snapshots WHERE DATE(created_at) = 'дата'

7. Если вопрос про ПЕРИОД (с ... по ...) → используй BETWEEN 'дата1' AND 'дата2'

7a. Если вопрос про прирост по КРЕАТОРУ с ВРЕМЕННЫМ ИНТЕРВАЛОМ:
   - ОБЯЗАТЕЛЬНО используй JOIN: SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'id' AND DATE(vs.created_at) = 'дата' AND EXTRACT(HOUR FROM vs.created_at) >= час1 AND EXTRACT(HOUR FROM vs.created_at) < час2
   - ВАЖНО: В таблице video_snapshots НЕТ поля creator_id! Используй JOIN для доступа к creator_id из таблицы videos!
   - Для фильтрации по часам используй EXTRACT(HOUR FROM vs.created_at) >= час1 AND EXTRACT(HOUR FROM vs.created_at) < час2
   - НЕ используй BETWEEN для EXTRACT(HOUR)! Используй >= и <
   - НЕ используй creator_id напрямую в WHERE без JOIN!

8. Если вопрос про ОТРИЦАТЕЛЬНЫЕ значения или условия сравнения БЕЗ упоминания даты:
   - "Сколько замеров с отрицательным приростом?" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
   - "Сколько замеров, где просмотры уменьшились?" → SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0
   - НЕ добавляй фильтрацию по дате, если дата НЕ упоминается в вопросе!

9. Если вопрос про прирост по КРЕАТОРУ за дату и ВРЕМЯ:
   - "На сколько просмотров выросли видео креатора с id X с 10:00 до 15:00 28 ноября?" → SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'X' AND DATE(vs.created_at) = '2025-11-28' AND EXTRACT(HOUR FROM vs.created_at) >= 10 AND EXTRACT(HOUR FROM vs.created_at) < 15
   - Для фильтрации по времени используй EXTRACT(HOUR FROM created_at) >= час1 AND EXTRACT(HOUR FROM created_at) < час2
   - Для фильтрации по креатору в video_snapshots используй JOIN с таблицей videos

ВАЖНО: 
- Всегда используй DATE() для сравнения дат ТОЛЬКО если в вопросе упоминается конкретная дата или период
- Если в вопросе НЕТ упоминания даты, используй только условия сравнения (>, <, =, >=, <=) без DATE()
- Для фильтрации по дате замера используй DATE(created_at), для фильтрации по дате публикации используй DATE(video_created_at)
- Для фильтрации по ВРЕМЕНИ (часам) используй EXTRACT(HOUR FROM created_at) >= час AND EXTRACT(HOUR FROM created_at) < час
- НЕ используй BETWEEN для времени отдельно! Используй EXTRACT(HOUR FROM ...)

Верни ТОЛЬКО SQL запрос, без markdown, без объяснений, без дополнительного текста.
"""

class LLMQueryBuilder:
    def __init__(self, ollama_url: str = "http://localhost:11434", model: str = "llama3.2"):
        self.ollama_url = ollama_url
        self.model = model
        self.client = httpx.AsyncClient(timeout=60.0)
    
    async def build_query(self, user_query: str) -> str:
        prompt = PROMPT_TEMPLATE.format(user_query=user_query)
        
        try:
            response = await self.client.post(
                f"{self.ollama_url}/api/chat",
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": "Ты эксперт по SQL и PostgreSQL. Твоя задача - преобразовать вопрос на русском языке в SQL запрос."},
                        {"role": "user", "content": prompt}
                    ],
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 500
                    }
                }
            )
            response.raise_for_status()
            data = response.json()
            sql_query = data["message"]["content"].strip()
        except Exception as e:
            raise Exception(f"Ошибка при запросе к Ollama: {e}")
        
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
    
    async def close(self):
        await self.client.aclose()
