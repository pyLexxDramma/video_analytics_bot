import httpx
from os import getenv

SCHEMA_DESCRIPTION = """
База данных PostgreSQL с двумя таблицами:

1. videos - итоговая статистика по видео:
   - id UUID PRIMARY KEY
   - creator_id VARCHAR
   - video_created_at TIMESTAMP WITH TIME ZONE (дата публикации видео)
   - views_count INTEGER (текущее количество просмотров)
   - likes_count INTEGER
   - comments_count INTEGER
   - reports_count INTEGER
   - created_at, updated_at TIMESTAMP

2. video_snapshots - почасовые замеры статистики:
   - id VARCHAR PRIMARY KEY
   - video_id UUID REFERENCES videos(id)
   - views_count, likes_count, comments_count, reports_count INTEGER (значения на момент замера)
   - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count INTEGER (прирост за час)
   - created_at TIMESTAMP WITH TIME ZONE (дата и время замера)
   - updated_at TIMESTAMP

ВАЖНЫЕ ПРАВИЛА:
- COUNT(*) для подсчета количества видео
- SUM(delta_views_count) для суммирования прироста просмотров за период
- COUNT(DISTINCT video_id) для подсчета уникальных видео
- Для фильтрации по конкретной ДАТЕ используй: DATE(created_at) = 'YYYY-MM-DD'
- Для фильтрации по диапазону ДАТ используй: DATE(created_at) BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
- Для фильтрации по дате публикации видео: DATE(video_created_at) = 'YYYY-MM-DD' или BETWEEN
- Преобразование русских дат: "28 ноября 2025" = '2025-11-28', "1 ноября 2025" = '2025-11-01', "5 ноября 2025" = '2025-11-05'

ПРИМЕРЫ SQL ЗАПРОСОВ:

1. Подсчет общего количества видео:
   - "Сколько всего видео есть в системе?" → SELECT COUNT(*) FROM videos
   - "Сколько всего видео?" → SELECT COUNT(*) FROM videos

2. Подсчет видео по креатору и дате публикации:
   - "Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'
   - "Сколько видео у креатора с id X вышло с 1 по 5 ноября 2025?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'X' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'
   - "Сколько видео у креатора abc вышло с 1 по 5 ноября?" → SELECT COUNT(*) FROM videos WHERE creator_id = 'abc' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'

3. Подсчет видео по количеству просмотров:
   - "Сколько видео набрало больше 100000 просмотров за всё время?" → SELECT COUNT(*) FROM videos WHERE views_count > 100000
   - "Сколько видео набрало больше 100 000 просмотров за всё время?" → SELECT COUNT(*) FROM videos WHERE views_count > 100000
   - "Сколько видео имеет более 50000 просмотров?" → SELECT COUNT(*) FROM videos WHERE views_count > 50000

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
"""

PROMPT_TEMPLATE = f"""{SCHEMA_DESCRIPTION}

Вопрос пользователя: "{{user_query}}"

Напиши SQL запрос для PostgreSQL, который вернет одно число.

КРИТИЧЕСКИ ВАЖНО:
- Только SQL код, без markdown разметки (```sql), без объяснений, без текста до/после
- Запрос должен начинаться с SELECT и возвращать одно число
- Для фильтрации по конкретной дате ОБЯЗАТЕЛЬНО используй DATE(created_at) = 'YYYY-MM-DD'
- Для фильтрации по диапазону дат используй DATE(created_at) BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
- Правильно преобразуй русские даты: "28 ноября 2025" = '2025-11-28', "27 ноября 2025" = '2025-11-27'
- Если вопрос про прирост просмотров за дату - используй SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = 'дата'
- Если вопрос про количество видео - используй COUNT(*) FROM videos
- Если вопрос про уникальные видео - используй COUNT(DISTINCT video_id) FROM video_snapshots

КРИТИЧЕСКИЕ ПРАВИЛА ДЛЯ КАЖДОГО ТИПА ЗАПРОСА:

1. Если вопрос про ОБЩЕЕ КОЛИЧЕСТВО видео → SELECT COUNT(*) FROM videos

2. Если вопрос про количество видео у КРЕАТОРА за ПЕРИОД → 
   SELECT COUNT(*) FROM videos WHERE creator_id = 'id' AND DATE(video_created_at) BETWEEN 'дата1' AND 'дата2'

3. Если вопрос про количество видео с определенным количеством ПРОСМОТРОВ → 
   SELECT COUNT(*) FROM videos WHERE views_count > число

4. Если вопрос про ПРИРОСТ просмотров за КОНКРЕТНУЮ ДАТУ → 
   SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = 'дата'

5. Если вопрос про УНИКАЛЬНЫЕ видео с приростом за дату → 
   SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = 'дата' AND delta_views_count > 0

6. Если вопрос про прирост ЛАЙКОВ/КОММЕНТАРИЕВ за дату → 
   SELECT SUM(delta_likes_count) или SUM(delta_comments_count) FROM video_snapshots WHERE DATE(created_at) = 'дата'

7. Если вопрос про ПЕРИОД (с ... по ...) → используй BETWEEN 'дата1' AND 'дата2'

ВАЖНО: Всегда используй DATE() для сравнения дат. Для фильтрации по дате замера используй DATE(created_at), для фильтрации по дате публикации используй DATE(video_created_at).

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
