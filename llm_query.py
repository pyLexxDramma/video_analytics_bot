from openai import AsyncOpenAI
from os import getenv

SCHEMA_DESCRIPTION = """
База данных PostgreSQL с двумя таблицами:

1. videos - итоговая статистика по видео:
   - id UUID PRIMARY KEY
   - creator_id VARCHAR
   - video_created_at TIMESTAMP WITH TIME ZONE
   - views_count INTEGER
   - likes_count INTEGER
   - comments_count INTEGER
   - reports_count INTEGER
   - created_at, updated_at TIMESTAMP

2. video_snapshots - почасовые замеры:
   - id VARCHAR PRIMARY KEY
   - video_id UUID REFERENCES videos(id)
   - views_count, likes_count, comments_count, reports_count INTEGER
   - delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count INTEGER
   - created_at TIMESTAMP WITH TIME ZONE
   - updated_at TIMESTAMP

Правила:
- COUNT(*) для подсчета видео
- SUM(delta_*) для прироста за период
- COUNT(DISTINCT video_id) для уникальных видео
- Даты: "28 ноября 2025" = '2025-11-28', "с 1 по 5 ноября 2025" = BETWEEN '2025-11-01' AND '2025-11-05'
- Для фильтрации по дате публикации: WHERE video_created_at >= '2025-11-01' AND video_created_at <= '2025-11-05'
- Для фильтрации по дате замера: WHERE created_at >= '2025-11-01' AND created_at < '2025-11-06'
"""

PROMPT_TEMPLATE = f"""{SCHEMA_DESCRIPTION}

Вопрос пользователя: "{{user_query}}"

Напиши SQL запрос для PostgreSQL, который вернет одно число.

Требования:
- Только SQL, без markdown, без объяснений
- Запрос должен возвращать одно число через SELECT
- Правильно преобразуй русские даты в формат 'YYYY-MM-DD'
- Для диапазона дат используй BETWEEN или >= и <=
- Если вопрос про прирост - используй SUM(delta_*) из video_snapshots
- Если вопрос про количество видео - используй COUNT(*) или COUNT(DISTINCT ...)
"""

class LLMQueryBuilder:
    def __init__(self, api_key: str):
        self.client = AsyncOpenAI(api_key=api_key)
    
    async def build_query(self, user_query: str) -> str:
        prompt = PROMPT_TEMPLATE.format(user_query=user_query)
        
        response = await self.client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Ты эксперт по SQL и PostgreSQL. Твоя задача - преобразовать вопрос на русском языке в SQL запрос."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=500
        )
        
        sql_query = response.choices[0].message.content.strip()
        
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()
