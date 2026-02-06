import httpx

SCHEMA_DESCRIPTION = """
# Схема данных: видео и аналитика

PostgreSQL. Две таблицы с разной семантикой — от этого зависит, что считать и откуда брать данные.

---

## 1. Таблица `videos` (текущее состояние по каждому видео)

**Смысл:** одна строка = одно видео с актуальной (итоговой) статистикой на момент последнего обновления.

| Поле | Тип | Смысл |
|------|-----|--------|
| id | UUID | идентификатор видео |
| creator_id | VARCHAR | идентификатор креатора (есть только в этой таблице) |
| video_created_at | TIMESTAMPTZ | дата и время публикации видео |
| views_count, likes_count, comments_count, reports_count | INTEGER | текущие итоговые значения |
| created_at, updated_at | TIMESTAMPTZ | служебные |

**Когда использовать:** вопросы про количество видео, про итоговые просмотры/лайки, про креатора, про дату публикации, про «набрали всего», «по итоговой статистике», «опубликованные в [период]».

---

## 2. Таблица `video_snapshots` (почасовые замеры)

**Смысл:** одна строка = один почасовой замер статистики по одному видео. Есть приросты за час (delta_*).

| Поле | Тип | Смысл |
|------|-----|--------|
| id | VARCHAR | идентификатор замера |
| video_id | UUID | ссылка на videos(id) |
| views_count, likes_count, … | INTEGER | значения на момент замера |
| delta_views_count, delta_likes_count, … | INTEGER | прирост за этот час |
| created_at | TIMESTAMPTZ | дата и время замера |

**Важно:** в `video_snapshots` нет creator_id. Чтобы фильтровать замеры по креатору — делай JOIN с `videos`:  
`FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = '...'`

**Когда использовать:** вопросы про прирост за дату/период, про «выросли за день», «замеры», «снапшоты», про интервал по часам (например с 10:00 до 15:00).

---

## 3. Как выбрать таблицу и что считать

- **«Сколько видео»** (штук) → `videos`, агрегат **COUNT(*)**.
- **«Суммарные просмотры» / «всего просмотров набрали»** → `videos`, агрегат **SUM(views_count)**. Не путать с COUNT(*) (это число видео).
- **«Сколько видео с просмотрами больше N»** / **«по итоговой статистике»** → `videos`, **COUNT(*)** и **WHERE views_count > N**. Без фильтра по дате замера.
- **«Прирост просмотров/лайков за дату или период»** → `video_snapshots`, **SUM(delta_views_count)** или SUM(delta_likes_count) и т.д., фильтр по **DATE(created_at)**.
- **«Сколько разных видео получили прирост за дату»** → `video_snapshots`, **COUNT(DISTINCT video_id)** и при необходимости `delta_views_count > 0`.
- **«Сколько замеров с отрицательным приростом»** → `video_snapshots`, **COUNT(*)** и `WHERE delta_views_count < 0` (или другая delta_*). Без фильтра по дате, если дата не указана.
- **Креатор + замеры/прирост/время** → всегда **video_snapshots JOIN videos** по `vs.video_id = v.id`, фильтр по `v.creator_id`.

---

## 4. Даты и время

- **Дата публикации видео** — поле `video_created_at` в таблице `videos`. Фильтр всегда через **DATE(video_created_at)** (например `BETWEEN 'YYYY-MM-01' AND 'YYYY-MM-DD'`).
- **Дата замера** — поле `created_at` в таблице `video_snapshots`. Фильтр через **DATE(created_at)**.
- Русские даты в запросе переводи в ISO: «28 ноября 2025» → `'2025-11-28'`, «1 ноября 2025» → `'2025-11-01'`. Месяц: «ноябрь 2025» → `BETWEEN '2025-11-01' AND '2025-11-30'`, «июнь 2025» → `BETWEEN '2025-06-01' AND '2025-06-30'`.
- **«С X по Y включительно»** — используй **BETWEEN 'дата1' AND 'дата2'** (обе границы включаются). Не использовать `>= и <` для «включительно».
- **Часы в течение дня** (например с 10:00 до 15:00): фильтр по `created_at` через **EXTRACT(HOUR FROM created_at) >= 10 AND EXTRACT(HOUR FROM created_at) < 15**. Не использовать BETWEEN по времени как строке.

---

## 5. Краткие шаблоны

- Количество видео (всего): `SELECT COUNT(*) FROM videos`
- Количество видео креатора за период по дате публикации: `SELECT COUNT(*) FROM videos WHERE creator_id = 'ID' AND DATE(video_created_at) BETWEEN 'Y-M-D' AND 'Y-M-D'`
- Количество видео креатора с просмотрами > N (итог): `SELECT COUNT(*) FROM videos WHERE creator_id = 'ID' AND views_count > N`
- Суммарные просмотры (всех/за период публикации/по креатору): `SELECT SUM(views_count) FROM videos` [+ WHERE по video_created_at и/или creator_id]
- Прирост просмотров за дату: `SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = 'Y-M-D'`
- Прирост по креатору за дату и часы: `SELECT SUM(vs.delta_views_count) FROM video_snapshots vs JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = 'ID' AND DATE(vs.created_at) = 'Y-M-D' AND EXTRACT(HOUR FROM vs.created_at) >= H1 AND EXTRACT(HOUR FROM vs.created_at) < H2`
- Уникальные видео с приростом за дату: `SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = 'Y-M-D' AND delta_views_count > 0`
- Замеры с отрицательным приростом: `SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0`

Все значения (ID креатора, даты, числа, часы) бери только из текущего вопроса пользователя, не из примеров.
"""

PROMPT_TEMPLATE = f"""{SCHEMA_DESCRIPTION}

---

Вопрос пользователя: "{{user_query}}"

Задание:
1. Извлеки из вопроса все нужные значения: ID креатора (после «id » или «id»), даты (переведи «N ноября 2025» в '2025-11-N'), числа (порог просмотров и т.д.), часы (если указаны). Используй только эти значения, не подставляй примеры из описания схемы.
2. По смыслу вопроса выбери таблицу(ы) и агрегат по правилам из раздела «Как выбрать таблицу и что считать».
3. Напиши один SQL-запрос для PostgreSQL, который возвращает одно число (скаляр).

Формат ответа: только SQL, без markdown (без ```sql), без пояснений до или после. Запрос должен начинаться с SELECT.
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
                        {
                            "role": "system",
                            "content": "Ты эксперт по SQL и PostgreSQL. По описанию схемы и правилам определи, из каких таблиц что брать и как считать. Преобразуй вопрос на русском в один SQL-запрос, возвращающий одно число.",
                        },
                        {"role": "user", "content": prompt},
                    ],
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 500,
                    },
                },
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
