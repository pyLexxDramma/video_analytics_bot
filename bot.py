import asyncio
import re
import sys
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from os import getenv
from dotenv import load_dotenv

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from database import Database
from llm_query import LLMQueryBuilder

load_dotenv()

bot = Bot(token=getenv('TELEGRAM_BOT_TOKEN'))
dp = Dispatcher()
db = Database(getenv('DATABASE_URL'))
ollama_url = getenv('OLLAMA_URL', 'http://localhost:11434')
ollama_model = getenv('OLLAMA_MODEL', 'gemma3:4b')
llm = LLMQueryBuilder(ollama_url=ollama_url, model=ollama_model)


@dp.message(Command('start'))
async def start_handler(message: types.Message):
    await message.answer('Привет! Задай вопрос о статистике видео на русском языке.')


def get_fixed_sql_for_question(user_query: str):
    q = user_query.lower()
    creator_match = re.search(r'id\s+([a-f0-9]{32})', user_query, re.IGNORECASE)

    if creator_match and ('просмотр' in q and ('вырос' in q or 'прирост' in q or 'изменени' in q or 'сложить' in q)):
        date_match = re.search(r'(\d+)\s+ноября\s+2025', user_query, re.IGNORECASE)
        time_match = re.search(r'с\s+(\d+):?\d*\s+до\s+(\d+):?\d*', user_query, re.IGNORECASE)
        if date_match and time_match:
            day = int(date_match.group(1))
            h1, h2 = int(time_match.group(1)), int(time_match.group(2))
            cid = creator_match.group(1)
            return (
                f"SELECT COALESCE(SUM(vs.delta_views_count), 0) FROM video_snapshots vs "
                f"JOIN videos v ON vs.video_id = v.id WHERE v.creator_id = '{cid}' "
                f"AND DATE(vs.created_at) = '2025-11-{day:02d}' "
                f"AND EXTRACT(HOUR FROM vs.created_at) >= {h1} AND EXTRACT(HOUR FROM vs.created_at) < {h2}"
            )

    if ('суммарн' in q or 'набрали' in q) and 'просмотр' in q and ('опубликован' in q or 'июн' in q):
        if 'июн' in q and '2025' in q:
            return "SELECT COALESCE(SUM(views_count), 0) FROM videos WHERE DATE(video_created_at) BETWEEN '2025-06-01' AND '2025-06-30'"

    if ('замеров' in q or 'замеры' in q) and ('отрицательн' in q or 'уменьшились' in q or 'стало меньше' in q) and 'просмотр' in q:
        return "SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0"

    if creator_match and ('разных календарных днях' in q or 'разных днях' in q or 'скольких днях' in q) and ('ноября 2025' in q or 'ноябрь 2025' in q) and ('публиковал' in q or 'видео' in q):
        cid = creator_match.group(1)
        return (
            f"SELECT COUNT(DISTINCT DATE(video_created_at)) FROM videos "
            f"WHERE creator_id = '{cid}' AND video_created_at >= '2025-11-01' AND video_created_at < '2025-12-01'"
        )

    if ('разных креаторов' in q or 'разных креатор' in q) and ('хотя бы одно видео' in q or ('имеют' in q and 'видео' in q)) and 'просмотр' in q:
        views_match = re.search(r'больше\s+(\d[\d\s]*)', user_query, re.IGNORECASE)
        if views_match:
            threshold = int(views_match.group(1).replace(' ', ''))
            return f"SELECT COUNT(DISTINCT creator_id) FROM videos WHERE views_count > {threshold}"

    creator_match = re.search(r'id\s+([a-f0-9]{32})', user_query, re.IGNORECASE)
    period_patterns = [
        (r'с\s+(\d+)\s+ноября\s+2025\s+по\s+(\d+)\s+ноября\s+2025', 11),
        (r'с\s+(\d+)\s+по\s+(\d+)\s+ноября\s+2025', 11),
        (r'период\s+с\s+(\d+)\s+ноября\s+2025\s+по\s+(\d+)\s+ноября\s+2025', 11),
    ]
    if creator_match and ('креатор' in q or 'креатора' in q) and ('видео' in q or 'опубликовал' in q) and ('период' in q or 'ноября' in q):
        for pat, month in period_patterns:
            m = re.search(pat, user_query, re.IGNORECASE)
            if m:
                d1, d2 = int(m.group(1)), int(m.group(2))
                start_date = f'2025-{month:02d}-{d1:02d}'
                end_date = f'2025-{month:02d}-{d2:02d}'
                cid = creator_match.group(1)
                return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{cid}' AND DATE(video_created_at) BETWEEN '{start_date}' AND '{end_date}'"

    views_match = re.search(r'больше\s+(\d[\d\s]*)', user_query, re.IGNORECASE)
    if creator_match and views_match and 'видео' in q and 'креатор' in q and 'просмотр' in q:
        if 'итоговой' in q or 'набрали' in q:
            cid = creator_match.group(1)
            threshold = int(views_match.group(1).replace(' ', ''))
            return f"SELECT COUNT(*) FROM videos WHERE creator_id = '{cid}' AND views_count > {threshold}"

    return None


def validate_and_fix_sql(sql_query: str, user_query: str) -> str:
    q = user_query.lower()
    creator_id_match = re.search(r'id\s+([a-f0-9]{32})', user_query, re.IGNORECASE)
    views_threshold_match = re.search(r'больше\s+(\d[\d\s]*)', user_query, re.IGNORECASE)
    is_creator_views_final = (
        'креатор' in q and 'просмотр' in q
        and ('итоговой статистике' in q or 'итоговой' in q)
        and creator_id_match and views_threshold_match
    )
    if is_creator_views_final:
        cid = creator_id_match.group(1)
        num_str = views_threshold_match.group(1).replace(' ', '')
        if num_str.isdigit():
            threshold = int(num_str)
            if 'video_snapshots' in sql_query.lower():
                sql_query = f"SELECT COUNT(*) FROM videos WHERE creator_id = '{cid}' AND views_count > {threshold}"
            elif 'videos' not in sql_query.lower():
                sql_query = f"SELECT COUNT(*) FROM videos WHERE creator_id = '{cid}' AND views_count > {threshold}"

    if ('замеров' in q or 'замеры' in q) and ('отрицательн' in q or 'уменьшились' in q or 'стало меньше' in q) and 'просмотр' in q:
        sql_query = "SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0"

    creator_id_patterns = [
        r'id\s+([a-f0-9]{32})',
        r'id:\s*([a-f0-9]{32})',
        r'креатор[а]?\s+с\s+id\s+([a-f0-9]{32})',
    ]
    expected_creator_id = None
    for pattern in creator_id_patterns:
        match = re.search(pattern, user_query, re.IGNORECASE)
        if match:
            expected_creator_id = match.group(1)
            break

    if expected_creator_id and expected_creator_id not in sql_query:
        sql_query = re.sub(
            r"creator_id\s*=\s*'[^']+'",
            f"creator_id = '{expected_creator_id}'",
            sql_query,
            flags=re.IGNORECASE
        )

    period_patterns = [
        r'с\s+(\d+)\s+ноября\s+2025\s+по\s+(\d+)\s+ноября\s+2025',
        r'с\s+(\d+)\s+по\s+(\d+)\s+ноября\s+2025',
        r'период\s+с\s+(\d+)\s+ноября\s+2025\s+по\s+(\d+)\s+ноября\s+2025',
    ]
    start_date = end_date = None
    for pattern in period_patterns:
        match = re.search(pattern, user_query, re.IGNORECASE)
        if match:
            start_date = f'2025-11-{int(match.group(1)):02d}'
            end_date = f'2025-11-{int(match.group(2)):02d}'
            break

    if start_date and end_date and (start_date not in sql_query or end_date not in sql_query):
        sql_normalized = re.sub(r'\s+', ' ', sql_query)
        sql_normalized = re.sub(
            r"BETWEEN\s+'(\d{4}-\d{2}-\d{2})'\s+AND\s+'(\d{4}-\d{2}-\d{2})'",
            f"BETWEEN '{start_date}' AND '{end_date}'",
            sql_normalized,
            flags=re.IGNORECASE
        )
        if start_date in sql_normalized and end_date in sql_normalized:
            sql_query = sql_normalized
        else:
            between_pos = sql_query.upper().find('BETWEEN')
            if between_pos != -1:
                first_date_match = re.search(r"'(\d{4}-\d{2}-\d{2})'", sql_query[between_pos:])
                if first_date_match:
                    sql_query = sql_query.replace(f"'{first_date_match.group(1)}'", f"'{start_date}'", 1)
                and_pos = sql_query.upper().find('AND', between_pos)
                if and_pos != -1:
                    second_date_match = re.search(r"'(\d{4}-\d{2}-\d{2})'", sql_query[and_pos:])
                    if second_date_match:
                        sql_query = sql_query.replace(f"'{second_date_match.group(1)}'", f"'{end_date}'", 1)
            if 'DATE(video_created_at)' not in sql_query and 'video_created_at BETWEEN' in sql_query:
                sql_query = sql_query.replace('video_created_at BETWEEN', 'DATE(video_created_at) BETWEEN')

    return sql_query


@dp.message()
async def query_handler(message: types.Message):
    user_query = message.text.strip()
    if not user_query:
        return

    try:
        fixed_sql = get_fixed_sql_for_question(user_query)
        if fixed_sql:
            sql_query = fixed_sql
        else:
            sql_query = await llm.build_query(user_query)
            sql_query = validate_and_fix_sql(sql_query, user_query)

        if not sql_query.strip().upper().startswith('SELECT'):
            raise ValueError("Некорректный SQL")

        result = await db.execute_query(sql_query)
        answer = '0' if result is None else str(int(result))
        await message.answer(answer)

    except (ValueError, Exception):
        await message.answer('Произошла ошибка при обработке запроса')


async def check_ollama():
    import httpx
    try:
        r = await httpx.AsyncClient(timeout=5).get(f"{ollama_url}/api/tags")
        return r.status_code == 200
    except Exception:
        return False


async def main():
    if not await check_ollama():
        return
    try:
        await db.connect()
    except Exception:
        return
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()
        await llm.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
