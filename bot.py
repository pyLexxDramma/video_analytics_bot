import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from os import getenv
from dotenv import load_dotenv
from database import Database
from llm_query import LLMQueryBuilder

load_dotenv()

bot = Bot(token=getenv('TELEGRAM_BOT_TOKEN'))
dp = Dispatcher()
db = Database(getenv('DATABASE_URL'))
llm = LLMQueryBuilder(getenv('OPENAI_API_KEY'))

@dp.message(Command('start'))
async def start_handler(message: types.Message):
    await message.answer('Привет! Задай вопрос о статистике видео на русском языке.')

@dp.message()
async def query_handler(message: types.Message):
    try:
        user_query = message.text.strip()
        
        if not user_query:
            return
        
        sql_query = await llm.build_query(user_query)
        
        result = await db.execute_query(sql_query)
        
        if result is None:
            answer = '0'
        else:
            answer = str(int(result))
        
        await message.answer(answer)
            
    except Exception as e:
        await message.answer('Произошла ошибка при обработке запроса')

async def main():
    await db.connect()
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        asyncio.run(db.close())
