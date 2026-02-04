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
ollama_url = getenv('OLLAMA_URL', 'http://localhost:11434')
ollama_model = getenv('OLLAMA_MODEL', 'gemma3:4b')
llm = LLMQueryBuilder(ollama_url=ollama_url, model=ollama_model)

@dp.message(Command('start'))
async def start_handler(message: types.Message):
    await message.answer('Привет! Задай вопрос о статистике видео на русском языке.')

@dp.message()
async def query_handler(message: types.Message):
    user_query = message.text.strip()
    
    if not user_query:
        return
    
    try:
        print(f"\n[ВХОДЯЩИЙ ЗАПРОС] {user_query}")
        
        sql_query = await llm.build_query(user_query)
        print(f"[SQL] {sql_query}")
        
        if not sql_query.strip().upper().startswith('SELECT'):
            raise ValueError(f"Некорректный SQL запрос: должен начинаться с SELECT")
        
        result = await db.execute_query(sql_query)
        print(f"[РЕЗУЛЬТАТ БД] {result} (тип: {type(result)})")
        
        if result is None:
            answer = '0'
        else:
            answer = str(int(result))
        
        print(f"[ОТВЕТ БОТА] {answer}")
        await message.answer(answer)
            
    except ValueError as e:
        print(f"[ОШИБКА ВАЛИДАЦИИ] {e}")
        await message.answer('Произошла ошибка при обработке запроса')
    except Exception as e:
        import traceback
        error_msg = str(e)
        print(f"\n[ОШИБКА] Запрос: '{user_query}'")
        print(f"[ОШИБКА] Сообщение: {error_msg}")
        traceback.print_exc()
        await message.answer('Произошла ошибка при обработке запроса')

async def check_ollama():
    import httpx
    try:
        response = await httpx.AsyncClient(timeout=5).get(f"{ollama_url}/api/tags")
        if response.status_code == 200:
            print(f"[✓] Ollama работает на {ollama_url}")
            return True
        else:
            print(f"[✗] Ollama отвечает с кодом: {response.status_code}")
            return False
    except Exception as e:
        print(f"[✗] Ollama НЕ запущена! Ошибка: {e}")
        print(f"[!] Запустите Ollama: ollama serve")
        print(f"[!] Или проверьте OLLAMA_URL в .env (текущий: {ollama_url})")
        return False

async def main():
    print("=" * 60)
    print("Запуск Telegram бота для аналитики видео")
    print("=" * 60)
    
    print("\n[1/3] Проверка Ollama...")
    if not await check_ollama():
        print("\n[ОШИБКА] Бот не может работать без Ollama!")
        print("Запустите Ollama и перезапустите бота.")
        return
    
    print("\n[2/3] Подключение к базе данных...")
    try:
        await db.connect()
        print("[✓] Подключение к БД установлено")
    except Exception as e:
        print(f"[✗] Ошибка подключения к БД: {e}")
        return
    
    print("\n[3/3] Запуск бота...")
    print(f"[✓] Бот готов к работе!")
    print(f"[✓] Username: @video_LD_bot")
    print("=" * 60)
    print("\nДля остановки нажмите Ctrl+C\n")
    
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
