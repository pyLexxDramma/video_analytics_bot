import asyncpg

class Database:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.pool = None
    
    async def connect(self):
        self.pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=5)
    
    async def close(self):
        if self.pool:
            await self.pool.close()
    
    async def execute_query(self, query: str, *args):
        async with self.pool.acquire() as conn:
            try:
                result = await conn.fetchval(query, *args)
                return result
            except Exception:
                raise
