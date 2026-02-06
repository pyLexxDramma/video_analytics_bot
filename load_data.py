import asyncio
import json
import asyncpg
from datetime import datetime
from os import getenv
from dotenv import load_dotenv

load_dotenv()

def parse_datetime(date_str):
    if isinstance(date_str, str):
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    return date_str

async def load_json_to_db(json_path: str, db_url: str):
    conn = await asyncpg.connect(db_url)
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        videos = data.get('videos', [])
        
        for idx, video in enumerate(videos):
            video_id = video['id']
            await conn.execute("""
                INSERT INTO videos (id, creator_id, video_created_at, views_count, 
                                  likes_count, comments_count, reports_count, 
                                  created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (id) DO UPDATE SET
                    views_count = EXCLUDED.views_count,
                    likes_count = EXCLUDED.likes_count,
                    comments_count = EXCLUDED.comments_count,
                    reports_count = EXCLUDED.reports_count,
                    updated_at = EXCLUDED.updated_at
            """, video_id, video['creator_id'], parse_datetime(video['video_created_at']),
                video['views_count'], video['likes_count'], 
                video['comments_count'], video['reports_count'],
                parse_datetime(video['created_at']), parse_datetime(video['updated_at']))
            
            snapshots = video.get('snapshots', [])
            for snapshot in snapshots:
                await conn.execute("""
                    INSERT INTO video_snapshots 
                    (id, video_id, views_count, likes_count, comments_count, 
                     reports_count, delta_views_count, delta_likes_count, 
                     delta_comments_count, delta_reports_count, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (id) DO UPDATE SET
                        views_count = EXCLUDED.views_count,
                        likes_count = EXCLUDED.likes_count,
                        comments_count = EXCLUDED.comments_count,
                        reports_count = EXCLUDED.reports_count,
                        delta_views_count = EXCLUDED.delta_views_count,
                        delta_likes_count = EXCLUDED.delta_likes_count,
                        delta_comments_count = EXCLUDED.delta_comments_count,
                        delta_reports_count = EXCLUDED.delta_reports_count,
                        updated_at = EXCLUDED.updated_at
                """, snapshot['id'], video_id, snapshot['views_count'],
                    snapshot['likes_count'], snapshot['comments_count'],
                    snapshot['reports_count'], snapshot['delta_views_count'],
                    snapshot['delta_likes_count'], snapshot['delta_comments_count'],
                    snapshot['delta_reports_count'], parse_datetime(snapshot['created_at']),
                    parse_datetime(snapshot['updated_at']))
            
            await conn.execute("""
                UPDATE videos SET
                    views_count = s.views_count,
                    likes_count = s.likes_count,
                    comments_count = s.comments_count,
                    reports_count = s.reports_count,
                    updated_at = NOW()
                FROM (
                    SELECT views_count, likes_count, comments_count, reports_count
                    FROM video_snapshots
                    WHERE video_id = $1
                    ORDER BY created_at DESC
                    LIMIT 1
                ) s
                WHERE id = $1
            """, video_id)
        print(len(videos))

    finally:
        await conn.close()

if __name__ == '__main__':
    import sys
    json_path = sys.argv[1] if len(sys.argv) > 1 else 'videos.json'
    db_url = getenv('DATABASE_URL')
    if not db_url:
        sys.exit(1)
    asyncio.run(load_json_to_db(json_path, db_url))
