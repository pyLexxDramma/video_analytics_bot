-- Итоговая статистика в videos = последний по created_at снимок из video_snapshots
UPDATE videos v SET
    views_count = s.views_count,
    likes_count = s.likes_count,
    comments_count = s.comments_count,
    reports_count = s.reports_count,
    updated_at = NOW()
FROM (
    SELECT DISTINCT ON (video_id) video_id, views_count, likes_count, comments_count, reports_count
    FROM video_snapshots
    ORDER BY video_id, created_at DESC
) s
WHERE v.id = s.video_id;
