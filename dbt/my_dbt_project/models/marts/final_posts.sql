SELECT
    userid,
    COUNT(*) AS total_posts,
    MIN(id) AS first_post_id,
    MAX(id) AS last_post_id
FROM stg_posts
GROUP BY userid
ORDER BY total_posts DESC