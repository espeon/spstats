-- active ppl by month, cumulative
SELECT
    toStartOfMonth(date) AS month,
    count() AS new_users,
    sum(count()) OVER (ORDER BY month) AS cumulative_users
FROM (
    SELECT
        did,
        toDate(min(created_at)) AS date
    FROM sp_stats.stream_place_events
    WHERE created_at IS NOT NULL
    GROUP BY did
)
GROUP BY month
ORDER BY month;


-- total DIDs
SELECT uniqExact(did) as total_dids FROM sp_stats.stream_place_events;

-- above but subtract below
SELECT uniqExact(did) as total_dids FROM sp_stats.stream_place_events WHERE created_at IS NOT NULL;

-- number of people with records with no created_at
SELECT count()
FROM (
    SELECT did
    FROM sp_stats.stream_place_events
    GROUP BY did
    HAVING countIf(created_at IS NOT NULL) = 0
);

-- below are some streamplace specific queries, using json parsing

-- length bucketing
SELECT
    floor(length(record_data.text) / 10) * 10 AS length_bucket,
    count() AS messages
FROM sp_stats.stream_place_events
WHERE collection = 'place.stream.chat.message'
AND length_bucket IS NOT null
GROUP BY length_bucket
ORDER BY length_bucket;

-- most active chatters
SELECT
    did,
    count() AS messages
FROM sp_stats.stream_place_events
WHERE collection = 'place.stream.chat.message'
GROUP BY did
ORDER BY messages DESC
LIMIT 20;


-- chat activity by hour of day UTC
SELECT
    toHour(created_at) AS hour,
    count() AS messages
FROM sp_stats.stream_place_events
WHERE collection = 'place.stream.chat.message'
  AND created_at IS NOT NULL
GROUP BY hour
ORDER BY hour;

-- word occurances in chat
SELECT
    word,
    count() AS occurrences
FROM (
    SELECT splitByChar(' ', lower(toString(record_data.text))) AS words
    FROM sp_stats.stream_place_events
    WHERE collection = 'place.stream.chat.message'
      AND record_data.text IS NOT NULL
)
ARRAY JOIN words AS word
WHERE length(word) > 3
GROUP BY word
ORDER BY occurrences DESC
LIMIT 50;

-- global msgs per chatter
SELECT
    toDate(created_at) AS day,
    count(DISTINCT did) AS chatters,
    count() AS messages,
    round(messages / chatters, 1) AS msgs_per_chatter
FROM sp_stats.stream_place_events
WHERE collection = 'place.stream.chat.message'
  AND created_at IS NOT NULL
GROUP BY day
ORDER BY day;

-- streamer churn
SELECT
    did,
    max(toDate(created_at)) AS last_stream,
    dateDiff('day', max(toDate(created_at)), today()) AS days_since_last,
    count() AS total_streams
FROM sp_stats.stream_place_events
WHERE collection = 'place.stream.livestream'
  AND created_at IS NOT NULL
GROUP BY did
HAVING days_since_last > 30 AND total_streams > 5
ORDER BY total_streams DESC;


-- streamer retention
SELECT
    toStartOfMonth(first_stream) AS cohort,
    count() AS streamers,
    countIf(last_stream >= first_stream + INTERVAL 7 DAY) AS retained_7d,
    countIf(last_stream >= first_stream + INTERVAL 30 DAY) AS retained_30d,
    round(retained_7d / count() * 100, 1) AS pct_7d,
    round(retained_30d / count() * 100, 1) AS pct_30d
FROM (
    SELECT
        did,
        min(toDate(created_at)) AS first_stream,
        max(toDate(created_at)) AS last_stream
    FROM sp_stats.stream_place_events
    WHERE collection = 'place.stream.livestream'
      AND created_at IS NOT NULL
    GROUP BY did
)
GROUP BY cohort
ORDER BY cohort;

-- streaks
WITH streamer_days AS (
    SELECT DISTINCT
        did,
        toDate(created_at) AS stream_date
    FROM sp_stats.stream_place_events
    WHERE collection = 'place.stream.livestream'
      AND created_at IS NOT NULL
),
with_rn AS (
    SELECT
        did,
        stream_date,
        stream_date - row_number() OVER (PARTITION BY did ORDER BY stream_date) AS streak_group
    FROM streamer_days
),
streaks AS (
    SELECT
        did,
        streak_group,
        count() AS streak_len
    FROM with_rn
    GROUP BY did, streak_group
)
SELECT
    did,
    max(streak_len) AS longest_streak
FROM streaks
GROUP BY did
ORDER BY longest_streak DESC
LIMIT 20;

-- streaming frequency per streamer
SELECT
    did,
    count() AS total_streams,
    count(DISTINCT toDate(created_at)) AS days_streamed,
    min(toDate(created_at)) AS first_stream,
    max(toDate(created_at)) AS last_stream,
    dateDiff('day', min(toDate(created_at)), max(toDate(created_at))) + 1 AS day_span,
    round(count(DISTINCT toDate(created_at)) / (dateDiff('day', min(toDate(created_at)), max(toDate(created_at))) + 1) * 100, 1) AS consistency_pct
FROM sp_stats.stream_place_events
WHERE collection = 'place.stream.livestream'
  AND created_at IS NOT NULL
GROUP BY did
HAVING day_span > 7
ORDER BY consistency_pct DESC;
