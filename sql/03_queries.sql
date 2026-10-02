USE booking_engine;

-- Set the theatre and local calendar date requested by the caller.
SET @theatre_id = 1;
SET @show_date = '2026-10-01';

-- P2: every scheduled show and its time at this theatre on this date.
SELECT
    t.theatre_id,
    t.theatre_name,
    sh.show_id,
    m.movie_id,
    m.title AS movie_title,
    m.language_code,
    sh.show_date,
    sh.starts_at,
    sh.ends_at,
    sc.screen_name,
    sh.base_price,
    (
        SELECT COUNT(*)
        FROM show_seats ss
        WHERE ss.show_id = sh.show_id
          AND (ss.status = 'AVAILABLE'
               OR (ss.status = 'HELD' AND ss.held_until <= UTC_TIMESTAMP(6)))
    ) AS available_seats
FROM theatres t
JOIN screens sc ON sc.theatre_id = t.theatre_id
JOIN shows sh ON sh.screen_id = sc.screen_id
JOIN movies m ON m.movie_id = sh.movie_id
WHERE t.theatre_id = @theatre_id
  AND sh.show_date = @show_date
  AND sh.status = 'SCHEDULED'
ORDER BY sh.starts_at, sc.screen_name;

-- Date picker: seven consecutive dates starting today, including dates with no shows.
WITH RECURSIVE next_dates (show_date, day_number) AS (
    SELECT CURRENT_DATE, 1
    UNION ALL
    SELECT show_date + INTERVAL 1 DAY, day_number + 1
    FROM next_dates
    WHERE day_number < 7
)
SELECT
    d.show_date,
    EXISTS (
        SELECT 1
        FROM screens sc
        JOIN shows sh ON sh.screen_id = sc.screen_id
        WHERE sc.theatre_id = @theatre_id
          AND sh.show_date = d.show_date
          AND sh.status = 'SCHEDULED'
    ) AS has_scheduled_shows
FROM next_dates d
ORDER BY d.show_date;

-- Seat map for a selected show. Expired holds are presented as available.
SET @show_id = 1;
SELECT
    ss.show_id,
    ss.seat_id,
    se.row_label,
    se.seat_number,
    se.seat_type,
    ss.price,
    CASE
        WHEN ss.status = 'HELD' AND ss.held_until <= UTC_TIMESTAMP(6) THEN 'AVAILABLE'
        ELSE ss.status
    END AS effective_status
FROM show_seats ss
JOIN seats se ON se.seat_id = ss.seat_id
WHERE ss.show_id = @show_id
ORDER BY se.row_label, se.seat_number;
