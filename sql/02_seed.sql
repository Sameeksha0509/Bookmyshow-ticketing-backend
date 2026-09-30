USE booking_engine;

INSERT INTO app_users (email, full_name) VALUES
('aisha@example.com', 'Aisha Rao'),
('ben@example.com', 'Ben Carter');

INSERT INTO theatres (theatre_name, city, address) VALUES
('PVR Orion Mall', 'Bengaluru', '26 Brigade Road'),
('Cinepolis Seasons', 'Pune', 'Magarpatta Road');

INSERT INTO screens (theatre_id, screen_name, capacity) VALUES
(1, 'Screen 1', 6),
(1, 'Screen 2', 4),
(2, 'Screen 1', 4);

INSERT INTO seats (screen_id, row_label, seat_number, seat_type) VALUES
(1, 'A', 1, 'REGULAR'), (1, 'A', 2, 'REGULAR'), (1, 'A', 3, 'REGULAR'),
(1, 'B', 1, 'PREMIUM'), (1, 'B', 2, 'PREMIUM'), (1, 'B', 3, 'PREMIUM'),
(2, 'A', 1, 'REGULAR'), (2, 'A', 2, 'REGULAR'), (2, 'B', 1, 'PREMIUM'), (2, 'B', 2, 'PREMIUM'),
(3, 'A', 1, 'REGULAR'), (3, 'A', 2, 'REGULAR'), (3, 'B', 1, 'PREMIUM'), (3, 'B', 2, 'PREMIUM');

INSERT INTO movies (title, duration_minutes, language_code, certificate) VALUES
('The Last Monsoon', 142, 'EN', 'U/A'),
('Orbit 9', 118, 'HI', 'U/A'),
('Paper Boats', 105, 'EN', 'U');

INSERT INTO shows (screen_id, movie_id, show_date, starts_at, ends_at, base_price) VALUES
(1, 1, '2026-10-01', '2026-10-01 10:00:00', '2026-10-01 12:22:00', 250.00),
(1, 2, '2026-10-01', '2026-10-01 13:30:00', '2026-10-01 15:28:00', 220.00),
(1, 1, '2026-10-01', '2026-10-01 18:30:00', '2026-10-01 20:52:00', 300.00),
(2, 3, '2026-10-01', '2026-10-01 11:15:00', '2026-10-01 13:00:00', 180.00),
(2, 2, '2026-10-02', '2026-10-02 19:00:00', '2026-10-02 20:58:00', 240.00),
(3, 1, '2026-10-01', '2026-10-01 20:00:00', '2026-10-01 22:22:00', 200.00);

INSERT INTO show_seats (show_id, seat_id, price)
SELECT sh.show_id, se.seat_id,
       sh.base_price + CASE se.seat_type WHEN 'PREMIUM' THEN 50.00 WHEN 'RECLINER' THEN 120.00 ELSE 0.00 END
FROM shows sh
JOIN seats se ON se.screen_id = sh.screen_id;
