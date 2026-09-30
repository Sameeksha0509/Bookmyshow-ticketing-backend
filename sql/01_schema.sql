CREATE DATABASE IF NOT EXISTS booking_engine;
USE booking_engine;

SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS payment_webhook_events;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS booking_seats;
DROP TABLE IF EXISTS bookings;
DROP TABLE IF EXISTS show_seats;
DROP TABLE IF EXISTS shows;
DROP TABLE IF EXISTS seats;
DROP TABLE IF EXISTS screens;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS theatres;
DROP TABLE IF EXISTS app_users;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE app_users (
    user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    email VARCHAR(254) NOT NULL,
    full_name VARCHAR(120) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (user_id),
    UNIQUE KEY uq_app_users_email (email)
) ENGINE = InnoDB;

CREATE TABLE theatres (
    theatre_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    theatre_name VARCHAR(160) NOT NULL,
    city VARCHAR(80) NOT NULL,
    address VARCHAR(255) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (theatre_id),
    KEY ix_theatres_city_name (city, theatre_name)
) ENGINE = InnoDB;

CREATE TABLE screens (
    screen_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    theatre_id BIGINT UNSIGNED NOT NULL,
    screen_name VARCHAR(80) NOT NULL,
    capacity SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (screen_id),
    UNIQUE KEY uq_screens_theatre_name (theatre_id, screen_name),
    CONSTRAINT fk_screens_theatre FOREIGN KEY (theatre_id) REFERENCES theatres(theatre_id),
    CONSTRAINT chk_screens_capacity CHECK (capacity > 0)
) ENGINE = InnoDB;

CREATE TABLE seats (
    seat_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    screen_id BIGINT UNSIGNED NOT NULL,
    row_label VARCHAR(5) NOT NULL,
    seat_number SMALLINT UNSIGNED NOT NULL,
    seat_type ENUM('REGULAR', 'PREMIUM', 'RECLINER') NOT NULL DEFAULT 'REGULAR',
    PRIMARY KEY (seat_id),
    UNIQUE KEY uq_seats_position (screen_id, row_label, seat_number),
    KEY ix_seats_screen (screen_id),
    CONSTRAINT fk_seats_screen FOREIGN KEY (screen_id) REFERENCES screens(screen_id),
    CONSTRAINT chk_seats_number CHECK (seat_number > 0)
) ENGINE = InnoDB;

CREATE TABLE movies (
    movie_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    title VARCHAR(200) NOT NULL,
    duration_minutes SMALLINT UNSIGNED NOT NULL,
    language_code CHAR(2) NOT NULL,
    certificate VARCHAR(10) NOT NULL,
    PRIMARY KEY (movie_id),
    CONSTRAINT chk_movies_duration CHECK (duration_minutes > 0)
) ENGINE = InnoDB;

CREATE TABLE shows (
    show_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    screen_id BIGINT UNSIGNED NOT NULL,
    movie_id BIGINT UNSIGNED NOT NULL,
    show_date DATE NOT NULL,
    starts_at DATETIME NOT NULL,
    ends_at DATETIME NOT NULL,
    base_price DECIMAL(10,2) NOT NULL,
    status ENUM('SCHEDULED', 'CANCELLED', 'COMPLETED') NOT NULL DEFAULT 'SCHEDULED',
    PRIMARY KEY (show_id),
    UNIQUE KEY uq_shows_screen_start (screen_id, starts_at),
    KEY ix_shows_screen_date_status (screen_id, show_date, status, starts_at),
    KEY ix_shows_movie_date (movie_id, show_date),
    CONSTRAINT fk_shows_screen FOREIGN KEY (screen_id) REFERENCES screens(screen_id),
    CONSTRAINT fk_shows_movie FOREIGN KEY (movie_id) REFERENCES movies(movie_id),
    CONSTRAINT chk_shows_time_order CHECK (ends_at > starts_at),
    CONSTRAINT chk_shows_price CHECK (base_price >= 0)
) ENGINE = InnoDB;

CREATE TABLE show_seats (
    show_id BIGINT UNSIGNED NOT NULL,
    seat_id BIGINT UNSIGNED NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    status ENUM('AVAILABLE', 'HELD', 'BOOKED', 'BLOCKED') NOT NULL DEFAULT 'AVAILABLE',
    hold_token CHAR(36) NULL,
    held_until DATETIME(6) NULL,
    version BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (show_id, seat_id),
    KEY ix_show_seats_available (show_id, status, held_until),
    KEY ix_show_seats_hold_token (hold_token),
    CONSTRAINT fk_show_seats_show FOREIGN KEY (show_id) REFERENCES shows(show_id),
    CONSTRAINT fk_show_seats_seat FOREIGN KEY (seat_id) REFERENCES seats(seat_id),
    CONSTRAINT chk_show_seats_price CHECK (price >= 0),
    CONSTRAINT chk_show_seats_hold_data CHECK (
        (status = 'HELD' AND hold_token IS NOT NULL AND held_until IS NOT NULL)
        OR status <> 'HELD'
    )
) ENGINE = InnoDB;

CREATE TABLE bookings (
    booking_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    booking_reference CHAR(12) NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    show_id BIGINT UNSIGNED NOT NULL,
    hold_token CHAR(36) NOT NULL,
    idempotency_key CHAR(36) NOT NULL,
    request_hash CHAR(64) NOT NULL,
    status ENUM('PENDING_PAYMENT', 'CONFIRMED', 'EXPIRED', 'CANCELLED') NOT NULL DEFAULT 'PENDING_PAYMENT',
    total_amount DECIMAL(10,2) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (booking_id),
    UNIQUE KEY uq_bookings_reference (booking_reference),
    UNIQUE KEY uq_bookings_hold_token (hold_token),
    UNIQUE KEY uq_bookings_idempotency (idempotency_key),
    KEY ix_bookings_user_created (user_id, created_at),
    KEY ix_bookings_show_status (show_id, status),
    CONSTRAINT fk_bookings_user FOREIGN KEY (user_id) REFERENCES app_users(user_id),
    CONSTRAINT fk_bookings_show FOREIGN KEY (show_id) REFERENCES shows(show_id),
    CONSTRAINT chk_bookings_total CHECK (total_amount >= 0)
) ENGINE = InnoDB;

CREATE TABLE booking_seats (
    booking_id BIGINT UNSIGNED NOT NULL,
    seat_id BIGINT UNSIGNED NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    PRIMARY KEY (booking_id, seat_id),
    CONSTRAINT fk_booking_seats_booking FOREIGN KEY (booking_id) REFERENCES bookings(booking_id),
    CONSTRAINT chk_booking_seats_price CHECK (price >= 0)
) ENGINE = InnoDB;

CREATE TABLE payments (
    payment_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    booking_id BIGINT UNSIGNED NOT NULL,
    provider_payment_id VARCHAR(120) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status ENUM('PENDING', 'SUCCEEDED', 'FAILED', 'REFUNDED') NOT NULL DEFAULT 'PENDING',
    provider_created_at DATETIME(6) NULL,
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (payment_id),
    UNIQUE KEY uq_payments_booking (booking_id),
    UNIQUE KEY uq_payments_provider_id (provider_payment_id),
    CONSTRAINT fk_payments_booking FOREIGN KEY (booking_id) REFERENCES bookings(booking_id),
    CONSTRAINT chk_payments_amount CHECK (amount >= 0)
) ENGINE = InnoDB;

CREATE TABLE payment_webhook_events (
    webhook_event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    provider_event_id VARCHAR(120) NOT NULL,
    provider_payment_id VARCHAR(120) NOT NULL,
    event_type VARCHAR(80) NOT NULL,
    payload JSON NOT NULL,
    received_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    processed_at TIMESTAMP(6) NULL,
    PRIMARY KEY (webhook_event_id),
    UNIQUE KEY uq_webhook_provider_event (provider_event_id),
    KEY ix_webhook_payment (provider_payment_id)
) ENGINE = InnoDB;

DELIMITER $$

CREATE PROCEDURE release_expired_holds()
BEGIN
    UPDATE show_seats
    SET status = 'AVAILABLE', hold_token = NULL, held_until = NULL, version = version + 1
    WHERE status = 'HELD' AND held_until <= UTC_TIMESTAMP(6);

    UPDATE bookings
    SET status = 'EXPIRED'
        WHERE status = 'PENDING_PAYMENT'
            AND expires_at <= UTC_TIMESTAMP(6)
            AND NOT EXISTS (
                    SELECT 1 FROM payments p
                    WHERE p.booking_id = bookings.booking_id AND p.status = 'SUCCEEDED'
            );
END$$

DELIMITER ;
