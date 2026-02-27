-- Reactor database setup for Dolt
--
-- Run this against your Dolt server to create the required tables.
-- Reactor stores classified events and timer state here.

CREATE DATABASE IF NOT EXISTS reactor;
USE reactor;

CREATE TABLE IF NOT EXISTS reactor_events (
    id VARCHAR(26) PRIMARY KEY,       -- ULID (time-sortable)
    timestamp DATETIME(3) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    source_db VARCHAR(50),
    source_table VARCHAR(50),
    subject_id VARCHAR(50),
    actor VARCHAR(100),
    summary TEXT,
    payload JSON,
    INDEX idx_timestamp (timestamp),
    INDEX idx_event_type (event_type),
    INDEX idx_subject_id (subject_id)
);

CREATE TABLE IF NOT EXISTS reactor_timers (
    id VARCHAR(26) PRIMARY KEY,
    subject_id VARCHAR(50) NOT NULL,
    timer_type VARCHAR(50) NOT NULL,
    fire_at DATETIME NOT NULL,
    reaction JSON NOT NULL,
    created_at DATETIME NOT NULL,
    INDEX idx_fire_at (fire_at)
);

CREATE TABLE IF NOT EXISTS reactor_state (
    key_name VARCHAR(50) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
