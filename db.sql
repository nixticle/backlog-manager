PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    platform TEXT,
    year INTEGER,
    status TEXT,
    rating REAL,
    title_norm TEXT NOT NULL,
    platform_norm TEXT,
    platform_family TEXT,
    source_id TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS hltb_results (
    id INTEGER PRIMARY KEY,
    query_key TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    platforms TEXT,
    year INTEGER,
    main REAL,
    main_extra REAL,
    complete REAL,
    votes INTEGER,
    raw_json TEXT NOT NULL,
    fetched_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS matches (
    game_id INTEGER PRIMARY KEY,
    hltb_id INTEGER NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
    method TEXT NOT NULL,
    decided_by TEXT NOT NULL,
    matched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
    FOREIGN KEY (hltb_id) REFERENCES hltb_results(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS review_queue (
    game_id INTEGER PRIMARY KEY,
    candidates_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS etl_runs (
    id INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    stats_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_games_title_norm ON games(title_norm);
CREATE INDEX IF NOT EXISTS idx_games_platform_family ON games(platform_family);

CREATE UNIQUE INDEX IF NOT EXISTS uidx_games_full
ON games(title_norm, platform_family, year)
WHERE platform_family IS NOT NULL AND year IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_games_platform
ON games(title_norm, platform_family)
WHERE platform_family IS NOT NULL AND year IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_games_year
ON games(title_norm, year)
WHERE platform_family IS NULL AND year IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_games_title
ON games(title_norm)
WHERE platform_family IS NULL AND year IS NULL;
CREATE INDEX IF NOT EXISTS idx_hltb_query_key ON hltb_results(query_key);

CREATE TRIGGER IF NOT EXISTS trg_games_updated
AFTER UPDATE ON games
FOR EACH ROW
WHEN NEW.updated_at <= OLD.updated_at
BEGIN
    UPDATE games SET updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now') WHERE id = NEW.id;
END;
