-- ============================================
-- Football Prediction MVP - Database Schema
-- ============================================

CREATE TABLE IF NOT EXISTS teams (
    id SERIAL PRIMARY KEY,
    api_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    logo VARCHAR(500),
    country VARCHAR(100),
    league_id INTEGER,
    league_name VARCHAR(255),
    season INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS players (
    id SERIAL PRIMARY KEY,
    api_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    firstname VARCHAR(255),
    lastname VARCHAR(255),
    age INTEGER,
    nationality VARCHAR(100),
    height VARCHAR(20),
    weight VARCHAR(20),
    photo VARCHAR(500),
    team_id INTEGER REFERENCES teams(id) ON DELETE SET NULL,
    position VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS player_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    team_id INTEGER REFERENCES teams(id) ON DELETE SET NULL,
    league_id INTEGER,
    season INTEGER,
    appearances INTEGER DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,
    -- Goals
    goals_total INTEGER DEFAULT 0,
    goals_assists INTEGER DEFAULT 0,
    goals_saves INTEGER DEFAULT 0,
    -- Shots
    shots_total INTEGER DEFAULT 0,
    shots_on_target INTEGER DEFAULT 0,
    -- Passes
    passes_total INTEGER DEFAULT 0,
    passes_key INTEGER DEFAULT 0,
    passes_accuracy VARCHAR(10),
    -- Tackles / Duels
    tackles_total INTEGER DEFAULT 0,
    duels_total INTEGER DEFAULT 0,
    duels_won INTEGER DEFAULT 0,
    -- Dribbles
    dribbles_attempts INTEGER DEFAULT 0,
    dribbles_success INTEGER DEFAULT 0,
    -- Fouls
    fouls_committed INTEGER DEFAULT 0,
    fouls_drawn INTEGER DEFAULT 0,
    -- Cards
    cards_yellow INTEGER DEFAULT 0,
    cards_red INTEGER DEFAULT 0,
    cards_yellowred INTEGER DEFAULT 0,
    -- Penalty
    penalty_scored INTEGER DEFAULT 0,
    penalty_missed INTEGER DEFAULT 0,
    penalty_won INTEGER DEFAULT 0,
    penalty_committed INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(player_id, league_id, season)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_players_team_id ON players(team_id);
CREATE INDEX IF NOT EXISTS idx_players_api_id ON players(api_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_player_id ON player_stats(player_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_season ON player_stats(season);
CREATE INDEX IF NOT EXISTS idx_teams_api_id ON teams(api_id);
