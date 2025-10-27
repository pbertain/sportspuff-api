-- Initialize sports data database
-- This script runs when the PostgreSQL container starts for the first time

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create games table
CREATE TABLE IF NOT EXISTS games (
    id SERIAL PRIMARY KEY,
    league VARCHAR(10) NOT NULL,
    game_id VARCHAR(50) NOT NULL,
    game_date DATE NOT NULL,
    game_time TIMESTAMP WITH TIME ZONE,
    game_type VARCHAR(20) NOT NULL,  -- preseason, regular, playoffs, allstar, nba_cup, etc.
    
    -- Home team
    home_team VARCHAR(100) NOT NULL,
    home_team_abbrev VARCHAR(10) NOT NULL,
    home_team_id VARCHAR(20),
    home_wins INTEGER,
    home_losses INTEGER,
    home_score_total INTEGER,
    
    -- Visitor team
    visitor_team VARCHAR(100) NOT NULL,
    visitor_team_abbrev VARCHAR(10) NOT NULL,
    visitor_team_id VARCHAR(20),
    visitor_wins INTEGER,
    visitor_losses INTEGER,
    visitor_score_total INTEGER,
    
    -- Game state
    game_status VARCHAR(20) NOT NULL,  -- scheduled, in_progress, final, postponed
    current_period VARCHAR(20),
    time_remaining VARCHAR(20),
    is_final BOOLEAN DEFAULT FALSE,
    is_overtime BOOLEAN DEFAULT FALSE,
    
    -- Sport-specific scoring (JSON for flexibility)
    home_period_scores JSONB,  -- {"q1":25, "q2":30, ...} or {"1":2, "2":1, ...}
    visitor_period_scores JSONB,
    
    -- MLB specific
    home_hits INTEGER,
    home_runs INTEGER,
    home_errors INTEGER,
    visitor_hits INTEGER,
    visitor_runs INTEGER,
    visitor_errors INTEGER,
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(league, game_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_games_league_date ON games(league, game_date);
CREATE INDEX IF NOT EXISTS idx_games_status ON games(game_status);
CREATE INDEX IF NOT EXISTS idx_games_league_status ON games(league, game_status);
CREATE INDEX IF NOT EXISTS idx_games_game_date ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_league_game_date ON games(league, game_date);

-- Create API usage tracking table
CREATE TABLE IF NOT EXISTS api_usage (
    id SERIAL PRIMARY KEY,
    league VARCHAR(10) NOT NULL,
    endpoint VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP DEFAULT NOW(),
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT,
    response_time_ms INTEGER
);

-- Create indexes for API usage
CREATE INDEX IF NOT EXISTS idx_api_usage_league ON api_usage(league);
CREATE INDEX IF NOT EXISTS idx_api_usage_timestamp ON api_usage(timestamp);
CREATE INDEX IF NOT EXISTS idx_api_usage_success ON api_usage(success);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_games_updated_at 
    BEFORE UPDATE ON games 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();
