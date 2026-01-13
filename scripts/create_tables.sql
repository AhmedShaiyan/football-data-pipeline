-- Dimension: Teams
CREATE TABLE IF NOT EXISTS dim_teams (
    team_id INTEGER PRIMARY KEY,
    team_name VARCHAR(100),
    short_name VARCHAR(50),
    tla VARCHAR(10),
    country VARCHAR(50),
    founded INTEGER,
    stadium VARCHAR(100),
    club_colors VARCHAR(50),
    website VARCHAR(200),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Dates
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id INTEGER PRIMARY KEY,
    full_date DATE,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    day_of_week INTEGER,
    matchday INTEGER
);

-- Fact: Matches
CREATE TABLE IF NOT EXISTS fact_matches (
    match_id INTEGER PRIMARY KEY,
    competition_id INTEGER,
    competition_name VARCHAR(100),
    season_id INTEGER,
    matchday INTEGER,
    stage VARCHAR(50),
    utc_date VARCHAR(50),
    match_date DATE,
    match_timestamp TIMESTAMP,
    status VARCHAR(20),
    home_team_id INTEGER,
    home_team_name VARCHAR(100),
    away_team_id INTEGER,
    away_team_name VARCHAR(100),
    home_score_fulltime INTEGER,
    away_score_fulltime INTEGER,
    home_score_halftime INTEGER,
    away_score_halftime INTEGER,
    winner VARCHAR(20),
    duration VARCHAR(20),
    referees VARCHAR(200),
    day INTEGER,
    month INTEGER,
    year INTEGER,
    day_of_week INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Standings
CREATE TABLE IF NOT EXISTS standings_snapshot (
    id SERIAL PRIMARY KEY,
    competition_id INTEGER,
    competition_name VARCHAR(100),
    season_id INTEGER,
    season_start VARCHAR(20),
    season_end VARCHAR(20),
    standing_type VARCHAR(20),
    position INTEGER,
    team_id INTEGER,
    team_name VARCHAR(100),
    played_games INTEGER,
    won INTEGER,
    draw INTEGER,
    lost INTEGER,
    goals_for INTEGER,
    goals_against INTEGER,
    goal_difference INTEGER,
    points INTEGER,
    form VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_matches_date ON fact_matches(match_date);
CREATE INDEX IF NOT EXISTS idx_matches_home_team ON fact_matches(home_team_id);
CREATE INDEX IF NOT EXISTS idx_matches_away_team ON fact_matches(away_team_id);
CREATE INDEX IF NOT EXISTS idx_standings_team ON standings_snapshot(team_id);