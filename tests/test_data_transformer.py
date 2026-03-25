import pytest



# transform_teams

EXPECTED_TEAM_COLUMNS = {
    "team_id", "team_name", "short_name", "tla", "country",
    "founded", "stadium", "club_colors", "website", "loaded_at",
}

def test_transform_teams_columns(transformer, raw_teams):
    df = transformer.transform_teams(raw_teams)
    assert set(df.columns) == EXPECTED_TEAM_COLUMNS

def test_transform_teams_row_count(transformer, raw_teams):
    df = transformer.transform_teams(raw_teams)
    assert df.count() == 2

def test_transform_teams_optional_fields_default_to_empty_string(transformer, raw_teams_missing_optional):
    df = transformer.transform_teams(raw_teams_missing_optional)
    row = df.collect()[0]
    assert row["short_name"] == ""
    assert row["tla"] == ""
    assert row["country"] == ""
    assert row["stadium"] == ""
    assert row["club_colors"] == ""
    assert row["website"] == ""



# transform_matches

EXPECTED_MATCH_COLUMNS = {
    "match_id", "competition_id", "competition_name", "season_id",
    "matchday", "stage", "utc_date", "status",
    "home_team_id", "home_team_name", "away_team_id", "away_team_name",
    "home_score_fulltime", "away_score_fulltime",
    "home_score_halftime", "away_score_halftime",
    "winner", "duration", "referees",
    "match_date", "match_timestamp", "day", "month", "year", "day_of_week",
    "loaded_at",
}

def test_transform_matches_columns(transformer, raw_matches):
    df = transformer.transform_matches(raw_matches)
    assert set(df.columns) == EXPECTED_MATCH_COLUMNS

def test_transform_matches_row_count(transformer, raw_matches):
    df = transformer.transform_matches(raw_matches)
    assert df.count() == 2

def test_transform_matches_computes_date_fields(transformer, raw_matches):
    df = transformer.transform_matches(raw_matches)
    row = df.filter(df.match_id == 417406).collect()[0]
    # utcDate = "2023-08-11T19:00:00Z"
    assert row["day"] == 11
    assert row["month"] == 8
    assert row["year"] == 2023

def test_transform_matches_computes_day_of_week(transformer, raw_matches):
    df = transformer.transform_matches(raw_matches)
    row = df.filter(df.match_id == 417406).collect()[0]
    # 2023-08-11 is a Friday; Spark dayofweek: 1=Sun ... 6=Fri
    assert row["day_of_week"] == 6

def test_transform_matches_referees_joined_as_string(transformer, raw_matches):
    df = transformer.transform_matches(raw_matches)
    row = df.filter(df.match_id == 417406).collect()[0]
    assert row["referees"] == "Michael Oliver"

def test_transform_matches_empty_referees_becomes_empty_string(transformer, raw_matches):
    df = transformer.transform_matches(raw_matches)
    row = df.filter(df.match_id == 417407).collect()[0]
    assert row["referees"] == ""



# transform_standings

EXPECTED_STANDING_COLUMNS = {
    "competition_id", "competition_name", "season_id", "season_start", "season_end",
    "standing_type", "position", "team_id", "team_name", "played_games",
    "won", "draw", "lost", "goals_for", "goals_against", "goal_difference",
    "points", "form", "loaded_at",
}

def test_transform_standings_columns(transformer, raw_standings):
    df = transformer.transform_standings(raw_standings)
    assert set(df.columns) == EXPECTED_STANDING_COLUMNS

def test_transform_standings_flattens_table_entries(transformer, raw_standings):
    df = transformer.transform_standings(raw_standings)
    assert df.count() == 2

def test_transform_standings_form_none_becomes_empty_string(transformer, raw_standings):
    df = transformer.transform_standings(raw_standings)
    row = df.filter(df.team_id == 61).collect()[0]
    assert row["form"] == ""



# create_date_dimension

def test_create_date_dimension_date_id_calculation(transformer, raw_matches):
    matches_df = transformer.transform_matches(raw_matches)
    dates_df = transformer.create_date_dimension(matches_df)
    row = dates_df.filter(dates_df.year == 2023).filter(dates_df.month == 8).filter(dates_df.day == 11).collect()[0]
    # date_id = year*10000 + month*100 + day = 20230811
    assert row["date_id"] == 20230811

def test_create_date_dimension_deduplicates(transformer, spark):
    # Two matches on the same date should produce only one date row
    raw = {
        "competition": {"id": 2021, "name": "Premier League"},
        "matches": [
            {
                "id": 1, "season": {"id": 1}, "matchday": 1, "stage": "REGULAR_SEASON",
                "utcDate": "2023-08-11T15:00:00Z", "status": "FINISHED",
                "homeTeam": {"id": 1, "name": "A"}, "awayTeam": {"id": 2, "name": "B"},
                "score": {"fullTime": {"home": 1, "away": 0}, "halfTime": {"home": 0, "away": 0},
                          "winner": "HOME_TEAM", "duration": "REGULAR"},
                "referees": [],
            },
            {
                "id": 2, "season": {"id": 1}, "matchday": 1, "stage": "REGULAR_SEASON",
                "utcDate": "2023-08-11T17:30:00Z", "status": "FINISHED",
                "homeTeam": {"id": 3, "name": "C"}, "awayTeam": {"id": 4, "name": "D"},
                "score": {"fullTime": {"home": 0, "away": 2}, "halfTime": {"home": 0, "away": 1},
                          "winner": "AWAY_TEAM", "duration": "REGULAR"},
                "referees": [],
            },
        ],
    }
    matches_df = transformer.transform_matches(raw)
    dates_df = transformer.create_date_dimension(matches_df)
    assert dates_df.count() == 1

def test_create_date_dimension_filters_null_dates(transformer, spark):
    # Match with no utcDate should not produce a date row
    raw = {
        "competition": {"id": 2021, "name": "Premier League"},
        "matches": [
            {
                "id": 3, "season": {"id": 1}, "matchday": 1, "stage": "REGULAR_SEASON",
                "utcDate": None, "status": "SCHEDULED",
                "homeTeam": {"id": 1, "name": "A"}, "awayTeam": {"id": 2, "name": "B"},
                "score": {"fullTime": {"home": None, "away": None}, "halfTime": {"home": None, "away": None},
                          "winner": None, "duration": "REGULAR"},
                "referees": [],
            },
        ],
    }
    matches_df = transformer.transform_matches(raw)
    dates_df = transformer.create_date_dimension(matches_df)
    assert dates_df.count() == 0



# transform_scorers

EXPECTED_SCORER_COLUMNS = {
    "competition_id", "competition_name", "season_id",
    "player_id", "player_name", "nationality",
    "team_id", "team_name",
    "goals", "assists", "penalties", "played_matches",
    "loaded_at",
}

def test_transform_scorers_columns(transformer, raw_scorers):
    df = transformer.transform_scorers(raw_scorers)
    assert set(df.columns) == EXPECTED_SCORER_COLUMNS

def test_transform_scorers_row_count(transformer, raw_scorers):
    df = transformer.transform_scorers(raw_scorers)
    assert df.count() == 2
