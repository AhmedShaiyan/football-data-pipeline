import pytest
from pyspark.sql import SparkSession
from scripts.data_transformer import FootballDataTransformer


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .appName("TestFootballPipeline")
        .master("local[1]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def transformer(spark):
    # Bypass __init__ to avoid JDBC jar / findspark setup; inject the test session directly
    t = FootballDataTransformer.__new__(FootballDataTransformer)
    t.spark = spark
    return t



# Sample raw API responses

@pytest.fixture
def raw_teams():
    return {
        "teams": [
            {
                "id": 57,
                "name": "Arsenal FC",
                "shortName": "Arsenal",
                "tla": "ARS",
                "area": {"name": "England"},
                "founded": 1886,
                "venue": "Emirates Stadium",
                "clubColors": "Red / White",
                "website": "http://www.arsenal.com",
            },
            {
                "id": 61,
                "name": "Chelsea FC",
                "shortName": "Chelsea",
                "tla": "CHE",
                "area": {"name": "England"},
                "founded": 1905,
                "venue": "Stamford Bridge",
                "clubColors": "Blue",
                "website": "http://www.chelseafc.com",
            },
        ]
    }


@pytest.fixture
def raw_teams_missing_optional():
    """Team that omits every optional field to test default values."""
    return {
        "teams": [
            {"id": 99, "name": "Unknown FC"}
        ]
    }


@pytest.fixture
def raw_matches():
    return {
        "competition": {"id": 2021, "name": "Premier League"},
        "matches": [
            {
                "id": 417406,
                "season": {"id": 1564},
                "matchday": 1,
                "stage": "REGULAR_SEASON",
                "utcDate": "2023-08-11T19:00:00Z",
                "status": "FINISHED",
                "homeTeam": {"id": 57, "name": "Arsenal FC"},
                "awayTeam": {"id": 61, "name": "Chelsea FC"},
                "score": {
                    "fullTime": {"home": 2, "away": 1},
                    "halfTime": {"home": 1, "away": 0},
                    "winner": "HOME_TEAM",
                    "duration": "REGULAR",
                },
                "referees": [{"name": "Michael Oliver"}],
            },
            {
                "id": 417407,
                "season": {"id": 1564},
                "matchday": 1,
                "stage": "REGULAR_SEASON",
                "utcDate": "2023-08-12T14:00:00Z",
                "status": "FINISHED",
                "homeTeam": {"id": 65, "name": "Manchester City"},
                "awayTeam": {"id": 73, "name": "Newcastle United"},
                "score": {
                    "fullTime": {"home": 0, "away": 0},
                    "halfTime": {"home": 0, "away": 0},
                    "winner": "DRAW",
                    "duration": "REGULAR",
                },
                "referees": [],
            },
        ],
    }


@pytest.fixture
def raw_standings():
    return {
        "competition": {"id": 2021, "name": "Premier League"},
        "season": {"id": 1564, "startDate": "2023-08-11", "endDate": "2024-05-19"},
        "standings": [
            {
                "type": "TOTAL",
                "table": [
                    {
                        "position": 1,
                        "team": {"id": 57, "name": "Arsenal FC"},
                        "playedGames": 10,
                        "won": 7,
                        "draw": 2,
                        "lost": 1,
                        "goalsFor": 20,
                        "goalsAgainst": 8,
                        "goalDifference": 12,
                        "points": 23,
                        "form": "W,W,D,W,L",
                    },
                    {
                        "position": 2,
                        "team": {"id": 61, "name": "Chelsea FC"},
                        "playedGames": 10,
                        "won": 5,
                        "draw": 3,
                        "lost": 2,
                        "goalsFor": 15,
                        "goalsAgainst": 10,
                        "goalDifference": 5,
                        "points": 18,
                        "form": None,  # deliberately None to test the `or ''` logic
                    },
                ],
            }
        ],
    }


@pytest.fixture
def raw_scorers():
    return {
        "competition": {"id": 2021, "name": "Premier League"},
        "season": {"id": 1564},
        "scorers": [
            {
                "player": {"id": 7839, "name": "Erling Haaland", "nationality": "Norway"},
                "team": {"id": 65, "name": "Manchester City"},
                "goals": 18,
                "assists": 4,
                "penalties": 2,
                "playedMatches": 20,
            },
            {
                "player": {"id": 3476, "name": "Mohamed Salah", "nationality": "Egypt"},
                "team": {"id": 64, "name": "Liverpool FC"},
                "goals": 15,
                "assists": 8,
                "penalties": 1,
                "playedMatches": 22,
            },
        ],
    }
