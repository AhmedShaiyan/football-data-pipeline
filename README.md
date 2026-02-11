# Multi league Football Analytics Platform
Interactive dashboard delivering football stats and analysis. Use it to track team performance, analyze scoring trends, and compare statistics across Premier League, La Liga, and Bundesliga.

**Key Features:**
- Live league standings 
- Top scorers leaderboard with goals, assists, and penalties
- Home vs away performance analysis
- Head-to-head team comparisons
- Recent form tracking (last 5 matches)
- League filtering
  
The dashboard is the downstream product of an End-to-end ETL pipeline extracting data from football-data.org API.

## Pipeline Architecture
```
API (football-data.org) → Extract (Python) → Transform (PySpark) → Load (PostgreSQL) → Visualize (Power BI)
```

**Tech Stack:**
- Python 3.x
- PySpark 3.5.0
- PostgreSQL (Neon Cloud)
- Power BI Desktop

## Data Model

**Dimension Tables:**
- `dim_teams` - Team attributes (name, stadium, colors, founded)
- `dim_dates` - Date dimension for time-based analysis

**Fact Tables:**
- `fact_matches` - Match results and scores
- `standings_snapshot` - League standings snapshots
- `dim_scorers` - Top scorers by competition

**Competitions Covered:**
- Premier League (England)
- La Liga (Spain)
- Bundesliga (Germany)


## Setup

1. **Clone and install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Configure environment variables (.env):**
```
FOOTBALL_API_KEY=your_api_key
POSTGRES_HOST=your_neon_host
POSTGRES_PORT=5432
POSTGRES_DB=football_db
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
POSTGRES_SSLMODE=require
```

3. **Initialize database:**
```bash
psql -h <host> -U <user> -d football_db -f scripts/create_tables.sql
```

4. **Update JDBC path in `data_transformer.py`:**
```python
jdbc_jar_path = r'C:\path\to\postgresql-42.7.4.jar'
```

## Usage

Run full pipeline for all competitions:
```bash
python scripts/etl_pipeline.py
```

Run for specific competitions:
```python
from etl_pipeline import run_etl_pipeline
run_etl_pipeline(["PL", "PD"])  # Premier League and La Liga only
```

## Power BI Connection

1. Install Npgsql driver (4.1.x)
2. Get Data → PostgreSQL → Enter Neon credentials
3. Use DirectQuery for real-time or Import for performance

## Future Enhancements

- Incremental loads (process only new matches since last run)
- Airflow scheduling for automated daily refreshes
- Additional leagues (Serie A, Ligue 1)
- Advanced metrics (xG, possession stats)

## Resources
Data provided by [football-data.org](https://www.football-data.org/)
