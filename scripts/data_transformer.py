import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


spark_temp = r'C:\Temp\spark_work'
if not os.path.exists(spark_temp):
    os.makedirs(spark_temp)
os.environ['SPARK_LOCAL_DIRS'] = spark_temp
os.environ['TEMP'] = spark_temp
os.environ['TMP'] = spark_temp


from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, to_timestamp, dayofmonth, month, year, 
    dayofweek, current_timestamp, lit, when, coalesce
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, TimestampType, DateType
)
import logging
import findspark
findspark.init()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FootballDataTransformer:
    
    def __init__(self):
        jdbc_jar_path = r'C:\Java\postgresql-42.7.4.jar'
        
        self.spark = SparkSession.builder \
            .appName("FootballDataPipeline") \
            .config("spark.jars", jdbc_jar_path) \
            .config("spark.driver.extraClassPath", jdbc_jar_path) \
            .config("spark.executor.extraClassPath", jdbc_jar_path) \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.driver.host", "localhost") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark session initialized successfully")
    
    def transform_teams(self, raw_data: dict) -> DataFrame:
        teams = raw_data.get('teams', [])
        
        rows = []
        for team in teams:
            rows.append({
                'team_id': team['id'],
                'team_name': team['name'],
                'short_name': team.get('shortName', ''),
                'tla': team.get('tla', ''),  # Three-letter acronym
                'country': team.get('area', {}).get('name', ''),
                'founded': team.get('founded'),
                'stadium': team.get('venue', ''),
                'club_colors': team.get('clubColors', ''),
                'website': team.get('website', '')
            })
        
        df = self.spark.createDataFrame(rows)
        df = df.withColumn('loaded_at', current_timestamp())
        
        logger.info(f"Transformed {df.count()} team records")
        return df
    
    def transform_matches(self, raw_data: dict) -> DataFrame:
        matches = raw_data.get('matches', [])
        competition = raw_data.get('competition', {})
        
        rows = []
        for match in matches:
            full_time = match.get('score', {}).get('fullTime', {})
            half_time = match.get('score', {}).get('halfTime', {})
            
            rows.append({
                'match_id': match['id'],
                'competition_id': competition.get('id'),
                'competition_name': competition.get('name', ''),
                'season_id': match.get('season', {}).get('id'),
                'matchday': match.get('matchday'),
                'stage': match.get('stage', ''),
                'utc_date': match.get('utcDate'),
                'status': match.get('status'),
                'home_team_id': match.get('homeTeam', {}).get('id'),
                'home_team_name': match.get('homeTeam', {}).get('name', ''),
                'away_team_id': match.get('awayTeam', {}).get('id'),
                'away_team_name': match.get('awayTeam', {}).get('name', ''),
                'home_score_fulltime': full_time.get('home'),
                'away_score_fulltime': full_time.get('away'),
                'home_score_halftime': half_time.get('home'),
                'away_score_halftime': half_time.get('away'),
                'winner': match.get('score', {}).get('winner'),
                'duration': match.get('score', {}).get('duration', 'REGULAR'),
                'referees': ', '.join([r.get('name', '') for r in match.get('referees', [])])
            })
        
        df = self.spark.createDataFrame(rows)
        
  
        df = df.withColumn('match_date', to_date(col('utc_date'))) \
               .withColumn('match_timestamp', to_timestamp(col('utc_date'))) \
               .withColumn('day', dayofmonth(col('match_date'))) \
               .withColumn('month', month(col('match_date'))) \
               .withColumn('year', year(col('match_date'))) \
               .withColumn('day_of_week', dayofweek(col('match_date'))) \
               .withColumn('loaded_at', current_timestamp())
        
        logger.info(f"{df.count()} match records")
        return df
    
    def transform_standings(self, raw_data: dict) -> DataFrame:
        competition = raw_data.get('competition', {})
        season = raw_data.get('season', {})
        standings = raw_data.get('standings', [])
        
        rows = []
        for standing_type in standings:
            for team in standing_type.get('table', []):
                rows.append({
                    'competition_id': competition.get('id', 0),
                    'competition_name': competition.get('name', ''),
                    'season_id': season.get('id', 0),
                    'season_start': season.get('startDate', ''),
                    'season_end': season.get('endDate', ''),
                    'standing_type': standing_type.get('type', 'TOTAL'),
                    'position': team.get('position', 0),
                    'team_id': team.get('team', {}).get('id', 0),
                    'team_name': team.get('team', {}).get('name', ''),
                    'played_games': team.get('playedGames', 0),
                    'won': team.get('won', 0),
                    'draw': team.get('draw', 0),
                    'lost': team.get('lost', 0),
                    'goals_for': team.get('goalsFor', 0),
                    'goals_against': team.get('goalsAgainst', 0),
                    'goal_difference': team.get('goalDifference', 0),
                    'points': team.get('points', 0),
                    'form': team.get('form') or ''
                })
        
        df = self.spark.createDataFrame(rows)
        df = df.withColumn('loaded_at', current_timestamp())
        
        logger.info(f"Transformed {df.count()} standing records")
        return df
    
    def create_date_dimension(self, matches_df: DataFrame) -> DataFrame:
        date_df = matches_df.select(
            col('match_date').alias('full_date'),
            col('day'),
            col('month'),
            col('year'),
            col('day_of_week'),
            col('matchday')
        ).distinct().filter(col('full_date').isNotNull())
        
        date_df = date_df.withColumn(
            'date_id',
            (col('year') * 10000 + col('month') * 100 + col('day')).cast(IntegerType())
        )
        
        logger.info(f"Created date dimension with {date_df.count()} records")
        return date_df
    
    def stop(self):
        self.spark.stop()
        logger.info("Spark session stopped")


    def transform_scorers(self, raw_data: dict) -> DataFrame:
        """Transform top scorers data"""
        competition = raw_data.get('competition', {})
        season = raw_data.get('season', {})
        scorers = raw_data.get('scorers', [])
        
        rows = []
        for scorer in scorers:
            player = scorer.get('player', {})
            team = scorer.get('team', {})
            
            rows.append({
                'competition_id': competition.get('id'),
                'competition_name': competition.get('name', ''),
                'season_id': season.get('id'),
                'player_id': player.get('id'),
                'player_name': player.get('name', ''),
                'nationality': player.get('nationality', ''),
                'team_id': team.get('id'),
                'team_name': team.get('name', ''),
                'goals': scorer.get('goals'),
                'assists': scorer.get('assists'),
                'penalties': scorer.get('penalties'),
                'played_matches': scorer.get('playedMatches')
            })
        
        df = self.spark.createDataFrame(rows)
        df = df.withColumn('loaded_at', current_timestamp())
        
        logger.info(f"{df.count()} scorer records")
        return df