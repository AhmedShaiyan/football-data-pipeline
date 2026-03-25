import logging
import sys
from datetime import datetime
from functools import reduce

from api_client import FootballAPIClient
from data_transformer import FootballDataTransformer
from data_loader import PostgresDataLoader
from validators import validate_raw_response, validate_dataframe

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'logs/etl_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

COMPETITIONS = ["PL", "PD", "BL1"] # Premier League, La Liga, Bundesliga

def run_etl_pipeline(competitions: list = None) -> bool:
    """Run ETL pipeline for multiple competitions"""
    
    if competitions is None:
        competitions = COMPETITIONS
    
    logger.info(f"Starting Football Data ETL Pipeline")
    logger.info(f"Competitions: {competitions}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    api_client = None
    transformer = None
    loader = None
    
    try:
        logger.info("Initializing pipeline components")
        api_client = FootballAPIClient()
        transformer = FootballDataTransformer()
        loader = PostgresDataLoader(spark=transformer.spark)

        all_dates_dfs = []
        
        for competition_code in competitions:
            logger.info("\n" + "="*50)
            logger.info(f"PROCESSING: {competition_code}")
            logger.info("="*50)
            
            # EXTRACT
            logger.info(f"[{competition_code}] Extracting teams data")
            teams_raw = api_client.get_teams(competition_code)
            validate_raw_response(teams_raw, "teams", competition_code)

            logger.info(f"[{competition_code}] Extracting matches data")
            matches_raw = api_client.get_matches(competition_code)
            validate_raw_response(matches_raw, "matches", competition_code)

            logger.info(f"[{competition_code}] Extracting standings data")
            standings_raw = api_client.get_standings(competition_code)
            validate_raw_response(standings_raw, "standings", competition_code)

            logger.info(f"[{competition_code}] Extracting scorers data")
            scorers_raw = api_client.get_scorers(competition_code)
            validate_raw_response(scorers_raw, "scorers", competition_code)

            # TRANSFORM
            logger.info(f"[{competition_code}] Transforming data")

            teams_df = transformer.transform_teams(teams_raw)
            validate_dataframe(teams_df, "team_id", competition_code)

            matches_df = transformer.transform_matches(matches_raw)
            validate_dataframe(matches_df, "match_id", competition_code)

            standings_df = transformer.transform_standings(standings_raw)
            validate_dataframe(standings_df, "team_id", competition_code)

            scorers_df = transformer.transform_scorers(scorers_raw)
            validate_dataframe(scorers_df, "player_id", competition_code)

            dates_df = transformer.create_date_dimension(matches_df)

            all_dates_dfs.append(dates_df)
            
            # LOAD
            logger.info(f"[{competition_code}] Loading data to database")

            loader.load_dim_teams(teams_df)
            loader.load_fact_matches(matches_df)

            competition_id = standings_df.select('competition_id').first()[0]

            if loader.snapshot_exists_today("standings_snapshot", competition_id):
                logger.info(f"[{competition_code}] Standings snapshot already loaded today — skipping")
            else:
                loader.load_standings(standings_df)

            if loader.snapshot_exists_today("dim_scorers", competition_id):
                logger.info(f"[{competition_code}] Scorers snapshot already loaded today — skipping")
            else:
                loader.load_scorers(scorers_df)
            
            logger.info(f"[{competition_code}] Complete")

            # Load dates
            logger.info("Loading consolidated date dimension")
        
        # Union all date dataframes, deduplicate within this run, then filter against DB
        all_dates = reduce(lambda df1, df2: df1.union(df2), all_dates_dfs)
        unique_dates = all_dates.dropDuplicates(["date_id"])

        logger.info(f"Checking for new dates to load")
        loader.load_dim_dates(unique_dates)
        
        logger.info("\n" + "="*50)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*50)
        
        return True
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        logger.exception("Full traceback:")
        return False
        
    finally:
        if transformer:
            transformer.stop()
        logger.info("Pipeline cleanup complete")


if __name__ == "__main__":
    import os
    os.makedirs('logs', exist_ok=True)
    
    success = run_etl_pipeline(COMPETITIONS)
    sys.exit(0 if success else 1)