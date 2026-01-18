import logging
import sys
from datetime import datetime

from api_client import FootballAPIClient
from data_transformer import FootballDataTransformer
from data_loader import PostgresDataLoader

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
        loader = PostgresDataLoader()
        
        for competition_code in competitions:
            logger.info("\n" + "="*50)
            logger.info(f"PROCESSING: {competition_code}")
            logger.info("="*50)
            
            # EXTRACT
            logger.info(f"[{competition_code}] Extracting teams data")
            teams_raw = api_client.get_teams(competition_code)
            logger.info(f"[{competition_code}] Extracted {len(teams_raw.get('teams', []))} teams")
            
            logger.info(f"[{competition_code}] Extracting matches data")
            matches_raw = api_client.get_matches(competition_code)
            logger.info(f"[{competition_code}] Extracted {len(matches_raw.get('matches', []))} matches")
            
            logger.info(f"[{competition_code}] Extracting standings data")
            standings_raw = api_client.get_standings(competition_code)
            logger.info(f"[{competition_code}] Extracted standings data")
            
            logger.info(f"[{competition_code}] Extracting scorers data")
            scorers_raw = api_client.get_scorers(competition_code)
            logger.info(f"[{competition_code}] Extracted {len(scorers_raw.get('scorers', []))} scorers")
            
            # TRANSFORM 
            logger.info(f"[{competition_code}] Transforming data")
            
            teams_df = transformer.transform_teams(teams_raw)
            matches_df = transformer.transform_matches(matches_raw)
            standings_df = transformer.transform_standings(standings_raw)
            scorers_df = transformer.transform_scorers(scorers_raw)
            dates_df = transformer.create_date_dimension(matches_df)
            
            # LOAD 
            logger.info(f"[{competition_code}] Loading data to database")
            
            loader.load_dim_teams(teams_df)
            loader.load_dim_dates(dates_df)
            loader.load_fact_matches(matches_df)
            loader.load_standings(standings_df)
            loader.load_scorers(scorers_df)
            
            logger.info(f"[{competition_code}] ✅ Complete")
        
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