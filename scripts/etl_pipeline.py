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


def run_etl_pipeline(competition_code: str = "PL") -> bool:
    logger.info(f"Starting Football Data ETL Pipeline")
    logger.info(f"Competition: {competition_code}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    # Initialize components
    api_client = None
    transformer = None
    loader = None
    
    try:
        logger.info("Initializing pipeline components")
        api_client = FootballAPIClient()
        transformer = FootballDataTransformer()
        loader = PostgresDataLoader()
        
        logger.info("\n" + "="*40)
        
        logger.info("Extracting teams data")
        teams_raw = api_client.get_teams(competition_code)
        logger.info(f"Extracted {len(teams_raw.get('teams', []))} teams")
        
        logger.info("Extracting matches data...")
        matches_raw = api_client.get_matches(competition_code)
        logger.info(f"Extracted {len(matches_raw.get('matches', []))} matches")
        
        logger.info("Extracting standings data")
        standings_raw = api_client.get_standings(competition_code)
        logger.info("Extracted standings data")
        
        logger.info("\n" + "="*40)
        logger.info("PHASE 2: TRANSFORM")
        logger.info("="*40)
        
        logger.info("Transforming teams data")
        teams_df = transformer.transform_teams(teams_raw)
        
        logger.info("Transforming matches data")
        matches_df = transformer.transform_matches(matches_raw)
        
        logger.info("Transforming standings data...")
        standings_df = transformer.transform_standings(standings_raw)
        
        logger.info("Creating date dimension...")
        dates_df = transformer.create_date_dimension(matches_df)
        
        # Load
        logger.info("\n" + "="*40)
        logger.info("PHASE 3: LOAD")
        logger.info("="*40)
        
        logger.info("Loading teams dimension...")
        loader.load_dim_teams(teams_df)
        
        logger.info("Loading dates dimension...")
        loader.load_dim_dates(dates_df)
        
        logger.info("Loading matches fact table...")
        loader.load_fact_matches(matches_df)
        
        logger.info("Loading standings snapshot...")
        loader.load_standings(standings_df)
        
        # Completion 
        logger.info("ETL PIPELINE COMPLETED ")

        
        return True
        
    except Exception as e:
        logger.error(f"ETL Pipeline failed: {e}")
        logger.exception("Full traceback:")
        return False
        
    finally:
        # Clean up
        if transformer:
            transformer.stop()
        logger.info("Pipeline cleanup complete")


if __name__ == "__main__":    
    import os
    os.makedirs('logs', exist_ok=True)
    
    success = run_etl_pipeline("PL")
    sys.exit(0 if success else 1)