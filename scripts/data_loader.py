import os
import logging
from pyspark.sql import DataFrame
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresDataLoader:
    
    
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = os.getenv('POSTGRES_PORT', '5432')
        self.database = os.getenv('POSTGRES_DB', 'football_db')
        self.user = os.getenv('POSTGRES_USER', 'postgres')
        self.password = os.getenv('POSTGRES_PASSWORD', '')
        self.sslmode = os.getenv('POSTGRES_SSLMODE', 'prefer')
        
        self.jdbc_url = (
            f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
            f"?sslmode={self.sslmode}"
        )
        self.connection_properties = {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }
        
        logger.info(f"PostgresDataLoader initialized for {self.host}:{self.port}/{self.database}")
    
    def load_dataframe(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        try:
            logger.info(f"Loading {df.count()} rows to table: {table_name}")
            
            df.write.jdbc(
                url=self.jdbc_url,
                table=table_name,
                mode=mode,
                properties=self.connection_properties
            )
            
            logger.info(f"Successfully loaded data to {table_name}")
        except Exception as e:
            logger.error(f"Failed to load data to {table_name}: {e}")
            raise
    
    def load_dim_teams(self, df: DataFrame) -> None:
        self.load_dataframe(df, "dim_teams", mode="append")
    
    def load_dim_dates(self, df: DataFrame) -> None:
        # append to accumulate dates over time
        self.load_dataframe(df, "dim_dates", mode="append")
    
    def load_fact_matches(self, df: DataFrame) -> None:
        self.load_dataframe(df, "fact_matches", mode="append")
    
    def load_standings(self, df: DataFrame) -> None:
        self.load_dataframe(df, "standings_snapshot", mode="append")


    def load_scorers(self, df: DataFrame) -> None:
        self.load_dataframe(df, "dim_scorers", mode="append")