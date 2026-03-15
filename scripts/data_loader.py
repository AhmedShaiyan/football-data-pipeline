import os
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date, current_date
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgresDataLoader:

    def __init__(self, spark: SparkSession = None):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = os.getenv('POSTGRES_PORT', '5432')
        self.database = os.getenv('POSTGRES_DB', 'football_db')
        self.user = os.getenv('POSTGRES_USER', 'postgres')
        self.password = os.getenv('POSTGRES_PASSWORD', '')
        self.sslmode = os.getenv('POSTGRES_SSLMODE', 'prefer')
        self.spark = spark

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

    def _read_table(self, table: str) -> DataFrame:
        """Read an existing table from Postgres via JDBC"""
        return self.spark.read.jdbc(
            url=self.jdbc_url,
            table=table,
            properties=self.connection_properties
        )

    def get_existing_ids(self, table: str, id_column: str):
        """Return a DataFrame of existing IDs, or None if table doesn't exist or is empty"""
        try:
            return self._read_table(table).select(id_column)
        except Exception as e:
            logger.warning(f"Could not read existing IDs from {table} (first run?): {e}")
            return None

    def filter_new_records(self, df: DataFrame, existing_ids_df: DataFrame, id_column: str) -> DataFrame:
        """Return only records whose id_column value is not already in the database"""
        if existing_ids_df is None:
            return df
        return df.join(existing_ids_df, on=id_column, how='left_anti')

    def snapshot_exists_today(self, table: str, competition_id: int) -> bool:
        """Check if today's snapshot is already loaded for this competition"""
        try:
            count = (
                self._read_table(table)
                .filter(
                    (col('competition_id') == competition_id) &
                    (to_date(col('loaded_at')) == current_date())
                )
                .count()
            )
            exists = count > 0
            if exists:
                logger.info(f"Snapshot already exists in {table} for competition_id={competition_id} today — skipping")
            return exists
        except Exception as e:
            logger.warning(f"Could not check snapshot in {table} (first run?): {e}")
            return False

    def load_dataframe(self, df: DataFrame, table_name: str, mode: str = "append") -> None:
        try:
            count = df.count()
            if count == 0:
                logger.info(f"No new records to load to {table_name}")
                return
            logger.info(f"Loading {count} rows to table: {table_name}")

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
        existing = self.get_existing_ids("dim_teams", "team_id")
        new_teams = self.filter_new_records(df, existing, "team_id")
        self.load_dataframe(new_teams, "dim_teams", mode="append")

    def load_dim_dates(self, df: DataFrame) -> None:
        existing = self.get_existing_ids("dim_dates", "date_id")
        new_dates = self.filter_new_records(df, existing, "date_id")
        self.load_dataframe(new_dates, "dim_dates", mode="append")

    def load_fact_matches(self, df: DataFrame) -> None:
        existing = self.get_existing_ids("fact_matches", "match_id")
        new_matches = self.filter_new_records(df, existing, "match_id")
        self.load_dataframe(new_matches, "fact_matches", mode="append")

    def load_standings(self, df: DataFrame) -> None:
        self.load_dataframe(df, "standings_snapshot", mode="append")

    def load_scorers(self, df: DataFrame) -> None:
        self.load_dataframe(df, "dim_scorers", mode="append")
