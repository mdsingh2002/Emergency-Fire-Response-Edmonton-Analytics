"""
Load Module - Fire Incident ETL Pipeline
Handles loading data into PostgreSQL database
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine, text
from pathlib import Path
from typing import Optional
from loguru import logger
import sys
from datetime import datetime
from tqdm import tqdm

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.config import DB_CONFIG, DB_CONNECTION_STRING, ETL_CONFIG, LOGS_DIR, LOG_CONFIG, SQL_DIR

# Configure logger
logger.add(
    LOGS_DIR / "load.log",
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    level=LOG_CONFIG["level"],
    format=LOG_CONFIG["format"],
)


class DataLoader:
    """
    Loads fire incident data into PostgreSQL database
    """

    def __init__(self):
        """Initialize the DataLoader"""
        self.engine = None
        self.connection = None
        logger.info("Initialized DataLoader")

    def create_engine(self):
        """Create SQLAlchemy engine for database connection"""
        try:
            self.engine = create_engine(DB_CONNECTION_STRING)
            logger.info("Database engine created successfully")
            return self.engine
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise

    def test_connection(self) -> bool:
        """
        Test database connection

        Returns:
            True if connection successful, False otherwise
        """
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Database connection successful: {version[0]}")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def execute_schema(self, schema_file: Optional[Path] = None):
        """
        Execute SQL schema file to create tables

        Args:
            schema_file: Path to schema.sql file. Uses default if not provided.
        """
        schema_path = schema_file or SQL_DIR / "schema.sql"

        if not schema_path.exists():
            logger.error(f"Schema file not found: {schema_path}")
            raise FileNotFoundError(f"Schema file not found: {schema_path}")

        logger.info(f"Executing schema from: {schema_path}")

        try:
            # Read schema file
            with open(schema_path, "r") as f:
                schema_sql = f.read()

            # Execute schema
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(schema_sql)
            conn.commit()

            logger.info("Schema executed successfully")
            logger.info("Tables created: dim_event_types, dim_response_codes, dim_neighbourhoods, fire_incidents")

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"Error executing schema: {e}")
            raise

    def populate_dimension_tables(self, df: pd.DataFrame):
        """
        Populate dimension tables from the main DataFrame

        Args:
            df: DataFrame containing the data
        """
        logger.info("Populating dimension tables...")

        try:
            if not self.engine:
                self.create_engine()

            # Populate dim_event_types
            # Drop duplicates by event_type_group only (keep first occurrence)
            event_types = df[["event_type_group", "event_description"]].dropna(subset=["event_type_group"]).drop_duplicates(subset=["event_type_group"])
            event_types.columns = ["event_type_code", "event_description"]
            # Fill null descriptions with 'UNKNOWN'
            event_types["event_description"] = event_types["event_description"].fillna("UNKNOWN")

            if len(event_types) > 0:
                event_types.to_sql(
                    "dim_event_types",
                    self.engine,
                    if_exists="append",
                    index=False,
                    method="multi"
                )
                logger.info(f"Inserted {len(event_types)} records into dim_event_types")

            # Populate dim_response_codes
            response_codes = df[["response_code"]].dropna().drop_duplicates()
            response_codes["response_description"] = None
            response_codes.columns = ["response_code", "response_description"]

            if len(response_codes) > 0:
                response_codes.to_sql(
                    "dim_response_codes",
                    self.engine,
                    if_exists="append",
                    index=False,
                    method="multi"
                )
                logger.info(f"Inserted {len(response_codes)} records into dim_response_codes")

            # Populate dim_neighbourhoods
            # Drop duplicates by neighbourhood_id only (keep first occurrence)
            neighbourhoods = df[["neighbourhood_id", "neighbourhood_name"]].dropna(subset=["neighbourhood_id"]).drop_duplicates(subset=["neighbourhood_id"])

            if len(neighbourhoods) > 0:
                neighbourhoods.to_sql(
                    "dim_neighbourhoods",
                    self.engine,
                    if_exists="append",
                    index=False,
                    method="multi"
                )
                logger.info(f"Inserted {len(neighbourhoods)} records into dim_neighbourhoods")

        except Exception as e:
            logger.error(f"Error populating dimension tables: {e}")
            raise

    def load_data_batch(
        self,
        df: pd.DataFrame,
        table_name: str = "fire_incidents",
        batch_size: Optional[int] = None
    ):
        """
        Load data into PostgreSQL in batches

        Args:
            df: DataFrame to load
            table_name: Target table name
            batch_size: Number of rows per batch. Uses config default if not provided.
        """
        batch_size = batch_size or ETL_CONFIG["batch_size"]

        logger.info(f"Loading {len(df):,} records into {table_name} in batches of {batch_size:,}")

        try:
            if not self.engine:
                self.create_engine()

            # Calculate number of batches
            num_batches = (len(df) + batch_size - 1) // batch_size

            # Load data in batches with progress bar
            for i in tqdm(range(0, len(df), batch_size), desc="Loading batches", unit="batch"):
                batch = df.iloc[i:i + batch_size]

                batch.to_sql(
                    table_name,
                    self.engine,
                    if_exists="append",
                    index=False,
                    method="multi"
                )

                logger.debug(f"Loaded batch {i // batch_size + 1}/{num_batches}")

            logger.info(f"Successfully loaded {len(df):,} records into {table_name}")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def load_data_fast(
        self,
        df: pd.DataFrame,
        table_name: str = "fire_incidents"
    ):
        """
        Fast bulk load using COPY command (faster than batch insert)

        Args:
            df: DataFrame to load
            table_name: Target table name
        """
        logger.info(f"Fast loading {len(df):,} records into {table_name} using COPY")

        try:
            if not self.engine:
                self.create_engine()

            # Use pandas to_sql with method='multi' which is optimized
            df.to_sql(
                table_name,
                self.engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10000
            )

            logger.info(f"Successfully loaded {len(df):,} records into {table_name}")

        except Exception as e:
            logger.error(f"Error during fast load: {e}")
            raise

    def update_foreign_keys(self):
        """
        Update foreign key references in fire_incidents table
        """
        logger.info("Updating foreign key references...")

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Update event_type_id
            cursor.execute("""
                UPDATE fire_incidents fi
                SET event_type_id = et.event_type_id
                FROM dim_event_types et
                WHERE fi.event_type_group = et.event_type_code
                AND fi.event_type_id IS NULL
            """)
            logger.info(f"Updated event_type_id for {cursor.rowcount} records")

            # Update response_code_id
            cursor.execute("""
                UPDATE fire_incidents fi
                SET response_code_id = rc.response_code_id
                FROM dim_response_codes rc
                WHERE fi.response_code = rc.response_code
                AND fi.response_code_id IS NULL
            """)
            logger.info(f"Updated response_code_id for {cursor.rowcount} records")

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("Foreign key references updated successfully")

        except Exception as e:
            logger.error(f"Error updating foreign keys: {e}")
            raise

    def get_table_stats(self, table_name: str) -> dict:
        """
        Get statistics about a table

        Args:
            table_name: Name of the table

        Returns:
            Dictionary containing table statistics
        """
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]

            # Get table size
            cursor.execute(f"""
                SELECT pg_size_pretty(pg_total_relation_size('{table_name}'))
            """)
            table_size = cursor.fetchone()[0]

            cursor.close()
            conn.close()

            stats = {
                "table_name": table_name,
                "row_count": row_count,
                "table_size": table_size
            }

            logger.info(f"Table {table_name}: {row_count:,} rows, {table_size}")

            return stats

        except Exception as e:
            logger.error(f"Error getting table stats: {e}")
            return {"error": str(e)}

    def verify_load(self):
        """
        Verify that data was loaded correctly
        """
        logger.info("Verifying data load...")

        try:
            # Get stats for all tables
            tables = ["fire_incidents", "dim_event_types", "dim_response_codes", "dim_neighbourhoods"]

            for table in tables:
                stats = self.get_table_stats(table)
                logger.info(f"  {stats['table_name']}: {stats.get('row_count', 0):,} rows")

            logger.info("Data load verification complete")

        except Exception as e:
            logger.error(f"Error during verification: {e}")

    def truncate_tables(self):
        """
        Truncate all tables (use with caution!)
        """
        logger.warning("Truncating all tables...")

        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            cursor.execute("TRUNCATE TABLE fire_incidents CASCADE")
            cursor.execute("TRUNCATE TABLE dim_event_types CASCADE")
            cursor.execute("TRUNCATE TABLE dim_response_codes CASCADE")
            cursor.execute("TRUNCATE TABLE dim_neighbourhoods CASCADE")

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("All tables truncated")

        except Exception as e:
            logger.error(f"Error truncating tables: {e}")
            raise


def main():
    """
    Main function for testing the load module
    """
    logger.info("=" * 80)
    logger.info("Starting Load Module Test")
    logger.info("=" * 80)

    loader = DataLoader()

    # Test connection
    logger.info("\n1. Testing database connection...")
    if loader.test_connection():
        logger.info("✓ Connection successful")
    else:
        logger.error("✗ Connection failed")
        return

    # Execute schema
    logger.info("\n2. Executing schema...")
    try:
        loader.execute_schema()
        logger.info("✓ Schema executed")
    except Exception as e:
        logger.error(f"✗ Schema execution failed: {e}")
        return

    # Test with small sample data
    logger.info("\n3. Testing with sample data...")

    from extract import DataExtractor
    from transform import DataTransformer

    # Extract and transform data (first 1000 rows for testing)
    extractor = DataExtractor()
    df = extractor.extract_data()
    df_sample = df.head(1000).copy()

    transformer = DataTransformer()
    df_transformed = transformer.transform(df_sample)
    df_db_ready = transformer.prepare_for_database(df_transformed)

    # Populate dimension tables
    logger.info("\n4. Populating dimension tables...")
    loader.populate_dimension_tables(df_db_ready)

    # Load data
    logger.info("\n5. Loading fact table...")
    loader.load_data_batch(df_db_ready, batch_size=500)

    # Update foreign keys
    logger.info("\n6. Updating foreign keys...")
    loader.update_foreign_keys()

    # Verify load
    logger.info("\n7. Verifying load...")
    loader.verify_load()

    logger.info("=" * 80)
    logger.info("Load Module Test Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
