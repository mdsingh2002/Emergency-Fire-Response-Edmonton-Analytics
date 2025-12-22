"""
Main ETL Pipeline Orchestration
Fire Incident Data - Edmonton Analytics
"""
import sys
from pathlib import Path
from loguru import logger
from datetime import datetime
import argparse

# Add etl directory to path
sys.path.append(str(Path(__file__).parent))

from etl.extract import DataExtractor
from etl.validate import DataValidator
from etl.transform import DataTransformer
from etl.load import DataLoader
from config.config import LOGS_DIR, LOG_CONFIG, ETL_CONFIG

# Configure main logger
logger.remove()  # Remove default handler
logger.add(
    sys.stdout,
    level=LOG_CONFIG["level"],
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)
logger.add(
    LOGS_DIR / "main.log",
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    level=LOG_CONFIG["level"],
    format=LOG_CONFIG["format"],
)


class FireIncidentETL:
    """
    Main ETL Pipeline for Fire Incident Data
    """

    def __init__(self, skip_validation: bool = False, skip_schema: bool = False):
        """
        Initialize the ETL pipeline

        Args:
            skip_validation: If True, skip data validation step
            skip_schema: If True, skip schema creation (assumes tables already exist)
        """
        self.skip_validation = skip_validation
        self.skip_schema = skip_schema

        self.extractor = DataExtractor()
        self.validator = DataValidator()
        self.transformer = DataTransformer()
        self.loader = DataLoader()

        self.df_raw = None
        self.df_transformed = None
        self.df_db_ready = None
        self.validation_report = None

        logger.info("FireIncidentETL initialized")

    def run_extract(self) -> bool:
        """
        Extract data from CSV file

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("STEP 1: EXTRACT")
            logger.info("=" * 80)

            self.df_raw = self.extractor.extract_data()

            logger.info(f"✓ Extracted {len(self.df_raw):,} records")
            return True

        except Exception as e:
            logger.error(f"✗ Extract failed: {e}")
            return False

    def run_validate(self) -> bool:
        """
        Validate extracted data

        Returns:
            True if validation passes, False otherwise
        """
        if self.skip_validation:
            logger.info("Skipping validation (--skip-validation flag set)")
            return True

        try:
            logger.info("=" * 80)
            logger.info("STEP 2: VALIDATE")
            logger.info("=" * 80)

            self.validation_report = self.validator.generate_validation_report(self.df_raw)
            self.validator.print_summary()

            status = self.validation_report["summary"]["status"]

            if status == "PASS":
                logger.info("✓ Validation passed")
                return True
            elif status == "WARNING":
                logger.warning("⚠ Validation passed with warnings")
                return True
            else:
                logger.error("✗ Validation failed")
                return False

        except Exception as e:
            logger.error(f"✗ Validation error: {e}")
            return False

    def run_transform(self) -> bool:
        """
        Transform data

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("STEP 3: TRANSFORM")
            logger.info("=" * 80)

            self.df_transformed = self.transformer.transform(self.df_raw)
            self.df_db_ready = self.transformer.prepare_for_database(self.df_transformed)

            logger.info(f"✓ Transformed {len(self.df_transformed):,} records")
            return True

        except Exception as e:
            logger.error(f"✗ Transform failed: {e}")
            return False

    def run_load(self) -> bool:
        """
        Load data into PostgreSQL

        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info("=" * 80)
            logger.info("STEP 4: LOAD")
            logger.info("=" * 80)

            # Test database connection
            logger.info("Testing database connection...")
            if not self.loader.test_connection():
                logger.error("✗ Database connection failed")
                return False

            # Execute schema if needed
            if not self.skip_schema:
                logger.info("Creating database schema...")
                self.loader.execute_schema()
                logger.info("✓ Schema created")
            else:
                logger.info("Skipping schema creation (--skip-schema flag set)")

            # Populate dimension tables
            logger.info("Populating dimension tables...")
            self.loader.populate_dimension_tables(self.df_db_ready)
            logger.info("✓ Dimension tables populated")

            # Load fact table
            logger.info("Loading fire incidents data...")
            self.loader.load_data_batch(self.df_db_ready)
            logger.info("✓ Data loaded")

            # Update foreign keys
            logger.info("Updating foreign key references...")
            self.loader.update_foreign_keys()
            logger.info("✓ Foreign keys updated")

            # Verify load
            logger.info("Verifying data load...")
            self.loader.verify_load()
            logger.info("✓ Load verified")

            return True

        except Exception as e:
            logger.error(f"✗ Load failed: {e}")
            return False

    def run_pipeline(self) -> bool:
        """
        Run the complete ETL pipeline

        Returns:
            True if all steps successful, False otherwise
        """
        start_time = datetime.now()

        logger.info("╔" + "═" * 78 + "╗")
        logger.info("║" + " " * 20 + "FIRE INCIDENT ETL PIPELINE" + " " * 32 + "║")
        logger.info("║" + " " * 24 + "Edmonton Analytics" + " " * 35 + "║")
        logger.info("╚" + "═" * 78 + "╝")
        logger.info("")

        # Extract
        if not self.run_extract():
            logger.error("Pipeline failed at EXTRACT step")
            return False

        # Validate
        if not self.run_validate():
            logger.error("Pipeline failed at VALIDATE step")
            response = input("\nContinue despite validation errors? (y/n): ")
            if response.lower() != 'y':
                return False

        # Transform
        if not self.run_transform():
            logger.error("Pipeline failed at TRANSFORM step")
            return False

        # Load
        if not self.run_load():
            logger.error("Pipeline failed at LOAD step")
            return False

        # Success
        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("")
        logger.info("╔" + "═" * 78 + "╗")
        logger.info("║" + " " * 28 + "PIPELINE COMPLETE" + " " * 33 + "║")
        logger.info("╚" + "═" * 78 + "╝")
        logger.info(f"Duration: {duration}")
        logger.info(f"Records processed: {len(self.df_raw):,}")
        logger.info("")

        return True


def main():
    """
    Main entry point with command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Fire Incident ETL Pipeline - Edmonton Analytics"
    )

    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip data validation step"
    )

    parser.add_argument(
        "--skip-schema",
        action="store_true",
        help="Skip schema creation (assumes tables already exist)"
    )

    parser.add_argument(
        "--test",
        action="store_true",
        help="Run with sample data (first 1000 rows) for testing"
    )

    args = parser.parse_args()

    # Create and run pipeline
    pipeline = FireIncidentETL(
        skip_validation=args.skip_validation,
        skip_schema=args.skip_schema
    )

    # If test mode, limit to first 1000 rows
    if args.test:
        logger.info("Running in TEST mode (first 1000 rows)")
        original_extract = pipeline.extractor.extract_data

        def extract_sample():
            df = original_extract()
            return df.head(1000)

        pipeline.extractor.extract_data = extract_sample

    # Run pipeline
    success = pipeline.run_pipeline()

    if success:
        logger.info("✓ ETL Pipeline completed successfully")
        sys.exit(0)
    else:
        logger.error("✗ ETL Pipeline failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
