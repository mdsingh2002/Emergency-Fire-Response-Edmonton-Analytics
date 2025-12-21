"""
Extract Module - Fire Incident ETL Pipeline
Handles data extraction from CSV source files
"""
import pandas as pd
from pathlib import Path
from typing import Optional
from loguru import logger
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.config import ETL_CONFIG, LOGS_DIR, LOG_CONFIG

# Configure logger
logger.add(
    LOGS_DIR / "extract.log",
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    level=LOG_CONFIG["level"],
    format=LOG_CONFIG["format"],
)


class DataExtractor:
    """
    Extracts fire incident data from CSV files
    """

    def __init__(self, file_path: Optional[Path] = None):
        """
        Initialize the DataExtractor

        Args:
            file_path: Path to the CSV file. Uses config default if not provided.
        """
        self.file_path = file_path or ETL_CONFIG["csv_file_path"]
        logger.info(f"Initialized DataExtractor with file: {self.file_path}")

    def extract_data(self, chunk_size: Optional[int] = None) -> pd.DataFrame:
        """
        Extract data from CSV file

        Args:
            chunk_size: If provided, reads data in chunks. Otherwise loads entire file.

        Returns:
            DataFrame containing the extracted data

        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            pd.errors.EmptyDataError: If the CSV file is empty
        """
        if not self.file_path.exists():
            logger.error(f"File not found: {self.file_path}")
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

        logger.info(f"Starting data extraction from: {self.file_path}")
        logger.info(f"File size: {self.file_path.stat().st_size / (1024**2):.2f} MB")

        try:
            # Read CSV with appropriate data types
            dtype_spec = {
                'event_number': str,
                'dispatch_year': 'Int64',
                'dispatch_month': 'Int64',
                'dispatch_day': 'Int64',
                'dispatch_month_name': str,
                'dispatch_dayofweek': str,
                'dispatch_date': str,
                'dispatch_date_date': str,
                'dispatch_time': str,
                'dispatch_datetime': str,
                'event_close_date': str,
                'event_close_date_date': str,
                'event_close_time': str,
                'event_close_datetime': str,
                'event_duration_mins': 'Int64',
                'event_type_group': str,
                'event_description': str,
                'neighbourhood_id': 'Int64',
                'neighbourhood_name': str,
                'approximate_location': str,
                'equipment_assigned': str,
                'latitude': float,
                'longitude': float,
                'geometry_point': str,
                'response_code': str,
            }

            if chunk_size:
                logger.info(f"Reading data in chunks of {chunk_size} rows")
                chunks = []
                for chunk in pd.read_csv(
                    self.file_path,
                    dtype=dtype_spec,
                    low_memory=False,
                    chunksize=chunk_size
                ):
                    chunks.append(chunk)
                    logger.debug(f"Loaded chunk with {len(chunk)} rows")

                df = pd.concat(chunks, ignore_index=True)
            else:
                logger.info("Reading entire file at once")
                df = pd.read_csv(
                    self.file_path,
                    dtype=dtype_spec,
                    low_memory=False
                )

            logger.info(f"Successfully extracted {len(df):,} records")
            logger.info(f"Number of columns: {len(df.columns)}")
            logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

            # Log basic statistics
            logger.info(f"Date range: {df['dispatch_year'].min()} to {df['dispatch_year'].max()}")
            logger.info(f"Unique event types: {df['event_type_group'].nunique()}")
            logger.info(f"Unique neighbourhoods: {df['neighbourhood_id'].nunique()}")

            return df

        except pd.errors.EmptyDataError:
            logger.error("CSV file is empty")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV file: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during extraction: {e}")
            raise

    def get_file_info(self) -> dict:
        """
        Get metadata about the source file

        Returns:
            Dictionary containing file metadata
        """
        if not self.file_path.exists():
            return {"error": "File not found"}

        file_stat = self.file_path.stat()

        return {
            "file_path": str(self.file_path),
            "file_name": self.file_path.name,
            "file_size_mb": file_stat.st_size / (1024**2),
            "last_modified": file_stat.st_mtime,
        }

    def preview_data(self, n_rows: int = 10) -> pd.DataFrame:
        """
        Preview first n rows of the CSV file

        Args:
            n_rows: Number of rows to preview

        Returns:
            DataFrame with first n rows
        """
        logger.info(f"Previewing first {n_rows} rows")

        try:
            df_preview = pd.read_csv(self.file_path, nrows=n_rows)
            return df_preview
        except Exception as e:
            logger.error(f"Error previewing data: {e}")
            raise


def main():
    """
    Main function for testing the extract module
    """
    logger.info("=" * 80)
    logger.info("Starting Extract Module Test")
    logger.info("=" * 80)

    # Initialize extractor
    extractor = DataExtractor()

    # Get file info
    file_info = extractor.get_file_info()
    logger.info(f"File Info: {file_info}")

    # Preview data
    logger.info("\nPreviewing data:")
    preview = extractor.preview_data(5)
    logger.info(f"\n{preview}")

    # Extract full data
    logger.info("\nExtracting full dataset...")
    df = extractor.extract_data()

    logger.info("\nExtraction Summary:")
    logger.info(f"Total records: {len(df):,}")
    logger.info(f"Columns: {list(df.columns)}")
    logger.info(f"\nData types:\n{df.dtypes}")
    logger.info(f"\nMissing values:\n{df.isnull().sum()}")

    logger.info("=" * 80)
    logger.info("Extract Module Test Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
