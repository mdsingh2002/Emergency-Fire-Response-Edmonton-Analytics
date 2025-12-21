"""
Data Cleaning Script
Cleans and standardizes the raw CSV file before ETL processing
"""
import pandas as pd
from pathlib import Path
import sys
from loguru import logger

# Configure logger
logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"
)


def clean_fire_incident_data(input_file: str, output_file: str = None):
    """
    Clean the raw fire incident CSV file

    Args:
        input_file: Path to the raw CSV file
        output_file: Path to save cleaned CSV (default: input_file with _cleaned suffix)
    """
    input_path = Path(input_file)

    if not input_path.exists():
        logger.error(f"Input file not found: {input_file}")
        return False

    if output_file is None:
        output_file = input_path.parent / f"{input_path.stem}_cleaned.csv"
    else:
        output_file = Path(output_file)

    logger.info(f"Input file: {input_path}")
    logger.info(f"Output file: {output_file}")
    logger.info(f"File size: {input_path.stat().st_size / (1024**2):.2f} MB")

    try:
        # Read CSV with minimal parsing - let pandas auto-detect
        logger.info("Reading raw CSV file...")
        df = pd.read_csv(input_path, low_memory=False)

        logger.info(f"Loaded {len(df):,} rows and {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")

        # Identify numeric columns that might have comma formatting
        numeric_columns = [
            'dispatch_year',
            'dispatch_month',
            'dispatch_day',
            'event_duration_mins',
            'neighbourhood_id'
        ]

        logger.info("Cleaning numeric columns...")
        for col in numeric_columns:
            if col in df.columns:
                # Convert to string, remove commas, convert back to numeric
                logger.info(f"  Cleaning: {col}")
                df[col] = df[col].astype(str).str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Clean latitude/longitude (remove any commas)
        if 'latitude' in df.columns:
            logger.info("  Cleaning: latitude")
            df['latitude'] = df['latitude'].astype(str).str.replace(',', '', regex=False)
            df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')

        if 'longitude' in df.columns:
            logger.info("  Cleaning: longitude")
            df['longitude'] = df['longitude'].astype(str).str.replace(',', '', regex=False)
            df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

        # Strip whitespace from string columns
        logger.info("Cleaning string columns...")
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()
            # Replace 'nan' strings with actual NaN
            df[col] = df[col].replace('nan', pd.NA)

        # Log cleaning summary
        logger.info("Cleaning summary:")
        logger.info(f"  Total rows: {len(df):,}")
        logger.info(f"  Total columns: {len(df.columns)}")
        logger.info(f"  Memory usage: {df.memory_usage(deep=True).sum() / (1024**2):.2f} MB")

        # Check for any remaining issues
        null_counts = df.isnull().sum()
        if null_counts.sum() > 0:
            logger.info("Null values found:")
            for col, count in null_counts[null_counts > 0].items():
                logger.info(f"  {col}: {count:,} null values")

        # Save cleaned data
        logger.info(f"Saving cleaned data to: {output_file}")
        df.to_csv(output_file, index=False)

        output_size = output_file.stat().st_size / (1024**2)
        logger.info(f"Cleaned file size: {output_size:.2f} MB")
        logger.info("Data cleaning completed successfully!")

        return True

    except Exception as e:
        logger.error(f"Error during cleaning: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """
    Main entry point
    """
    import argparse

    parser = argparse.ArgumentParser(description="Clean fire incident CSV data")
    parser.add_argument(
        "--input",
        "-i",
        default="Fire_Response_Current_and_Historical_20251214.csv",
        help="Input CSV file path"
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output CSV file path (default: input_file_cleaned.csv)"
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("FIRE INCIDENT DATA CLEANING")
    logger.info("=" * 80)

    success = clean_fire_incident_data(args.input, args.output)

    if success:
        logger.info("=" * 80)
        logger.info("CLEANING COMPLETE")
        logger.info("=" * 80)
        sys.exit(0)
    else:
        logger.error("=" * 80)
        logger.error("CLEANING FAILED")
        logger.error("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    main()
