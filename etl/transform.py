"""
Transform Module - Fire Incident ETL Pipeline
Handles data transformation and cleaning
"""
import pandas as pd
from pathlib import Path
from typing import Optional
from loguru import logger
import sys
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.config import LOGS_DIR, LOG_CONFIG

# Configure logger
logger.add(
    LOGS_DIR / "transform.log",
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    level=LOG_CONFIG["level"],
    format=LOG_CONFIG["format"],
)


class DataTransformer:
    """
    Transforms and cleans fire incident data
    """

    def __init__(self):
        """Initialize the DataTransformer"""
        logger.info("Initialized DataTransformer")

    def parse_datetime_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse datetime string columns to proper datetime objects

        Args:
            df: DataFrame with datetime string columns

        Returns:
            DataFrame with parsed datetime columns
        """
        logger.info("Parsing datetime columns...")

        # Parse dispatch datetime
        if "dispatch_datetime" in df.columns:
            df["dispatch_datetime_parsed"] = pd.to_datetime(
                df["dispatch_datetime"],
                format="%Y/%m/%d %I:%M:%S %p",
                errors="coerce"
            )
            logger.info(f"Parsed dispatch_datetime: {df['dispatch_datetime_parsed'].notna().sum()} valid entries")

        # Parse event close datetime
        if "event_close_datetime" in df.columns:
            df["event_close_datetime_parsed"] = pd.to_datetime(
                df["event_close_datetime"],
                format="%Y/%m/%d %I:%M:%S %p",
                errors="coerce"
            )
            logger.info(f"Parsed event_close_datetime: {df['event_close_datetime_parsed'].notna().sum()} valid entries")

        # Parse dispatch date (formatted)
        if "dispatch_date_date" in df.columns:
            df["dispatch_date_formatted"] = pd.to_datetime(
                df["dispatch_date_date"],
                format="%Y/%m/%d",
                errors="coerce"
            )
            logger.info(f"Parsed dispatch_date_formatted: {df['dispatch_date_formatted'].notna().sum()} valid entries")

        # Parse event close date (formatted)
        if "event_close_date_date" in df.columns:
            df["event_close_date_formatted"] = pd.to_datetime(
                df["event_close_date_date"],
                format="%Y/%m/%d",
                errors="coerce"
            )
            logger.info(f"Parsed event_close_date_formatted: {df['event_close_date_formatted'].notna().sum()} valid entries")

        # Parse time columns
        if "dispatch_time" in df.columns:
            df["dispatch_time_parsed"] = pd.to_datetime(
                df["dispatch_time"],
                format="%H:%M:%S",
                errors="coerce"
            ).dt.time

        if "event_close_time" in df.columns:
            df["event_close_time_parsed"] = pd.to_datetime(
                df["event_close_time"],
                format="%H:%M:%S",
                errors="coerce"
            ).dt.time

        return df

    def create_derived_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create derived/calculated columns

        Args:
            df: DataFrame to add derived columns to

        Returns:
            DataFrame with derived columns
        """
        logger.info("Creating derived columns...")

        # Extract hour from dispatch datetime
        if "dispatch_datetime_parsed" in df.columns:
            df["dispatch_hour"] = df["dispatch_datetime_parsed"].dt.hour
            logger.debug("Created dispatch_hour column")

        # Create day of week number (0=Monday, 6=Sunday)
        if "dispatch_datetime_parsed" in df.columns:
            df["dispatch_day_of_week_num"] = df["dispatch_datetime_parsed"].dt.dayofweek
            logger.debug("Created dispatch_day_of_week_num column")

        # Create weekend flag
        if "dispatch_day_of_week_num" in df.columns:
            df["is_weekend"] = df["dispatch_day_of_week_num"].isin([5, 6]).astype(int)
            logger.debug("Created is_weekend column")

        # Create shift indicator (Day: 8-16, Night: 20-4, Evening: 16-20, Early Morning: 4-8)
        if "dispatch_hour" in df.columns:
            def get_shift(hour):
                if pd.isna(hour):
                    return None
                if 8 <= hour < 16:
                    return "Day"
                elif 16 <= hour < 20:
                    return "Evening"
                elif 20 <= hour or hour < 4:
                    return "Night"
                else:
                    return "Early Morning"

            df["shift"] = df["dispatch_hour"].apply(get_shift)
            logger.debug("Created shift column")

        # Calculate equipment count
        if "equipment_assigned" in df.columns:
            df["equipment_count"] = df["equipment_assigned"].apply(
                lambda x: len(str(x).split(",")) if pd.notna(x) and str(x).strip() != "" else 0
            )
            logger.debug("Created equipment_count column")

        # Create year-month column for time series analysis
        if "dispatch_date_formatted" in df.columns:
            df["year_month"] = df["dispatch_date_formatted"].dt.to_period("M").astype(str)
            logger.debug("Created year_month column")

        # Create event category grouping (simplified)
        if "event_type_group" in df.columns:
            def categorize_event(event_type):
                if pd.isna(event_type):
                    return "Unknown"
                elif event_type == "FR":
                    return "Fire"
                elif event_type == "MD":
                    return "Medical"
                elif event_type in ["AL", "OF"]:
                    return "Alarm/Fire"
                elif event_type == "TA":
                    return "Traffic Accident"
                elif event_type == "HZ":
                    return "Hazardous"
                else:
                    return "Other"

            df["event_category"] = df["event_type_group"].apply(categorize_event)
            logger.debug("Created event_category column")

        logger.info(f"Created {sum(['dispatch_hour' in df.columns, 'shift' in df.columns, 'equipment_count' in df.columns])} derived columns")

        return df

    def clean_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize text columns

        Args:
            df: DataFrame to clean

        Returns:
            DataFrame with cleaned text columns
        """
        logger.info("Cleaning text columns...")

        text_columns = [
            "event_description",
            "neighbourhood_name",
            "approximate_location",
            "response_code",
            "event_type_group"
        ]

        for col in text_columns:
            if col in df.columns:
                # Remove leading/trailing whitespace
                df[col] = df[col].str.strip()

                # Replace empty strings with None
                df[col] = df[col].replace("", None)

                logger.debug(f"Cleaned {col}")

        return df

    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values with appropriate strategies

        Args:
            df: DataFrame with missing values

        Returns:
            DataFrame with handled missing values
        """
        logger.info("Handling missing values...")

        original_nulls = df.isnull().sum().sum()

        # Handle negative event durations (data quality issue)
        if "event_duration_mins" in df.columns:
            negative_count = (df["event_duration_mins"] < 0).sum()
            if negative_count > 0:
                logger.warning(f"Found {negative_count} records with negative duration - setting to NULL")
                df.loc[df["event_duration_mins"] < 0, "event_duration_mins"] = None

        # For neighbourhood_name, fill with "Unknown" if neighbourhood_id is also missing
        if "neighbourhood_name" in df.columns and "neighbourhood_id" in df.columns:
            mask = df["neighbourhood_id"].isna() & df["neighbourhood_name"].isna()
            df.loc[mask, "neighbourhood_name"] = "Unknown"

        # For approximate_location, fill with "No location" if empty
        if "approximate_location" in df.columns:
            df["approximate_location"] = df["approximate_location"].fillna("No location")

        # For equipment_assigned, fill with "Unknown" if empty
        if "equipment_assigned" in df.columns:
            df["equipment_assigned"] = df["equipment_assigned"].fillna("Unknown")

        final_nulls = df.isnull().sum().sum()
        logger.info(f"Reduced null values from {original_nulls:,} to {final_nulls:,}")

        return df

    def validate_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and clean geographic coordinates

        Args:
            df: DataFrame with coordinate columns

        Returns:
            DataFrame with validated coordinates
        """
        logger.info("Validating coordinates...")

        if "latitude" in df.columns and "longitude" in df.columns:
            # Round coordinates to 8 decimal places to match database precision
            df["latitude"] = df["latitude"].round(8)
            df["longitude"] = df["longitude"].round(8)

            # Edmonton approximate bounds
            edmonton_lat_min, edmonton_lat_max = 53.3, 53.8
            edmonton_lon_min, edmonton_lon_max = -113.7, -113.2

            # Flag invalid coordinates
            invalid_mask = (
                (df["latitude"] < edmonton_lat_min) | (df["latitude"] > edmonton_lat_max) |
                (df["longitude"] < edmonton_lon_min) | (df["longitude"] > edmonton_lon_max)
            )

            invalid_count = invalid_mask.sum()
            if invalid_count > 0:
                logger.warning(f"Found {invalid_count} records with coordinates outside Edmonton bounds")

                # Optionally, set invalid coordinates to None
                # df.loc[invalid_mask, ["latitude", "longitude"]] = None

        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations to the DataFrame

        Args:
            df: Raw DataFrame from extraction

        Returns:
            Transformed DataFrame ready for loading
        """
        logger.info("=" * 80)
        logger.info("Starting data transformation...")
        logger.info(f"Input: {len(df):,} rows, {len(df.columns)} columns")
        logger.info("=" * 80)

        # Apply transformations in sequence
        df = self.parse_datetime_columns(df)
        df = self.create_derived_columns(df)
        df = self.clean_text_columns(df)
        df = self.handle_missing_values(df)
        df = self.validate_coordinates(df)

        logger.info("=" * 80)
        logger.info(f"Transformation complete!")
        logger.info(f"Output: {len(df):,} rows, {len(df.columns)} columns")
        logger.info("=" * 80)

        return df

    def prepare_for_database(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for database insertion

        Args:
            df: Transformed DataFrame

        Returns:
            DataFrame ready for database insertion
        """
        logger.info("Preparing data for database insertion...")

        # Select and rename columns to match database schema
        db_columns_map = {
            "event_number": "event_number",
            "dispatch_year": "dispatch_year",
            "dispatch_month": "dispatch_month",
            "dispatch_day": "dispatch_day",
            "dispatch_month_name": "dispatch_month_name",
            "dispatch_dayofweek": "dispatch_dayofweek",
            "dispatch_date": "dispatch_date",
            "dispatch_date_formatted": "dispatch_date_formatted",
            "dispatch_time_parsed": "dispatch_time",
            "dispatch_datetime_parsed": "dispatch_datetime",
            "event_close_date": "event_close_date",
            "event_close_date_formatted": "event_close_date_formatted",
            "event_close_time_parsed": "event_close_time",
            "event_close_datetime_parsed": "event_close_datetime",
            "event_duration_mins": "event_duration_mins",
            "event_type_group": "event_type_group",
            "event_description": "event_description",
            "neighbourhood_id": "neighbourhood_id",
            "neighbourhood_name": "neighbourhood_name",
            "approximate_location": "approximate_location",
            "equipment_assigned": "equipment_assigned",
            "latitude": "latitude",
            "longitude": "longitude",
            "geometry_point": "geometry_point",
            "response_code": "response_code",
        }

        # Create new DataFrame with only the columns we need
        db_df = pd.DataFrame()

        for source_col, target_col in db_columns_map.items():
            if source_col in df.columns:
                db_df[target_col] = df[source_col]
            else:
                logger.warning(f"Column {source_col} not found in DataFrame")
                db_df[target_col] = None

        logger.info(f"Prepared {len(db_df):,} rows for database with {len(db_df.columns)} columns")

        return db_df


def main():
    """
    Main function for testing the transform module
    """
    from extract import DataExtractor

    logger.info("=" * 80)
    logger.info("Starting Transform Module Test")
    logger.info("=" * 80)

    # Extract data
    extractor = DataExtractor()
    df = extractor.extract_data()

    # Transform data
    transformer = DataTransformer()
    df_transformed = transformer.transform(df)

    logger.info("\nTransformed Data Sample:")
    logger.info(f"\n{df_transformed.head()}")

    logger.info("\nNew Columns Created:")
    new_cols = set(df_transformed.columns) - set(df.columns)
    for col in new_cols:
        logger.info(f"  - {col}")

    # Prepare for database
    df_db_ready = transformer.prepare_for_database(df_transformed)

    logger.info("\nDatabase-ready Data:")
    logger.info(f"Columns: {list(df_db_ready.columns)}")
    logger.info(f"Sample:\n{df_db_ready.head()}")

    logger.info("=" * 80)
    logger.info("Transform Module Test Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
