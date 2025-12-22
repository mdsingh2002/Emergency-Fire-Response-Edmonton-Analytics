"""
Validate Module - Fire Incident ETL Pipeline
Comprehensive data validation and quality checks
"""
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from pathlib import Path
from typing import Dict, List, Tuple
from loguru import logger
import sys
from datetime import datetime
import json

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from config.config import VALIDATION_CONFIG, LOGS_DIR, LOG_CONFIG

# Configure logger
logger.add(
    LOGS_DIR / "validate.log",
    rotation=LOG_CONFIG["rotation"],
    retention=LOG_CONFIG["retention"],
    level=LOG_CONFIG["level"],
    format=LOG_CONFIG["format"],
)


# Expected columns from the CSV
EXPECTED_COLUMNS = [
    "event_number", "dispatch_year", "dispatch_month", "dispatch_day",
    "dispatch_month_name", "dispatch_dayofweek", "dispatch_date",
    "dispatch_date_date", "dispatch_time", "dispatch_datetime",
    "event_close_date", "event_close_date_date", "event_close_time",
    "event_close_datetime", "event_duration_mins", "event_type_group",
    "event_description", "neighbourhood_id", "neighbourhood_name",
    "approximate_location", "equipment_assigned", "latitude",
    "longitude", "geometry_point", "response_code"
]


class DataValidator:
    """
    Comprehensive data validation for fire incident data
    """

    def __init__(self):
        """Initialize the DataValidator"""
        self.validation_results = {
            "schema_validation": {},
            "data_quality": {},
            "business_rules": {},
            "anomalies": {},
            "summary": {},
        }
        logger.info("Initialized DataValidator")

    def validate_columns(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate that all expected columns are present

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list of errors)
        """
        logger.info("Validating columns...")
        errors = []

        # Check for missing columns
        missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            for col in missing_cols:
                error_msg = f"Missing column: {col}"
                errors.append(error_msg)
                logger.error(error_msg)

        # Check for extra columns
        extra_cols = set(df.columns) - set(EXPECTED_COLUMNS)
        if extra_cols:
            for col in extra_cols:
                warning_msg = f"Extra column found: {col}"
                logger.warning(warning_msg)

        is_valid = len(missing_cols) == 0
        logger.info(f"Column validation: {'PASS' if is_valid else 'FAIL'}")

        return is_valid, errors

    def create_schema(self) -> DataFrameSchema:
        """
        Create Pandera schema for data validation

        Returns:
            DataFrameSchema object defining expected data structure
        """
        schema = DataFrameSchema(
            {
                "event_number": Column(str, nullable=False, unique=True),
                "dispatch_year": Column("Int64", Check.in_range(2000, 2030), nullable=True),
                "dispatch_month": Column("Int64", Check.in_range(1, 12), nullable=True),
                "dispatch_day": Column("Int64", Check.in_range(1, 31), nullable=True),
                "dispatch_month_name": Column(str, nullable=True),
                "dispatch_dayofweek": Column(str, nullable=True),
                "dispatch_date": Column(str, nullable=True),
                "dispatch_date_date": Column(str, nullable=True),
                "dispatch_time": Column(str, nullable=True),
                "dispatch_datetime": Column(str, nullable=True),
                "event_close_date": Column(str, nullable=True),
                "event_close_date_date": Column(str, nullable=True),
                "event_close_time": Column(str, nullable=True),
                "event_close_datetime": Column(str, nullable=True),
                "event_duration_mins": Column("Int64", Check.greater_than_or_equal_to(0), nullable=True),
                "event_type_group": Column(str, nullable=True),
                "event_description": Column(str, nullable=True),
                "neighbourhood_id": Column("Int64", nullable=True),
                "neighbourhood_name": Column(str, nullable=True),
                "approximate_location": Column(str, nullable=True),
                "equipment_assigned": Column(str, nullable=True),
                "latitude": Column(float, Check.in_range(-90, 90), nullable=True),
                "longitude": Column(float, Check.in_range(-180, 180), nullable=True),
                "geometry_point": Column(str, nullable=True),
                "response_code": Column(str, nullable=True),
            },
            strict=False,
        )
        return schema

    def validate_schema(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Validate DataFrame against schema

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list of errors)
        """
        logger.info("Validating schema...")
        errors = []

        # First validate columns
        cols_valid, col_errors = self.validate_columns(df)
        errors.extend(col_errors)

        try:
            schema = self.create_schema()
            schema.validate(df, lazy=True)
            logger.info("Schema validation passed")
            self.validation_results["schema_validation"]["status"] = "PASS"
            return cols_valid and True, errors

        except pa.errors.SchemaErrors as e:
            logger.warning(f"Schema validation failed with {len(e.failure_cases)} errors")
            for idx, row in e.failure_cases.iterrows():
                error_msg = f"Column: {row['column']}, Check: {row['check']}"
                errors.append(error_msg)
                logger.debug(error_msg)

            self.validation_results["schema_validation"]["status"] = "FAIL"
            self.validation_results["schema_validation"]["errors"] = errors[:10]

            return False, errors

    def check_data_quality(self, df: pd.DataFrame) -> Dict:
        """
        Perform comprehensive data quality checks

        Args:
            df: DataFrame to check

        Returns:
            Dictionary containing quality metrics
        """
        logger.info("Checking data quality...")

        quality_report = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "duplicate_rows": df.duplicated().sum(),
            "missing_values": {},
            "completeness_score": 0.0,
        }

        # Check missing values per column
        missing_stats = df.isnull().sum()
        missing_pct = (missing_stats / len(df)) * 100

        for col in df.columns:
            if missing_stats[col] > 0:
                quality_report["missing_values"][col] = {
                    "count": int(missing_stats[col]),
                    "percentage": round(float(missing_pct[col]), 2),
                }

                # Flag columns with high missing percentages
                if missing_pct[col] > VALIDATION_CONFIG["max_null_percentage"]:
                    logger.warning(
                        f"Column '{col}' has {missing_pct[col]:.2f}% missing values "
                        f"(threshold: {VALIDATION_CONFIG['max_null_percentage']}%)"
                    )

        # Calculate overall completeness score
        total_cells = len(df) * len(df.columns)
        filled_cells = total_cells - df.isnull().sum().sum()
        quality_report["completeness_score"] = round((filled_cells / total_cells) * 100, 2)

        logger.info(f"Data completeness: {quality_report['completeness_score']}%")
        logger.info(f"Duplicate rows: {quality_report['duplicate_rows']}")

        self.validation_results["data_quality"] = quality_report

        return quality_report

    def validate_business_rules(self, df: pd.DataFrame) -> Dict:
        """
        Validate business-specific rules

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary containing business rule validation results
        """
        logger.info("Validating business rules...")

        business_rules = {
            "rules_passed": [],
            "rules_failed": [],
            "warnings": [],
        }

        # Rule 1: Minimum row count
        min_rows = VALIDATION_CONFIG["min_rows_expected"]
        if len(df) >= min_rows:
            business_rules["rules_passed"].append(f"Row count check: {len(df):,} >= {min_rows:,}")
        else:
            business_rules["rules_failed"].append(
                f"Row count check failed: {len(df):,} < {min_rows:,}"
            )
            logger.error(f"Insufficient data: {len(df):,} rows (expected >= {min_rows:,})")

        # Rule 2: Event duration reasonableness
        max_duration = VALIDATION_CONFIG["max_duration_minutes"]
        excessive_duration = df[df["event_duration_mins"] > max_duration]
        if len(excessive_duration) == 0:
            business_rules["rules_passed"].append("Event duration check: All within limits")
        else:
            pct = (len(excessive_duration) / len(df)) * 100
            business_rules["warnings"].append(
                f"{len(excessive_duration)} events ({pct:.2f}%) exceed {max_duration} minutes"
            )
            logger.warning(f"Found {len(excessive_duration)} events with excessive duration")

        # Rule 3: Valid event types
        valid_event_types = df["event_type_group"].dropna().unique()
        logger.info(f"Event types found: {sorted(valid_event_types)}")
        business_rules["rules_passed"].append(
            f"Event types: {len(valid_event_types)} unique types identified"
        )

        # Rule 4: Geographic coordinates consistency
        coords_df = df[["latitude", "longitude"]].dropna()
        if len(coords_df) > 0:
            # Check if coordinates are within Edmonton area (approximate)
            edmonton_lat_range = (53.3, 53.8)
            edmonton_lon_range = (-113.7, -113.2)

            invalid_coords = coords_df[
                ~coords_df["latitude"].between(*edmonton_lat_range) |
                ~coords_df["longitude"].between(*edmonton_lon_range)
            ]

            if len(invalid_coords) == 0:
                business_rules["rules_passed"].append("Geographic coordinates: All within Edmonton area")
            else:
                pct = (len(invalid_coords) / len(coords_df)) * 100
                business_rules["warnings"].append(
                    f"{len(invalid_coords)} records ({pct:.2f}%) have coordinates outside Edmonton area"
                )
                logger.warning(f"Found {len(invalid_coords)} records with unusual coordinates")

        # Rule 5: Date range validation
        if "dispatch_year" in df.columns:
            year_min = df["dispatch_year"].min()
            year_max = df["dispatch_year"].max()
            logger.info(f"Date range: {year_min} to {year_max}")

            expected_min = int(VALIDATION_CONFIG["date_range_start"][:4])
            expected_max = int(VALIDATION_CONFIG["date_range_end"][:4])

            if year_min >= expected_min and year_max <= expected_max:
                business_rules["rules_passed"].append(
                    f"Date range check: {year_min}-{year_max} within expected range"
                )
            else:
                business_rules["warnings"].append(
                    f"Date range {year_min}-{year_max} outside expected {expected_min}-{expected_max}"
                )

        # Rule 6: Event number uniqueness
        duplicates = df["event_number"].duplicated().sum()
        if duplicates == 0:
            business_rules["rules_passed"].append("Event number uniqueness: No duplicates")
        else:
            business_rules["rules_failed"].append(
                f"Event number duplicates: {duplicates} duplicate event numbers found"
            )
            logger.error(f"Found {duplicates} duplicate event numbers")

        logger.info(f"Business rules - Passed: {len(business_rules['rules_passed'])}, "
                   f"Failed: {len(business_rules['rules_failed'])}, "
                   f"Warnings: {len(business_rules['warnings'])}")

        self.validation_results["business_rules"] = business_rules

        return business_rules

    def detect_anomalies(self, df: pd.DataFrame) -> Dict:
        """
        Detect anomalies and outliers in the data

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary containing anomaly detection results
        """
        logger.info("Detecting anomalies...")

        anomalies = {
            "duration_outliers": [],
            "unusual_patterns": [],
            "data_inconsistencies": [],
        }

        # Detect duration outliers using IQR method
        if "event_duration_mins" in df.columns:
            duration_data = df["event_duration_mins"].dropna()
            if len(duration_data) > 0:
                Q1 = duration_data.quantile(0.25)
                Q3 = duration_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 3 * IQR
                upper_bound = Q3 + 3 * IQR

                outliers = df[
                    (df["event_duration_mins"] < lower_bound) |
                    (df["event_duration_mins"] > upper_bound)
                ]

                if len(outliers) > 0:
                    anomalies["duration_outliers"] = {
                        "count": len(outliers),
                        "percentage": round((len(outliers) / len(df)) * 100, 2),
                        "bounds": {"lower": float(lower_bound), "upper": float(upper_bound)},
                    }
                    logger.info(f"Found {len(outliers)} duration outliers")

        # Detect missing neighbourhood names where ID exists
        missing_names = df[df["neighbourhood_id"].notna() & df["neighbourhood_name"].isna()]
        if len(missing_names) > 0:
            anomalies["data_inconsistencies"].append({
                "type": "missing_neighbourhood_name",
                "count": len(missing_names),
                "description": "Records have neighbourhood_id but missing neighbourhood_name",
            })

        # Detect events with missing location data
        missing_location = df[
            df["latitude"].isna() & df["longitude"].isna() & df["approximate_location"].notna()
        ]
        if len(missing_location) > 0:
            anomalies["unusual_patterns"].append({
                "type": "missing_coordinates",
                "count": len(missing_location),
                "description": "Events have location description but no coordinates",
            })

        logger.info(f"Anomaly detection complete")

        self.validation_results["anomalies"] = anomalies

        return anomalies

    def generate_validation_report(self, df: pd.DataFrame) -> Dict:
        """
        Generate comprehensive validation report

        Args:
            df: DataFrame that was validated

        Returns:
            Complete validation report
        """
        logger.info("Generating validation report...")

        # Run all validations
        schema_valid, schema_errors = self.validate_schema(df)
        quality_report = self.check_data_quality(df)
        business_rules = self.validate_business_rules(df)
        anomalies = self.detect_anomalies(df)

        # Create summary
        total_issues = (
            len(schema_errors) +
            len(business_rules["rules_failed"]) +
            (1 if anomalies.get("duration_outliers") else 0)
        )

        self.validation_results["summary"] = {
            "timestamp": datetime.now().isoformat(),
            "total_records": len(df),
            "schema_valid": schema_valid,
            "total_issues": total_issues,
            "data_quality_score": quality_report["completeness_score"],
            "status": "PASS" if total_issues == 0 else "FAIL" if total_issues > 10 else "WARNING",
        }

        # Save report to file
        report_path = LOGS_DIR / f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, "w") as f:
            json.dump(self.validation_results, f, indent=2)

        logger.info(f"Validation report saved to: {report_path}")
        logger.info(f"Validation status: {self.validation_results['summary']['status']}")

        return self.validation_results

    def print_summary(self):
        """Print a human-readable summary of validation results"""
        print("\n" + "=" * 80)
        print("VALIDATION SUMMARY")
        print("=" * 80)

        summary = self.validation_results.get("summary", {})
        print(f"Status: {summary.get('status', 'N/A')}")
        print(f"Total Records: {summary.get('total_records', 0):,}")
        print(f"Data Quality Score: {summary.get('data_quality_score', 0):.2f}%")
        print(f"Total Issues: {summary.get('total_issues', 0)}")

        print("\nBusiness Rules:")
        br = self.validation_results.get("business_rules", {})
        print(f"  Passed: {len(br.get('rules_passed', []))}")
        print(f"  Failed: {len(br.get('rules_failed', []))}")
        print(f"  Warnings: {len(br.get('warnings', []))}")

        print("=" * 80 + "\n")


def main():
    """
    Main function for testing the validate module
    """
    from extract import DataExtractor

    logger.info("=" * 80)
    logger.info("Starting Validate Module Test")
    logger.info("=" * 80)

    # Extract data
    extractor = DataExtractor()
    df = extractor.extract_data()

    # Validate data
    validator = DataValidator()
    report = validator.generate_validation_report(df)

    # Print summary
    validator.print_summary()

    logger.info("=" * 80)
    logger.info("Validate Module Test Complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
