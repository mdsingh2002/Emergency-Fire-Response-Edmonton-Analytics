"""
Configuration module for Fire Incident ETL Pipeline
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT
LOGS_DIR = PROJECT_ROOT / "logs"
SQL_DIR = PROJECT_ROOT / "sql"

# Ensure logs directory exists
LOGS_DIR.mkdir(exist_ok=True)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "fire_incidents_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# Database connection string for SQLAlchemy
DB_CONNECTION_STRING = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# ETL Configuration
ETL_CONFIG = {
    "batch_size": int(os.getenv("BATCH_SIZE", 10000)),
    "csv_file_path": DATA_DIR / os.getenv("CSV_FILE_PATH", "Fire_Response_Current_and_Historical_20251214.csv"),
}

# Logging configuration
LOG_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    "rotation": "10 MB",
    "retention": "30 days",
}

# Validation thresholds
VALIDATION_CONFIG = {
    "max_null_percentage": 10,  # Maximum allowed percentage of null values per column
    "min_rows_expected": 100000,  # Minimum expected number of rows
    "max_duration_minutes": 1440,  # Maximum reasonable event duration (24 hours)
    "date_range_start": "2020-01-01",  # Expected earliest date
    "date_range_end": "2026-12-31",  # Expected latest date
}
