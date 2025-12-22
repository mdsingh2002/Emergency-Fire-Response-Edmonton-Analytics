# Emergency Fire Response Edmonton Analytics

An end-to-end ETL pipeline that ingests municipal fire incident data, transforms and validates it, loads it into PostgreSQL, and visualizes emergency response performance using Power BI.

## Project Overview

City fire departments need to understand response times, event duration, neighbourhood risk, and resource utilization to improve emergency response and staffing decisions. This project analyzes Edmonton's fire incident data to identify patterns and optimize resource allocation.

### Key Objectives

- Analyze emergency response patterns across Edmonton neighbourhoods
- Identify peak incident times and resource utilization trends
- Visualize geographic hotspots and response time performance
- Support data-driven decisions for fire department resource allocation

---

## Features

- **Comprehensive ETL Pipeline**: Automated extraction, transformation, and loading of 900K+ fire incident records
- **Data Validation**: Multi-layered validation with data quality checks, business rule validation, and anomaly detection
- **PostgreSQL Database**: Star schema with fact and dimension tables, optimized indexes, and analytical views
- **Power BI Integration**: Ready-to-use dashboard templates and connection guide
- **Scalable Architecture**: Batch processing with configurable chunk sizes for large datasets

---

## Project Structure

```
├── etl/
│   ├── extract.py          # CSV data extraction with chunking support
│   ├── transform.py        # Data transformation and feature engineering
│   ├── validate.py         # Comprehensive data validation
│   └── load.py             # PostgreSQL batch loading
├── sql/
│   └── schema.sql          # Database schema with tables, indexes, and views
├── config/
│   └── config.py           # Configuration management
├── powerbi/
│   ├── POWERBI_SETUP_GUIDE.md
│   └── dashboard_screenshots/
├── logs/                   # ETL execution logs
├── main.py                 # Pipeline orchestration
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variables template
└── README.md
```

---

## Prerequisites

### Software Requirements

- **Python**: 3.9 or higher
- **PostgreSQL**: 14 or higher
- **Power BI Desktop**: Latest version (for visualization)

### System Requirements

- **RAM**: 8GB minimum, 16GB recommended (for processing 900K+ records)
- **Storage**: 2GB free space for database and logs
- **OS**: Windows, macOS, or Linux

---

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/Emergency-Fire-Response-Edmonton-Analytics.git
cd Emergency-Fire-Response-Edmonton-Analytics
```

### 2. Set Up Python Environment

Create and activate a virtual environment:

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS/Linux
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Database

Create a PostgreSQL database:

```sql
CREATE DATABASE fire_incidents_db;
```

Copy `.env.example` to `.env` and update with your database credentials:

```bash
cp .env.example .env
```

Edit `.env`:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fire_incidents_db
DB_USER=your_username
DB_PASSWORD=your_password

BATCH_SIZE=10000
LOG_LEVEL=INFO

CSV_FILE_PATH=Fire_Response_Current_and_Historical_20251214.csv
```

### 5. Download Data

This project uses publicly available municipal fire incident data.
Due to file size limitations, raw CSV files are not included in the repository.

**Download the data from**:

- City of Edmonton Open Data Portal
- Place the CSV file in the project root directory
- Update `CSV_FILE_PATH` in `.env` to match your filename

---

## Usage

### Quick Start

Run the complete ETL pipeline:

```bash
python main.py
```

The pipeline will:

1. Extract data from CSV (~900K records)
2. Validate data quality and business rules
3. Transform and enrich the data
4. Load into PostgreSQL database
5. Create indexes and analytical views

### Command-Line Options

```bash
# Run with test data (first 1000 rows)
python main.py --test

# Skip data validation
python main.py --skip-validation

# Skip schema creation (if tables already exist)
python main.py --skip-schema
```

### Run Individual Modules

Test each module independently:

```bash
# Test extraction
python etl/extract.py

# Test validation
python etl/validate.py

# Test transformation
python etl/transform.py

# Test loading
python etl/load.py
```

---

## Database Schema

### Fact Table

- **fire_incidents**: Main fact table with ~900K incident records
  - Temporal data (dispatch times, event duration)
  - Event classification and descriptions
  - Location data (coordinates, neighbourhoods)
  - Response information (equipment, response codes)

### Dimension Tables

- **dim_event_types**: Event type classifications
- **dim_neighbourhoods**: Edmonton neighbourhood lookup
- **dim_response_codes**: Response code classifications

### Analytical Views

- **vw_daily_incident_summary**: Daily aggregated statistics
- **vw_neighbourhood_stats**: Neighbourhood-level metrics
- **vw_response_time_analysis**: Response time KPIs
- **vw_hourly_patterns**: Hourly incident patterns

See `sql/schema.sql` for complete schema definition.

---

## Data Validation

The pipeline includes comprehensive validation:

### Schema Validation

- Column presence and data types
- Value range checks
- Uniqueness constraints

### Data Quality Checks

- Missing value analysis
- Duplicate detection
- Completeness scoring

### Business Rules

- Minimum row count validation
- Event duration reasonableness
- Geographic coordinate bounds (Edmonton area)
- Date range validation

### Anomaly Detection

- Statistical outlier detection (IQR method)
- Data inconsistency identification
- Unusual pattern flagging

Validation reports are saved to `logs/validation_report_*.json`

---

## Power BI Integration

### Setup Power BI Connection

1. Install PostgreSQL ODBC driver
2. Open Power BI Desktop
3. Get Data → PostgreSQL database
4. Connect using your database credentials
5. Import tables and views

### Recommended Visualizations

- **Executive Dashboard**: KPIs, incident trends, type distribution
- **Geographic Analysis**: Neighbourhood hotspots, response time maps
- **Temporal Patterns**: Peak hours, day-of-week analysis, seasonal trends
- **Performance Metrics**: Response time analysis, equipment utilization

---

## Performance Considerations

### Large Dataset Handling

The pipeline is optimized for large datasets:

- **Chunked Processing**: Configurable batch sizes (default: 10,000 rows)
- **Indexed Tables**: All foreign keys and common query columns indexed
- **Materialized Views**: Pre-aggregated data for faster queries
- **Efficient Data Types**: Optimized column types to reduce storage

### Optimization Tips

1. **Adjust Batch Size**: Modify `BATCH_SIZE` in `.env` based on available RAM
2. **Use Test Mode**: Start with `--test` flag to validate pipeline with sample data
3. **Monitor Logs**: Check `logs/` directory for performance metrics
4. **Database Tuning**: Configure PostgreSQL memory settings for your system

---

## Logging

All ETL operations are logged to the `logs/` directory:

- `extract.log`: Data extraction logs
- `validate.log`: Validation results
- `transform.log`: Transformation operations
- `load.log`: Database loading logs
- `main.log`: Pipeline orchestration logs
- `validation_report_*.json`: Detailed validation reports

Log rotation: 10 MB per file, 30-day retention

---

## Troubleshooting

### Common Issues

**Database Connection Failed**

```bash
# Check PostgreSQL is running
# Windows
pg_ctl status

# Verify connection
psql -U your_username -d fire_incidents_db
```

**Out of Memory**

- Reduce `BATCH_SIZE` in `.env`
- Use `--test` mode for initial testing
- Close other applications

**Import Errors**

- Ensure virtual environment is activated
- Reinstall dependencies: `pip install -r requirements.txt --force-reinstall`

**Data Validation Failures**

- Review validation report in `logs/`
- Use `--skip-validation` to proceed with warnings

---

## Future Enhancements

Potential additions to the pipeline:

- [ ] Apache Airflow DAG for scheduled runs
- [ ] Real-time data streaming integration
- [ ] Machine learning models for incident prediction
- [ ] REST API for data access
- [ ] Docker containerization
- [ ] Automated data quality alerts
- [ ] Additional data sources (weather, traffic)

---

## Data Source

This project uses publicly available municipal fire incident data from the City of Edmonton Open Data Portal.

**Dataset**: Fire Response Current and Historical

- **Records**: 900K+ incident records
- **Time Range**: 2020 - Present
- **Update Frequency**: Regular updates from the city

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

---

## Acknowledgments

- City of Edmonton for providing open data
