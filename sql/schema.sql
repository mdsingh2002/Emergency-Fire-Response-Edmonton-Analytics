-- ============================================================================
-- Fire Incidents Database Schema
-- Edmonton Emergency Fire Response Analytics
-- ============================================================================

-- Drop existing tables (in reverse order of dependencies)
DROP TABLE IF EXISTS fire_incidents CASCADE;
DROP TABLE IF EXISTS dim_neighbourhoods CASCADE;
DROP TABLE IF EXISTS dim_event_types CASCADE;
DROP TABLE IF EXISTS dim_response_codes CASCADE;

-- ============================================================================
-- Dimension Tables
-- ============================================================================

-- Dimension: Event Types
CREATE TABLE dim_event_types (
    event_type_id SERIAL PRIMARY KEY,
    event_type_code VARCHAR(10) UNIQUE NOT NULL,
    event_description VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_event_type_code ON dim_event_types(event_type_code);

-- Dimension: Response Codes
CREATE TABLE dim_response_codes (
    response_code_id SERIAL PRIMARY KEY,
    response_code VARCHAR(10) UNIQUE NOT NULL,
    response_description VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_response_code ON dim_response_codes(response_code);

-- Dimension: Neighbourhoods
CREATE TABLE dim_neighbourhoods (
    neighbourhood_id INTEGER PRIMARY KEY,
    neighbourhood_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_neighbourhood_name ON dim_neighbourhoods(neighbourhood_name);

-- ============================================================================
-- Fact Table: Fire Incidents
-- ============================================================================

CREATE TABLE fire_incidents (
    incident_id SERIAL PRIMARY KEY,
    event_number VARCHAR(50) UNIQUE NOT NULL,

    -- Temporal columns
    dispatch_year INTEGER,
    dispatch_month INTEGER,
    dispatch_day INTEGER,
    dispatch_month_name VARCHAR(20),
    dispatch_dayofweek VARCHAR(20),
    dispatch_date VARCHAR(20),
    dispatch_date_formatted DATE,
    dispatch_time TIME,
    dispatch_datetime TIMESTAMP,

    event_close_date VARCHAR(20),
    event_close_date_formatted DATE,
    event_close_time TIME,
    event_close_datetime TIMESTAMP,

    event_duration_mins INTEGER,

    -- Event classification
    event_type_group VARCHAR(10),
    event_description VARCHAR(100),
    event_type_id INTEGER REFERENCES dim_event_types(event_type_id),

    -- Location information
    neighbourhood_id INTEGER REFERENCES dim_neighbourhoods(neighbourhood_id),
    neighbourhood_name VARCHAR(100),
    approximate_location TEXT,
    latitude DECIMAL(12, 8),
    longitude DECIMAL(12, 8),
    geometry_point TEXT,

    -- Response information
    equipment_assigned TEXT,
    response_code VARCHAR(10),
    response_code_id INTEGER REFERENCES dim_response_codes(response_code_id),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT chk_valid_month CHECK (dispatch_month BETWEEN 1 AND 12),
    CONSTRAINT chk_valid_day CHECK (dispatch_day BETWEEN 1 AND 31),
    CONSTRAINT chk_valid_duration CHECK (event_duration_mins >= 0),
    CONSTRAINT chk_valid_coords CHECK (
        (latitude IS NULL AND longitude IS NULL) OR
        (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)
    )
);

-- ============================================================================
-- Indexes for Query Performance
-- ============================================================================

-- Temporal indexes
CREATE INDEX idx_dispatch_datetime ON fire_incidents(dispatch_datetime);
CREATE INDEX idx_dispatch_date_formatted ON fire_incidents(dispatch_date_formatted);
CREATE INDEX idx_dispatch_year ON fire_incidents(dispatch_year);
CREATE INDEX idx_dispatch_month ON fire_incidents(dispatch_month);
CREATE INDEX idx_event_close_datetime ON fire_incidents(event_close_datetime);

-- Event classification indexes
CREATE INDEX idx_event_type_group ON fire_incidents(event_type_group);
CREATE INDEX idx_event_description ON fire_incidents(event_description);
CREATE INDEX idx_event_type_id ON fire_incidents(event_type_id);

-- Location indexes
CREATE INDEX idx_neighbourhood_id ON fire_incidents(neighbourhood_id);
CREATE INDEX idx_neighbourhood_name_fi ON fire_incidents(neighbourhood_name);
CREATE INDEX idx_lat_long ON fire_incidents(latitude, longitude);

-- Response indexes
CREATE INDEX idx_response_code_fi ON fire_incidents(response_code);
CREATE INDEX idx_response_code_id ON fire_incidents(response_code_id);

-- Duration index for performance analysis
CREATE INDEX idx_event_duration ON fire_incidents(event_duration_mins);

-- Composite indexes for common queries
CREATE INDEX idx_type_date ON fire_incidents(event_type_group, dispatch_date_formatted);
CREATE INDEX idx_neighbourhood_date ON fire_incidents(neighbourhood_id, dispatch_date_formatted);

-- ============================================================================
-- Views for Analytics
-- ============================================================================

-- View: Daily incident summary
CREATE OR REPLACE VIEW vw_daily_incident_summary AS
SELECT
    dispatch_date_formatted,
    dispatch_year,
    dispatch_month,
    dispatch_dayofweek,
    event_type_group,
    COUNT(*) as incident_count,
    AVG(event_duration_mins) as avg_duration_mins,
    MIN(event_duration_mins) as min_duration_mins,
    MAX(event_duration_mins) as max_duration_mins
FROM fire_incidents
WHERE dispatch_date_formatted IS NOT NULL
GROUP BY
    dispatch_date_formatted,
    dispatch_year,
    dispatch_month,
    dispatch_dayofweek,
    event_type_group
ORDER BY dispatch_date_formatted DESC;

-- View: Neighbourhood incident statistics
CREATE OR REPLACE VIEW vw_neighbourhood_stats AS
SELECT
    n.neighbourhood_id,
    n.neighbourhood_name,
    COUNT(f.incident_id) as total_incidents,
    COUNT(DISTINCT DATE(f.dispatch_datetime)) as days_with_incidents,
    AVG(f.event_duration_mins) as avg_response_duration,
    COUNT(CASE WHEN f.event_type_group = 'FR' THEN 1 END) as fire_incidents,
    COUNT(CASE WHEN f.event_type_group = 'MD' THEN 1 END) as medical_incidents,
    COUNT(CASE WHEN f.event_type_group = 'AL' THEN 1 END) as alarm_incidents
FROM dim_neighbourhoods n
LEFT JOIN fire_incidents f ON n.neighbourhood_id = f.neighbourhood_id
GROUP BY n.neighbourhood_id, n.neighbourhood_name
ORDER BY total_incidents DESC;

-- View: Response time analysis
CREATE OR REPLACE VIEW vw_response_time_analysis AS
SELECT
    event_type_group,
    response_code,
    COUNT(*) as incident_count,
    AVG(event_duration_mins) as avg_duration,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY event_duration_mins) as median_duration,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY event_duration_mins) as p95_duration
FROM fire_incidents
WHERE event_duration_mins IS NOT NULL
GROUP BY event_type_group, response_code
ORDER BY incident_count DESC;

-- View: Hourly incident patterns
CREATE OR REPLACE VIEW vw_hourly_patterns AS
SELECT
    EXTRACT(HOUR FROM dispatch_datetime) as hour_of_day,
    dispatch_dayofweek,
    event_type_group,
    COUNT(*) as incident_count
FROM fire_incidents
WHERE dispatch_datetime IS NOT NULL
GROUP BY
    EXTRACT(HOUR FROM dispatch_datetime),
    dispatch_dayofweek,
    event_type_group
ORDER BY hour_of_day, dispatch_dayofweek;

-- ============================================================================
-- Functions and Triggers
-- ============================================================================

-- Function: Update timestamp on row modification
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger: Auto-update updated_at timestamp
CREATE TRIGGER update_fire_incidents_updated_at
    BEFORE UPDATE ON fire_incidents
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Grant Permissions (adjust username as needed)
-- ============================================================================

-- GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO your_username;

-- ============================================================================
-- Comments for Documentation
-- ============================================================================

COMMENT ON TABLE fire_incidents IS 'Main fact table containing all fire incident records from Edmonton';
COMMENT ON TABLE dim_event_types IS 'Dimension table for event type classifications';
COMMENT ON TABLE dim_neighbourhoods IS 'Dimension table for Edmonton neighbourhoods';
COMMENT ON TABLE dim_response_codes IS 'Dimension table for response code classifications';

COMMENT ON COLUMN fire_incidents.event_duration_mins IS 'Duration in minutes from dispatch to event closure';
COMMENT ON COLUMN fire_incidents.response_code IS 'Response priority/type code';
COMMENT ON COLUMN fire_incidents.equipment_assigned IS 'List of equipment units assigned to the incident';
