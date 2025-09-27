# Flight Delays ETL Pipeline

A production-ready ETL pipeline for processing 5.8M+ flight records into a dimensional model with advanced batch processing and resumable state management.

## Project Overview

This project implements a dimensional modeling approach to store and analyze 2015 flight data from three CSV sources:
- **airlines.csv**: 14 airline carriers with IATA codes
- **airports.csv**: 300+ airports with geographic information
- **flights.csv**: 5.8M+ flight records with delay information

### Key Features
- **Batch Processing**: Memory-efficient processing in configurable batches (default 100K records)
- **State Management**: Resumable processing with automatic recovery from interruptions


## Architecture

### Dimensional Model
- **Fact Table**: `fact_flights` - Central table with flight metrics
- **Dimension Tables**:
  - `dim_airline` - Airline information and categorization
  - `dim_airport` - Airport details with geographic data  
  - `dim_date` - Calendar dimension with business attributes
  - `dim_time` - Time dimension for departure analysis
  - `dim_delay_cause` - Categorized delay causes

### Batch Processing Mechanism
The pipeline processes flights data in configurable batches directly from CSV without loading the entire 5.8M dataset into memory:
- **Memory Efficiency**: Constant ~500MB RAM usage regardless of dataset size
- **True Streaming**: Processes one batch at a time, immediately loads to database
- **Configurable Batches**: Adjustable batch size via `BATCH_SIZE` environment variable

### State Manager
Advanced state management system provides production-level reliability:
- **JSON Persistence**: Saves progress after each successful batch (`etl_state.json`)
- **Automatic Recovery**: Resumes from exact interruption point
- **Batch Size Recalculation**: Handles configuration changes between runs
- **Progress Tracking**: Detailed metrics with percentage completion

## Prerequisites

- Python 3.8+ 
- MySQL 8.0+ database server
- Minimum 4GB RAM (8GB recommended)
- 2GB free disk space

## Installation & Setup

### 1. Navigation & Setup
```bash
cd lab1_flights
mkdir -p logs data
```

### 2. Data Files
Place CSV files in `data/` directory (available from [Kaggle](https://www.kaggle.com/code/fabiendaniel/predicting-flight-delays-tutorial/input)):
- airlines.csv
- airports.csv  
- flights.csv

### 3. Environment Setup
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Configure Environment
In .env file following credencials should be replaced with yours:
- DATABASE_HOST
- DATABASE_PORT
- DATABASE_USER
- DATABASE_PASSWORD




### 5. Quick Start with Restart Script
For complete setup and testing:
```bash
chmod +x restart.sh
./restart.sh
```
This script handles database setup, schema creation, and runs a sample ETL process.

## Usage

### Basic Execution
```bash
# Create database schema
mysql -u username -pYourPassword < flights_db.sql

# Run complete ETL pipeline
python run_etl.py
```

### Command Line Options
```bash
# Basic execution
python run_etl.py

# Testing and development
python run_etl.py --sample-size 10000                    # Process limited records
python run_etl.py --skip-validation                      # Skip integrity validation

# State management (resumable processing)
python run_etl.py --show-state                           # Display current processing state
python run_etl.py --fresh-start                          # Clear state, start from beginning
python run_etl.py --reset-position --start-row 1000000   # Resume from specific row

# Help
python run_etl.py --help
```

## Pipeline Stages

### 1. **Data Extraction**
- Validates CSV file existence and structure
- **Memory Management**: True batch processing in 50K record batches
- Handles chunked processing for large datasets

### 2. **Data Transformation**
- **Dimensions**: Airlines categorization, airport size classification, business calendar
- **Facts**: Flight records with foreign key resolution and derived metrics
- **Cancellation Mapping**: 'A'→'Airline/Carrier', 'B'→'Weather', 'C'→'National Air System', 'D'→'Security'

### 3. **Data Loading**
- **Strategy**: Batch loading with transaction management
- **State Persistence**: Progress saved after each successful batch
- **Foreign Key Resolution**: Automatic lookup table generation

### 4. **Post-Load Operations**
- Creates optimized indexes for query performance
- Validates data integrity and referential constraints

## Monitoring & Logging

- **Console**: Real-time progress with batch-by-batch status
- **File**: `logs/etl_pipeline.log` (detailed execution log)
- **State File**: `etl_state.json` (resumable processing state)

## Analytical Capabilities

The dimensional model supports all eight analytical objectives with optimized queries:

### 1. **Carrier Performance Analysis**
```sql
SELECT 
    a.airline_name,
    COUNT(*) as total_flights,
    AVG(f.departure_delay_minutes) as avg_delay,
    ROUND(100.0 * SUM(CASE WHEN f.departure_delay_minutes <= 15 THEN 1 ELSE 0 END) / COUNT(*), 2) as on_time_pct
FROM fact_flights f
JOIN dim_airline a ON f.airline_key = a.airline_key
GROUP BY a.airline_name
ORDER BY avg_delay DESC;
```

### 2. **Airport Congestion Impact**
```sql
SELECT 
    ap.airport_name, 
    ap.city,
    ap.airport_size_category,
    COUNT(*) as flights, 
    AVG(f.departure_delay_minutes) as avg_delay
FROM fact_flights f
JOIN dim_airport ap ON f.origin_airport_key = ap.airport_key
WHERE f.departure_delay_minutes > 0
GROUP BY ap.airport_key, ap.airport_name, ap.city, ap.airport_size_category
ORDER BY avg_delay DESC;
```

### 3. **Temporal Pattern Analysis**
```sql
SELECT 
    d.season,
    t.hour_category,
    COUNT(*) as flights,
    AVG(f.departure_delay_minutes) as avg_delay
FROM fact_flights f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_time t ON f.departure_time_key = t.time_key
GROUP BY d.season, t.hour_category
ORDER BY avg_delay DESC;
```

### 4. **Delay Severity Analysis**
```sql
SELECT 
    f.delay_severity_category,
    COUNT(*) as flight_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM fact_flights f
GROUP BY f.delay_severity_category
ORDER BY flight_count DESC;
```

## File Structure

```
lab1_flights/
├── README.md                          # This documentation
├── requirements.txt                   # Python dependencies
├── .env                               # Environment configuration template
├── restart.sh                         # Quick setup and restart script
├── run_etl.py                         # Main ETL pipeline script
├── flights_db.sql                     # Database schema DDL
├── flights_views.sql                  # Analytical views
├── data/                              # Source CSV files
├── logs/                              # Execution logs (created at runtime)
├── src/                               # Source code
│   ├── config/
│   │   └── settings.py                # Configuration management
│   ├── extractors/
│   │   └── csv_extractor.py           # CSV data extraction with batch processing
│   ├── transformers/
│   │   ├── dimension_transformer.py   # Dimensional data transformation
│   │   └── fact_transformer.py        # Fact table transformation
│   ├── loaders/
│   │   └── mysql_loader.py            # MySQL database loading
│   └── utils/
│       ├── batch_state_manager.py     # Resumable processing state management
│       └── logging_config.py          # Logging configuration
└── etl_state.json                     # State management file (created at runtime)
```

## Troubleshooting

**Memory Issues**: Reduce `BATCH_SIZE` in `.env` file  
**Database Connection**: Check MySQL service and credentials in `.env`  
**Missing Files**: Ensure all CSV files are in `data/` directory  
**Processing Stuck**: Use `python run_etl.py --show-state` to check progress  

## Support

1. Check `logs/etl_pipeline.log` for detailed execution information
2. Use `python run_etl.py --show-state` for current processing status
3. Review configuration in `.env` file
4. Verify data files are accessible and properly formatted
