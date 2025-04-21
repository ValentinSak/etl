# ETL Project

This project implements an ETL pipeline using Apache Airflow to process and store various types of events (orders, products, stores, and sales) in a PostgreSQL database.
It also contains simple slack analytical alerts example (analyze store sales data and sends alerts for stores with significant sales decreases)

## Project Structure

```
.
├── airflow_src/                 # Airflow setup
│   ├── dags/                    # Airflow DAGs
│   │   └── etl/                 # ETL-specific DAGs
│   │   └── alerts/              # alert-specific DAGs
│   └── lib/                     # Shared library code
│       ├── messangers/          # Message sending utilities
│       └── ...                  # Other utility modules
├── etl_db/                      # ETL database setup
│   ├── initialize_db/           # Database initialization scripts
│   ├── import_files/            # Sample data files
│   ├── sql_scripts/             # SQL scripts
│   └── postgres_etl_data/       # Database data directory
├── shared_data_S3_replacement/  # Shared directory for file exchange
└── docker-compose.yml           # Docker configuration
```

## Components

1. **PostgreSQL Databases**:
   - `postgres`: Airflow metadata database
   - `postgres_etl`: Main ETL database for storing events

2. **Airflow Services**:
   - Airflow Webserver
   - Airflow Scheduler

3. **Event Types**:
   - Order events
   - Product events
   - Store events
   - Sales events

## Setup

1. **Environment Variables**:
   Create a `.env` file with the following variables:
   ```
   AIRFLOW_DB_USER=paste_your_value_here
   AIRFLOW_DB_PASSWORD=paste_your_value_here
   AIRFLOW_DB_NAME=paste_your_value_here
   AIRFLOW_DB_HOST=paste_your_value_here
   AIRFLOW_DB_PORT=5432

   AIRFLOW_SECRET_KEY=paste_your_value_here
   AIRFLOW_USER=paste_your_value_here
   AIRFLOW_USER_PASSWORD=paste_your_value_here

   ETL_DB_USER=paste_your_value_here
   ETL_DB_PASSWORD=paste_your_value_here
   ETL_DB_NAME=paste_your_value_here
   ETL_DB_HOST=paste_your_value_here
   ETL_DB_PORT=5432
   ```

2. **Start Services**:
   ```bash
   docker-compose up -d
   ```

3. **Access Airflow UI**:
   - Open `http://localhost:8080`
   - Login with credentials from `.env`

## ETL Flow

1. **Event Generation**:
   - Events are generated using `generate_events.py`
   - Each event type has its own generation function
   - Events are created with random data

2. **Event Storage**:
   - Events are written to CSV files in the shared directory
   - Each batch of events gets a unique batch ID and equals dag context['run_id']
   - Files include metadata like creation timestamp

3. **Event Processing**:
   - Airflow DAGs monitor the shared directory
   - New files are processed and loaded into the database
   - Processed files are tracked to avoid duplicates

4. **Monitoring and Alerts**:
   - Sales alerts are generated for significant changes
   - Alerts can be sent via Slack
   - Configurable thresholds for alert triggers

## Assumptions and Simplifications

   - Database credentials are stored in `.env` file
   - Single instance of each service 
   - Basic error logging