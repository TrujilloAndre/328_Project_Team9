# Chicago Food Inspections ETL Pipeline and Dashboard

## Overview

This repository contains a Dockerized ETL pipeline and an interactive Streamlit dashboard to process and visualize restaurant inspection data from the City of Chicago Open Data API.

## Features

* **Extraction**: Fetch raw inspection records in batches from the Chicago Data API using `extract.py`.
* **Transformation**: Clean, standardize, and deduplicate data with `transform.py`.
* **Loading**: Load processed data into a PostgreSQL database via `load.py`.
* **Orchestration**: Automate the full pipeline with `run_etl.sh` and Docker Compose.
* **Visualization**: Interactive dashboard built with Streamlit (`streamlit/app.py`).

## Prerequisites

* Docker & Docker Compose
* Python 3.8+ (if running locally)
* PostgreSQL (if running locally)

## Getting Started

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd 328Project_9
   ```
2. Copy the example environment file and update variables:

   ```bash
   cp .env.example .env
   ```
3. Edit `.env` with your credentials and API endpoint:

   ```text
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=admin
   POSTGRES_NAME=ChicagoDB
   POSTGRES_HOST=db
   POSTGRES_PORT=5432
   DATABASE_URL=postgresql://postgres:admin@db:5432/ChicagoDB
   POSTGRES_DB=ChicagoDB
   API_KEY=https://data.cityofchicago.org/resource/4ijn-s7e5.json
   ```
4. Install Python dependencies (local setup):

   ```bash
   pip install -r requirements.txt
   ```

### Local Execution

Run the ETL pipeline:

```bash
bash run_etl.sh
```

Launch the dashboard:

```bash
cd streamlit
streamlit run app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

### Dockerized Execution

Start all services in one command:

```bash
docker-compose up --build
```

* **db**: PostgreSQL database (exposed on host port 5433)
* **etl**: ETL processing service
* **streamlit**: Streamlit dashboard service

Visit [http://localhost:8501](http://localhost:8501) once the services are up.

## Project Structure

```
├── extract.py            # Data extraction script
├── transform.py          # Data cleaning & transformation
├── load.py               # Database loading script
├── run_etl.sh            # ETL orchestration script
├── requirements.txt      # Python dependencies
├── Dockerfile            # ETL service Dockerfile
├── docker-compose.yml    # Service definitions
├── .env                  # Environment variables
└── streamlit/
    ├── app.py            # Streamlit dashboard
    └── Dockerfile        # Dashboard service Dockerfile
```

## Database Schema

* **Facility**: Stores facility metadata (license ID, names, type, location).
* **Inspections**: Records inspection events (inspection ID, date, type, results).
* **Violations**: Catalogs unique violation codes and descriptions.

See `load.py` for the full DDL statements.

## Contributing

1. Fork the repository.
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes and push: `git push origin feature/your-feature`
4. Open a Pull Request and await review.

## License

This project is licensed under the MIT License.

## Acknowledgments

* City of Chicago Open Data Portal for providing inspection data.
* Streamlit, Pandas, SQLAlchemy, and other open-source libraries.
* All of the work on this was splitted evenly,
    * Maddie: Group Leader, handled cleaning, transforming, and loading the data as well as making sure everything works.
    * Andre: Handled docker, cleaning, and finalizing everything.
    * Gaurav: Handled extraction, docker, and cleaning.
    * John: Handled cleaning and the majority of streamlit.
