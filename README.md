# Multinational Retail Data Centralisation

## Description

This project is aimed at centralizing sales data for a multinational company. The goal is to create a system that consolidates data from various sources into a single database, making it accessible and analyzable from one central location. This will serve as a single source of truth for all sales data.

**What I Learned:**
- Integrating data from multiple sources
- Using Python for data extraction, cleaning, and uploading
- Working with Pandas, Requests, and Boto3
- Working with postgres, a few AWS concepts 
- WSL networking quirks (not recognising Windows + subsequent difficulties uploading to the local database)
- Star based schemas and SQL
## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/username/repository.git
   ```

2. **Navigate to the project directory:**
   ```bash
   cd path/to/my/project
   ```


## Usage

1. **Prepare configuration files:**
   - Create `db_creds.yaml` and `local_db_creds.yaml` for database connections

2. **Run the main script:**
   ```bash
   python main.py
   ```

   This script performs the following tasks:
   - Extracts data from various sources (e.g., PDFs, APIs, S3)
   - Cleans and processes the data
   - Uploads cleaned data to a local database

3. **Run database.sql**
   Use your preferred database tool.

## File Structure

- `main.py`: Main script that coordinates data extraction, cleaning, and uploading
- `data_cleaning.py`: Contains data cleaning functions for different datasets
- `database_utils.py`: Functions for database connection and operations
- `data_extraction.py`: Functions for extracting data from various sources
- `requirements.txt`: Lists Python dependencies required for the project
- `README.md`: This file, providing an overview of the project

## License

MIT License