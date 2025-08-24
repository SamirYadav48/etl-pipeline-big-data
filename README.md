# etl-pipeline-big-data
End-to-end ETL pipeline using PySpark and PostgreSQL to process large-scale booking data, enabling analytics and interactive dashboards.
Ride Bookings ETL Pipeline – User Manual
1. Project Overview
  This project is an End-to-End ETL (Extract, Transform, Load) pipeline built with Python, PySpark, and PostgreSQL. It processes large-scale    ride booking data, cleans it, transforms it, and loads it into a database. The pipeline also includes an interactive Excel dashboard that     hows key metrics (KPIs) like Total Bookings, Revenue, Cancellation Rate, Average Ratings, and Ride Distance.
  Key Features:
    Handles large datasets efficiently using PySpark.
    Extracts raw data from CSV files.
    Cleans and transforms data for analysis.
    Loads processed data into PostgreSQL.
    Generates pivot tables and KPI-ready data for dashboards.
    Interactive Excel dashboard with slicers to filter by Date, Vehicle Type, and Booking Status.
2. Prerequisites
   Before running the ETL, you need:
    Python 3.x installed.
    Download Python
    PySpark
    pip install pyspark
    PostgreSQL
    Install and create a database.
    Download PostgreSQL
    psycopg2 (Python PostgreSQL connector)
    pip install psycopg2
    Pandas (for Excel export)
    pip install pandas
    Microsoft Excel (or Google Sheets) to open the dashboard.
3. Folder Structure
ride-bookings-etl-pipeline/
│── README.md                  # This manual
│── requirements.txt           # Required Python libraries
│── extract/                   # Extract scripts
│   └── extract_data.py
│── transform/                 # Data cleaning & transformation
│   └── transform_data.py
│── load/                      # Load to PostgreSQL
│   └── load_data.py
│── data/                      # Sample CSV datasets
│── notebooks/                 # Optional Jupyter notebooks
│── dashboard/                 # Excel dashboard file
4. How to Download and Run
  Clone the repository from GitHub
    git clone https://github.com/yourusername/ride-bookings-etl-pipeline.git
  Navigate to the project folder
    cd ride-bookings-etl-pipeline
  Install dependencies
    pip install -r requirements.txt
  Set up PostgreSQL database
    Create a database (e.g., ride_bookings_db).
  Update load/load_data.py with your database credentials (host, port, username, password).
    Run the ETL scripts
    Step 1: Extract Data
      python extract/extract_data.py
      This script reads CSV files from the data/ folder and saves cleaned raw files for transformation.

<img width="1252" height="846" alt="data ectraction" src="https://github.com/user-attachments/assets/cf27d28c-2133-4bb8-ae5c-ccf899378e89" />


   Step 2: Transform Data
      python transform/transform_data.py
      Cleans missing/null values.
      Converts date/time columns to proper format.
      Calculates metrics like Average Ride Time, Average Booking Value.
      Standardizes text fields (Vehicle Type, Booking Status, Locations).
    Step 3: Load Data
      python load/load_data.py
      Loads transformed data into PostgreSQL tables.
      Creates tables if they don’t exist.
      Populates all required fields for dashboard and analysis.
      Open Dashboard
      Open dashboard/Ride_Bookings_Dashboard.xlsx.
      Use slicers to filter by:
      Date (day/week/month)
      Vehicle Type
      Booking Status
      KPIs and pivot tables update automatically based on your filters.
6. ETL Workflow Explanation
    Step 1: Extract
      Reads raw CSV files from the data/ folder.
        Handles multiple files if present.
      Ensures all columns are loaded correctly with proper data types.
      Saves a temporary “cleaned raw” file for transformation.
    Step 2: Transform
      Data Cleaning:
      Removes rows with missing or invalid values.
      Standardizes text fields (Vehicle Type, Status, Locations).
      Feature Engineering:
      Calculates Avg VTAT, Avg CTAT, and Ride Distance.
      Generates numeric columns for KPIs: Revenue, Completed Rides, Cancelled Rides.
      Formatting:
      Converts Date and Time to datetime format.
      Prepares data types for PostgreSQL (int, float, varchar).
    <img width="1252" height="846" alt="Screenshot 2025-08-19 at 1 36 12 pm" src="https://github.com/user-attachments/assets/34451b5d-142b-4911-abd7-13451a76ed15" />

    
    Step 3: Load
      Connects to PostgreSQL using psycopg2.
      Checks if tables exist; creates them if missing.
      Inserts transformed data row by row or in batches.
      Ensures referential integrity (primary keys, unique IDs).
    Step 4: Dashboard Preparation
      Pivot tables summarize data for KPIs:
      Total Bookings, Total Revenue, Completed %, Cancelled %, Average Ratings, Total Distance.
      Excel dashboard connects to these pivot tables.
      Slicers allow interactive filtering to explore different aspects of the data.
    6. Notes for Non-Technical Users
      No need to modify scripts unless your database credentials change.
      Always keep CSV files in the data/ folder.
      Pivot tables in Excel will update automatically if the underlying database is refreshed.

<img width="849" height="515" alt="Screenshot 2025-08-24 at 5 45 22 pm" src="https://github.com/user-attachments/assets/8dda6ed9-3e2c-47b3-98a1-a2f1f00e6a19" />
      
      
