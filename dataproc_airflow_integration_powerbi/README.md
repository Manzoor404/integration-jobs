# Daily COVID-19 ETL Pipeline with Google Cloud

This project demonstrates a daily ETL pipeline that processes COVID-19 data from Google Cloud Storage (GCS) and loads it into BigQuery using Dataproc for data transformation. The pipeline is orchestrated by Apache Airflow, and the data is visualized in GCP Looker Studio. The ETL job is triggered every day at 5:30 AM IST (12:00 AM UTC).

## Architecture

The pipeline follows this architecture:

![image](https://github.com/user-attachments/assets/8b02eeca-5e0d-4e92-8b9b-cde2a18e1679)


1. **Code Repository (GitHub)**: The PySpark ETL script is stored in GitHub.
2. **Jenkins**: Used to upload the ETL script to GCS.
3. **Airflow**: Orchestrates the ETL job on a daily schedule.
4. **Dataproc**: Executes the PySpark job to process the data from GCS.
5. **BigQuery**: Data is stored in BigQuery for querying and analysis.
6. **Looker Studio**: Visualizes the processed data through interactive dashboards.

## Steps Involved

1. **Data Extraction**:
    - Reads COVID-19 data from GCS in CSV format.
    - Cleans and processes the data to calculate total confirmed, deaths, and recovered cases.

2. **Data Transformation**:
    - Calculates daily growth rates and new confirmed cases.
    - Aggregates the data at both global and country levels.

3. **Data Loading**:
    - Writes the processed data back to GCS.
    - Loads the final datasets into BigQuery for further analysis and visualization.

4. **Google Looker Studio**:
    - Uses GCP Looker Studio to visualize the processed data from BigQuery, providing daily updated dashboards for insights.

5. **Airflow Scheduling**:
    - Airflow triggers the job daily at 5:30 AM IST, following the UTC 12:00 AM schedule.

6. **Jenkins**:
    - Jenkins is used to automate the uploading of the PySpark ETL script from GitHub to GCS.

## Code Overview

### Part 1: Data Extraction
- Reads the COVID-19 data from GCS using PySpark.
- Cleans up the `ObservationDate` field and calculates the earliest and latest dates in the dataset.

### Part 2: Data Transformation
- Filters the data to remove rows with `Confirmed`, `Deaths`, or `Recovered` values that are `0` or `null`.
- Aggregates the data to calculate total confirmed, deaths, and recovered cases.
- Joins the different aggregations and calculates daily growth rates and new confirmed cases.
- Filters the data to display the most recent date's information and aggregates at the country level.

### Part 3: Data Loading
- Saves the transformed data to GCS as CSV files.
- Loads the same data into BigQuery tables for querying and visualization.

### Part 4: Jenkins Pipeline (CI/CD)
- Jenkins fetches the latest PySpark ETL script from GitHub.
- Jenkins uploads the script to GCS, where it is executed by Dataproc.
- This ensures the ETL job uses the most up-to-date code each day.

### Part 5: Airflow Orchestration
- Airflow orchestrates the daily ETL process by triggering the Dataproc job that runs the PySpark script.
- The job is scheduled to run every day at 5:30 AM IST (12:00 AM UTC).

## How to Run

1. **Set up the Environment**:
    - Ensure Google Cloud SDK is installed.
    - Set up a Dataproc cluster and configure Google Cloud Storage.
    - Set up Jenkins with the necessary access to your GitHub repository and GCS.

2. **Run the ETL Job**:
    - The ETL job is scheduled automatically using Airflow. The PySpark script is stored in GCS and executed on Dataproc.

3. **Data Visualization**:
    - Use GCP Looker Studio to create a dashboard that visualizes the latest COVID-19 data from BigQuery.

## Code Summary

- The ETL job processes data daily, filtering rows, calculating growth rates, and saving results to both GCS and BigQuery.
- The job produces multiple outputs:
    - **Total active cases with growth rates**.
    - **Daily new confirmed cases**.
    - **Country-level aggregated data**.

## Output

- **BigQuery Tables**:
    - `covid_19_dataset.total_active_cases_with_growth_rates`
    - `covid_19_dataset.daily_new_confirmed_cases`
    - `covid_19_dataset.country_level_data`

- **Data Visualization**:
    - The data is visualized using GCP Looker Studio, updated daily with the latest available data.
