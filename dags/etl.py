from datetime import timedelta, datetime
import pandas as pd
import json
import os
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator

TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (job_id) REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (job_id) REFERENCES job(id)
)
"""

@task()
def extract():
    source_path="../source"
    target_path="..staging/etracted"
    df = pd.read_csv(os.path.join(source_path, 'jobs.csv'))
    context_data = df['context']
        
    for index, data in enumerate(context_data):
        with open(os.path.join(target_path, f'extracted_{index}.txt'), 'w') as file:
            file.write(str(data))

@task()
def transform():
    source_path="../source"
    target_path="..staging/etracted"
    for filename in os.listdir(source_path):
        if filename.endswith(".txt"):
            with open(os.path.join(source_path, filename), 'r') as file:
                data = json.loads(file.read())

                transformed_data = {
                    "job": {
                        "title": data.get("job_title", ""),
                        "industry": data.get("job_industry", ""),
                        "description": data.get("job_description", ""),
                        "employment_type": data.get("job_employment_type", ""),
                        "date_posted": data.get("job_date_posted", ""),
                    },
                    "company": {
                        "name": data.get("company_name", ""),
                        "link": data.get("company_linkedin_link", ""),
                    },
                    "education": {
                        "required_credential": data.get("job_required_credential", ""),
                    },
                    "experience": {
                        "months_of_experience": data.get("job_months_of_experience", ""),
                        "seniority_level": data.get("seniority_level", ""),
                    },
                    "salary": {
                        "currency": data.get("salary_currency", ""),
                        "min_value": data.get("salary_min_value", ""),
                        "max_value": data.get("salary_max_value", ""),
                        "unit": data.get("salary_unit", ""),
                    },
                    "location": {
                        "country": data.get("country", ""),
                        "locality": data.get("locality", ""),
                        "region": data.get("region", ""),
                        "postal_code": data.get("postal_code", ""),
                        "street_address": data.get("street_address", ""),
                        "latitude": data.get("latitude", ""),
                        "longitude": data.get("longitude", ""),
                    },
                }

                with open(os.path.join(target_path, f'transformed_{filename}'), 'w') as transformed_file:
                    json.dump(transformed_data, transformed_file, indent=2)

@task()
def load():
    source_path="../staging/transformed"
    sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_default')
    conn = sqlite_hook.get_conn()
    cursor = conn.cursor()

    for filename in os.listdir(source_path):
        if filename.endswith(".json"):
            with open(os.path.join(source_path, filename), 'r') as file:
                data = json.load(file)
                cursor.execute("INSERT INTO jobs_table (data) VALUES (?)", (json.dumps(data),))

    conn.commit()
    conn.close()


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}

@dag(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)

def etl_dag():
    create_tables = SqliteOperator(
        task_id="create_tables",
        sqlite_conn_id="sqlite_default",
        sql=TABLES_CREATION_QUERY
    )

    create_tables >> extract() >> transform() >> load()

etl_dag()
