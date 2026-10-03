FROM apache/airflow:2.10.4

USER airflow
RUN pip install --no-cache-dir "dbt-core==1.12.5" "dbt-snowflake==1.12.1" snowflake-connector-python   

#Lesson: dbt-core and dbt-snowflake minor versions don't have to match, but they need to be from the same #release track. 1.11 core + 1.12 adapter = broken. 1.12 core + 1.12 adapter = fine.
