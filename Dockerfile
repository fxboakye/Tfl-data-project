FROM python:3.9.9-slim-buster

RUN apt-get update \
    && apt-get install -y --no-install-recommends

# Install the dbt Postgres adapter. This step will also install dbt-core
RUN pip install --upgrade pip
RUN pip install dbt-bigquery==1.0.0
#Test Connection
ENTRYPOINT ["dbt"]