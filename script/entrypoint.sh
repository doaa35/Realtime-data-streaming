#!/bin/bash

# -e :  if error occures exit, do not continue the sequence
set -e

# Update pip and install dependencies if requirements.txt is present
if [ -e "/var/lib/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi


# Initialize the Airflow database and create an admin user if the database file does not exist
if [ ! -f "/var/lib/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi


# Upgrade the Airflow database schem
$(command -v airflow) db upgrade



# Start the Airflow webserver
exec airflow webserver
