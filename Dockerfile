FROM apache/airflow:2.9.2-python3.12

USER root
RUN apt-get update && apt-get install -y gcc git

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/"

USER airflow

COPY rialto_airflow ./rialto_airflow
# COPY requirements.txt pyproject.toml ./

# RUN pip install -r requirements.txt
# RUN poetry build --format=wheel --no-interaction --no-ansi
# RUN pip install dist/*.whl
