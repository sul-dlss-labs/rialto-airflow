# rialto-airflow
Airflow for harvesting data for open access analysis and research intelligence.

## Running Locally with Docker

Based on the documentation, [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html).

1. Clone repository `git clone https://github.com/sul-dlss/rialto-airflow.git`

2. Start up docker locally.

3. Create a `.env` file with the `AIRFLOW_UID` and `AIRFLOW_GROUP` values. For local development these can usually be:
```
 AIRFLOW_UID=50000
 AIRFLOW_GROUP=0
 ```
(See [Airflow docs](https://airflow.apache.org/docs/apache-airflow/2.9.2/howto/docker-compose/index.html#setting-the-right-airflow-user) for more info.)

4. Add to the `.env` values for any environment variables used by DAGs. Not in place yet--they will usually applied to VMs by puppet once productionized.

These environment variables must be prefixed with `AIRFLOW_VAR_` to be accessible to DAGs. (See [Airflow env var documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html#storing-variables-in-environment-variables and `docker-compose.yml`).) They can have placeholder values. The secrets will be in vault, not prefixed by `AIRFLOW_VAR_`: `vault kv list puppet/application/rialto_airflow/{env}`.

  Example script to quickly populate your .env file for dev:
  ```
  for i in `vault kv list puppet/application/rialto_airflow/dev`; do val=$(echo $i| tr '[a-z]' '[A-Z]'); echo AIRFLOW_VAR_$val=`vault kv get -field=content puppet/application/rialto_airflow/dev/$i`; done
  ```