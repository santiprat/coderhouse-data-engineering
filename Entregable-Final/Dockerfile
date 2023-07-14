# Dockerfile v1.0
FROM apache/airflow:2.6.3

# Copio los archivos de DAG, logs y plugins al directorio correspondiente de Airflow
COPY ./dags /opt/airflow/dags
COPY ./logs /opt/airflow/logs
COPY ./plugins /opt/airflow/plugins

# Inicializo la base de datos de Airflow y creo el usuario Admin
RUN airflow db init && airflow users create --username airflow --firstname airflow --lastname airflow --email airflow@airflow --role Admin --password airflow

# Inicio el webserver y scheduler de Airflow
CMD ["bash", "-c", "airflow webserver -p 8080 && airflow scheduler"]