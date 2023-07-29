# Dockerfile
FROM apache/airflow:2.3.3-python3.8

USER airflow
RUN pip install psycopg2-binary SQLAlchemy pandas pytrends datetime
#RUN touch /opt/airflow/__init__.py