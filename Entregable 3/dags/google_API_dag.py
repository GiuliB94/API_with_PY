from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta,datetime, date
import pandas as pd
from dateutil.relativedelta import relativedelta
from pytrends.request import TrendReq
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import extras
import os
from Python.google_API import cargar_data_region, conectar_API_Region, cargar_data_fecha, conectar_API_Fecha

default_args={
    'owner': 'GiulianaB',
    'retries':5,
    'retry_delay': timedelta(minutes=3)
}


with DAG(
    default_args=default_args,
    dag_id='mi_primer_dar_con_PythonOperator',
    description= 'Nuestro primer dag usando python Operator',
    schedule_interval='@daily'
    ) as dag:
    task1= cargar_data_region(conectar_API_Region())
    task2= cargar_data_fecha(conectar_API_Fecha())

    task1>>task2