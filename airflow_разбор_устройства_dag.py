#!/usr/bin/env python
# coding: utf-8

import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# Подгружаем данные
TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


# Таски

# читаем файл
def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index = False)
    
# сохраняем
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)

        
# топ-10 доменных зон по численности доменов
def top_10_zone():
    data = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    data['domain_zone'] = data.domain.str.split('.').str.get(-1)
    top_10_zone = data.groupby('domain_zone', as_index = False) \
                                    .agg({'domain' : 'count'}) \
                                    .sort_values('domain', ascending = False) \
                                    .head(10)
    with open('top_10_zone.csv', 'w') as f:
        f.write(top_10_zone.to_csv(index = False, header = False))


# домен с самым длинным именем
def max_length_name():
    data = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    data['domain_name_length'] = data.domain.str.split('.').str.get(0).apply(lambda x: len(x))
    max_length_name = data.sort_values(['domain_name_length','domain'], ascending =(False, True)).head(1)        
    with open('max_length_name.txt', 'w') as f:
        f.write(str(max_length_name))
     
    
# на каком месте находится домен airflow.com
def airflow_rank():
    data = pd.read_csv(TOP_1M_DOMAINS_FILE, names = ['rank', 'domain'])
    airflow_rank = data.query('domain == "airflow.com"')['rank'].values[0]
    with open('airflow_rank.txt', 'w') as f:
        f.write(str(airflow_rank))
        
        
# передаем переменные airflow
def print_data(ds): 
    with open('top_10_zone.csv', 'r') as f:
        top_10_zone = f.read()
    with open('max_length_name.txt', 'r') as f:
        max_length_name = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()
    date = ds

    print(f'Top 10 domain zone for date {date}')
    print(top_10_zone)

    print(f'Maximum length of domain name for date {date}')
    print(max_length_name)

    print(f'Rank of domain "airflow.com" for date {date}')
    print(airflow_rank)


# Инициализируем DAG

default_args = {
    'owner': 'a-eremina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 8),
    }
schedule_interval = '30 13 * * *'
dag = DAG('a_eremina_lesson_2', default_args=default_args, schedule_interval=schedule_interval)


# Инициализируем таски

t1 = PythonOperator(task_id = 'get_data',
                    python_callable = get_data,
                    dag = dag)

t2 = PythonOperator(task_id = 'top_10_zone',
                    python_callable = top_10_zone,
                    dag = dag)

t3 = PythonOperator(task_id = 'max_length_name',
                    python_callable = max_length_name,
                    dag = dag)

t4 = PythonOperator(task_id = 'airflow_rank',
                    python_callable = airflow_rank,
                    dag = dag)

t5 = PythonOperator(task_id = 'print_data',
                    python_callable = print_data,
                    dag = dag)


# Задаем порядок выполнения

t1 >> [t2, t3, t4] >> t5




