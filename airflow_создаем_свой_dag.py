import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable


year = 1994 + hash(f'a-eremina') % 23
SALES = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'



default_args = {
    'owner': 'a-eremina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 11, 10),
    }
schedule_interval = '15 14 * * *'



CHAT_ID = -1002018009780
try:
    BOT_TOKEN = Variable.get('5927447872:AAEw1XHA7QZ0YqLWlZRueQk-3vSnoGojoQA')
except:
    BOT_TOKEN = ''



def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'It\'s working! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, text=message)
    else:
        pass

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def a_eremina_lesson_3():
    @task(retries = 3)
    def get_data():
        sales = pd.read_csv(SALES).query('Year == @year')
        return sales

#  1. Какая игра была самой продаваемой в этом году во всем мире?
    @task(retries = 4, retry_delay = timedelta(10))
    def top_game(sales):
        task1 = sales.groupby('Name', as_index = False).agg({'Global_Sales':'sum'}) \
                     .sort_values('Global_Sales', ascending = False) \
                     .query('Global_Sales == Global_Sales.max()').Name.to_list()
        return task1
    
# 2. Игры какого жанра были самыми продаваемыми в Европе? Перечислить все, если их несколько
    @task(retries = 4, retry_delay = timedelta(10))
    def top_eu_genre(sales):
        task2 = sales.groupby('Genre', as_index = False).agg({'EU_Sales':'sum'}).sort_values('EU_Sales', ascending = False) \
                     .query('EU_Sales == EU_Sales.max()').Genre.to_list()
        return task2

# 3. На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке? 
# Перечислить все, если их несколько
    @task(retries = 4, retry_delay = timedelta(10))
    def top_na_platform(sales):
        task3 = sales.query('NA_Sales > 1').groupby('Platform', as_index = False) \
                     .agg({'Name':'nunique'}).sort_values('Name', ascending = False) \
                     .query('Name == Name.max()').Platform.to_list()
        return task3

# 4. У какого издателя самые высокие средние продажи в Японии? Перечислить все, если их несколько
    @task(retries = 4, retry_delay = timedelta(10))
    def top_jp_publisher(sales):
        task4 = sales.groupby('Publisher', as_index = False).agg({'JP_Sales':'mean'}) \
                     .sort_values('JP_Sales', ascending = False) \
                     .query('JP_Sales == JP_Sales.max()').Publisher.to_list()
        return task4

# 5. Сколько игр продались лучше в Европе, чем в Японии?
    @task(retries = 4, retry_delay = timedelta(10))
    def count_top_eu_games(sales):
        task5 = sales.query('EU_Sales > JP_Sales').Name.nunique()
        return task5

    @task(retries = 4, retry_delay = timedelta(10), on_success_callback = send_message)
    def print_data(task1, task2, task3, task4, task5):
        context = get_current_context()
        date = context['ds']
        print(f'For {date}:')
        print(f'Top sale game in {year}: {task1}')
        print(f'Top sale genre/genres in Europe in {year}: {task2}')
        print(f'Top sale platform/platforms in Nothern America in {year}: {task3}')
        print(f'Top sale publisher/publishers in Japan in {year}: {task4}')
        print(f'Count of top sale games in Europe compared to Japan: {task5}')   

    sales = get_data()
    task1 = top_game(sales)
    task2 = top_eu_genre(sales)
    task3 = top_na_platform(sales)
    task4 = top_jp_publisher(sales)
    task5 = count_top_eu_games(sales)
    
    print_data(task1, task2, task3, task4, task5)

a_eremina_lesson_3 = a_eremina_lesson_3()
