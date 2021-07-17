"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import requests
import psycopg2
from requests.auth import HTTPBasicAuth

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 16),
    "schedule_interval": None,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("rohit", default_args=default_args, schedule_interval=timedelta(1))

def insert_data():
    cur.execute('''

    ''')

def github_star_count():


    # API CALL
    github_repo_name  = Variable.get("repo_name")
    username = Variable.get("username")
    password = Variable.get("password")
    r = requests.get('https://api.github.com/repos/'+ github_repo_name, auth = HTTPBasicAuth(username, password)).json()
    #r = requests.get('https://api.github.com/repos/'+ github_repo_name).json()
    print("stargazers_count: ",r["stargazers_count"])
    print("watchers_count: ",r["watchers_count"])
    print("forks: ",r["forks"])
    star_count = int(r["stargazers_count"])

    # Database Ingestion
    con = psycopg2.connect(database="airflow", user="airflow", password="airflow", host="postgres", port="5432")
    cur = con.cursor()
    #cur = con.cursor()
    #cur.execute('create table star_count(repo_name varchar(256), star_count bigint, entry_date DATE NOT NULL DEFAULT CURRENT_DATE)')
    #con.commit()
    cur.execute("INSERT INTO star_count (repo_name, star_count) VALUES(%s, %s)", (github_repo_name, str(star_count)))
    con.commit()
    cur.close()
    return (str,200)


t1 = PythonOperator(
    task_id='GithubStarCount',
    python_callable= my_function,
    #op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)
