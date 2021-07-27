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
from star_count_helper.star_count_helper import star_count_helper

    
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

git_conf = {
    'github_repo_name' : Variable.get("repo_name"),
    'username' : Variable.get("username"),
    'password' : Variable.get("password")
} 
#git_conf

db_conf = {
    'db_name' : Variable.get("db_name"),
    'db_user' : Variable.get("db_user"),
    'db_password'  : Variable.get("db_password"),
    'db_host'  : Variable.get("db_host"),
    'db_port'  : Variable.get("db_port")    
}

# execution starts here
def github_star_count():

    # API CALL
    r = star_count_helper.call_api(git_conf)
    #r = requests.get('https://api.github.com/repos/'+ github_repo_name, auth = HTTPBasicAuth(username, password)).json()
    #r = requests.get('https://api.github.com/repos/'+ github_repo_name).json()
    print("stargazers_count: ",r["stargazers_count"])
    print("watchers_count: ",r["watchers_count"])
    print("forks: ",r["forks"])
    star_count = int(r["stargazers_count"])

    # Ingest data to DB 
    star_count_helper.insert_data(db_conf, git_conf.get('github_repo_name'), star_count)

    return (str,200)


t1 = PythonOperator(
    task_id='GithubStarCount',
    python_callable= github_star_count,
    #op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)
