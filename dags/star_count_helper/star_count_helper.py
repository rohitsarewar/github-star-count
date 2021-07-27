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


class star_count_helper:

    def call_api(git_var):

        # Dict - git_var 
        github_repo_name = git_var.get('github_repo_name')
        username = git_var.get('username')
        password = git_var.get('password') 
    
        try:
            r = requests.get('https://api.github.com/repos/'+ github_repo_name, auth = HTTPBasicAuth(username, password)).json()
            #r = requests.get('https://api.github.com/repos/'+ github_repo_name).json()
            print("stargazers_count: ",r["stargazers_count"])
            print("watchers_count: ",r["watchers_count"])
            print("forks: ",r["forks"])
            star_count = int(r["stargazers_count"])
            print('insert into star_count ("repo_name","star_count") values ('+github_repo_name+','+str(star_count)+')')
            #r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)  # will print - 404 Client Error: Not Found for url:
    
        return r


    def insert_data(db_conf, github_repo_name, star_count):
    
        # Database Connection    
        con = psycopg2.connect(
            database=db_conf.get('db_name'),
            user=db_conf.get('db_user'), 
            password=db_conf.get('db_password'), 
            host=db_conf.get('db_host'), 
            port=db_conf.get('db_port')
        )
    
        cur = con.cursor()
        # create table during first execution
        #cur.execute('create table if not exists star_count(repo_name varchar(256), star_count bigint, entry_date DATE NOT NULL DEFAULT CURRENT_DATE)')
        #con.commit()
        cur.execute("INSERT INTO star_count (repo_name, star_count) VALUES(%s, %s)", (github_repo_name, str(star_count)))
        con.commit()
        cur.close()
        return 