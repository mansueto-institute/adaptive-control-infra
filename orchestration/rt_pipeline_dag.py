import datetime
import json
import os

import requests
from airflow import models
from airflow.hooks.http_hook import HttpHook
from airflow.operators.http_operator import SimpleHttpOperator

AUDIENCE_ROOT = os.environ["GCF_URL"]
METADATA_ROOT = os.environ["METADATA"]

states = [
    'AP',
    'AR',
    'AS',
    'BR',
    'CT',
    'GA',
    'GJ',
    'HP',
    'HR',
    'JH',
    'KA',
    'KL',
    'LA',
    'LD',
    'MH',
    'ML',
    'MN',
    'MP',
    'MZ',
    'NL',
    'OR',
    'PB',
    'RJ',
    'SK',
    'TG',
    'TN',
    'TR',
    'UP',
    'UT',
    'WB',
]

class CloudFunction(SimpleHttpOperator):
    # see 
    # 1) https://github.com/salrashid123/composer_gcf
    # 2) https://github.com/salrashid123/composer_gcf/blob/master/composer_to_gcf/to_gcf.py
    def execute(self, context):
        token = requests.get(f"{METADATA_ROOT}{AUDIENCE_ROOT}/{self.endpoint}", headers = {"Metadata-Flavor": "Google"}).text
        HttpHook(self.method, http_conn_id = self.http_conn_id)\
            .run(self.endpoint, self.data, {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}, self.extra_options)

class CloudRun(SimpleHttpOperator):
    # see 
    # 1) https://github.com/salrashid123/composer_gcf
    # 2) https://github.com/salrashid123/composer_gcf/blob/master/composer_to_gcf/to_gcf.py
    # 3) https://cloud.google.com/run/docs/authenticating/service-to-service#python
    def execute(self, context):
        cloud_run_url = "https://get-twitter-images-sipjq3uhla-uc.a.run.app"
        token = requests.get(f"{METADATA_ROOT}{cloud_run_url}", headers = {"Metadata-Flavor": "Google"}).text
        HttpHook(self.method, http_conn_id = self.http_conn_id)\
            .run(self.endpoint, self.data, {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}, self.extra_options)

def epi_step(state):
    return CloudFunction(
        task_id      = f"epi_step_{state}",
        method       = "POST",
        endpoint     = "STEP_1_EST-get-state-Rt",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_functions",
        data         = json.dumps({"state_code": state})
    )

def create_Rt_report(state):
    return CloudRun(
        task_id      = f"create_report_{state}",
        method       = "GET",
        endpoint     = f"state/{state}",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_run_create_report",
        retries      = 3
    )

def tweet_Rt_report(state):
    return CloudFunction(
        task_id      = f"tweet_report_{state}",
        method       = "POST",
        endpoint     = "STEP_3_EXP-tweet-Rt-report",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_functions",
        data         = json.dumps({"state_code": state}),
        retries      = 3
    )

with models.DAG("Rt_pipeline", schedule_interval = "0 8 * * *", catchup = False) as dag:
    get_timeseries = CloudFunction(
        task_id      = "get_timeseries",
        method       = "POST",
        endpoint     = "STEP_0_RAW-get-state-case-timeseries",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_functions"
    )

    for state in states:
        get_timeseries          >>\
        epi_step(state)         >>\
        create_Rt_report(state) >>\
        tweet_Rt_report (state)
