import datetime
import json
import os

import requests
from airflow import models
from airflow.models.connection import Connection
from airflow.hooks.http_hook import HttpHook
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator

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
    # 3) https://cloud.google.com/run/docs/authenticating/service-to-service#python

    def get_metadata_url(self):
        return f"{METADATA_ROOT}{AUDIENCE_ROOT}/{self.endpoint}"
    
    def execute(self, context):
        token = requests.get(self.get_metadata_url(), headers = {"Metadata-Flavor": "Google"}).text
        HttpHook(self.method, http_conn_id = self.http_conn_id)\
            .run(self.endpoint, self.data, {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}, self.extra_options)

class CloudRun(CloudFunction):
    def __init__(self, run_url, conn_id, *args, **kwargs):
        conn = Connection(
            conn_id   = conn_id,
            conn_type = "http",
            host      = run_url,
            schema    = "https"
        )
        kwargs["http_conn_id"] = conn_id
        super(CloudRun, self).__init__(*args, **kwargs)
        self.run_url = run_url

    def get_token(self):
        return f"{METADATA_ROOT}{self.run_url}"

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
        run_url      = "get-twitter-images-sipjq3uhla-uc.a.run.app",
        conn_id      = "cloud_run_create_report",
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

def simulation_initial_conditions(state):
    return DummyOperator(task_id = f"simulation_initial_conditions_{state}", 
    start_date   = datetime.datetime(2021, 4, 29))
    # return CloudFunction(
    #     task_id      = f"simulation_initial_conditions_{state}",
    #     method       = "POST",
    #     endpoint     = "STEP_3_EXP-tweet-Rt-report",
    #     start_date   = datetime.datetime(2021, 4, 29),
    #     http_conn_id = "cloud_functions",
    #     data         = json.dumps({"state_code": state}),
    #     retries      = 3
    # )

def simulation_step(state):
    return DummyOperator(task_id = f"simulation_step_{state}",
    start_date   = datetime.datetime(2021, 4, 29))
    # return CloudFunction(
    #     task_id      = f"simulation_step_{state}",
    #     method       = "POST",
    #     endpoint     = "STEP_3_EXP-tweet-Rt-report",
    #     start_date   = datetime.datetime(2021, 4, 29),
    #     http_conn_id = "cloud_functions",
    #     data         = json.dumps({"state_code": state}),
    #     retries      = 3
    # )

def get_dag(name: str, tweet: bool) -> models.DAG:
    with models.DAG(name, schedule_interval = "45 8 * * *", catchup = False) as dag:
        get_timeseries = CloudFunction(
            task_id      = "get_timeseries",
            method       = "POST",
            endpoint     = "STEP_0_RAW-get-state-case-timeseries",
            start_date   = datetime.datetime(2021, 4, 29),
            http_conn_id = "cloud_functions"
        )

        get_vax_data = CloudFunction(
            task_id      = "get_vax_data",
            method       = "POST",
            endpoint     = "STEP_0_RAW-get-vax-data",
            start_date   = datetime.datetime(2021, 4, 29),
            http_conn_id = "cloud_functions"
        )

        fanout = DummyOperator(task_id = "fanout", start_date = datetime.datetime(2021, 4, 29))

        [get_timeseries, get_vax_data] >> fanout

        for state in states:
            epi_step_for_state = epi_step(state)
            fanout >> epi_step_for_state >> simulation_initial_conditions(state) >> simulation_step(state)
            if tweet:
                epi_step_for_state >> create_Rt_report(state) >> tweet_Rt_report(state)
           
        return dag 

rt_pipeline          = get_dag("Rt_pipeline",          tweet = True)
rt_pipeline_no_tweet = get_dag("Rt_pipeline_no_tweet", tweet = False)