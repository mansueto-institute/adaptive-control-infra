import datetime
import json
import os

import requests
from airflow import models
from airflow.hooks.http_hook import HttpHook
from airflow.operators.http_operator import SimpleHttpOperator

AUDIENCE_ROOT = os.environ["GCF_URL"]
METADATA_ROOT = os.environ["METADATA"]

states = ["MH", "PB", "TN", "KA", "WB"]

class ComputeEndpointOperator(SimpleHttpOperator):
    # see 
    # 1) https://github.com/salrashid123/composer_gcf
    # 2) https://github.com/salrashid123/composer_gcf/blob/master/composer_to_gcf/to_gcf.py
    def execute(self, context):
        token = requests.get(f"{METADATA_ROOT}{AUDIENCE_ROOT}/{self.endpoint}", headers = {"Metadata-Flavor": "Google"}).text
        HttpHook(self.method, http_conn_id = self.http_conn_id)\
            .run(self.endpoint, self.data, {"Authorization": f"Bearer {token}"}, self.extra_options)

def epi_step(state):
    return ComputeEndpointOperator(
        task_id      = f"epi_step_{state}",
        method       = "POST",
        endpoint     = "STEP_1_EST-get-state-Rt",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_functions",
        data         = {"state_code": state}
    )

def create_Rt_report(state):
    return ComputeEndpointOperator(
        task_id      = f"create_report_{state}",
        method       = "POST",
        endpoint     = f"state/{state}",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_run_create_report"
    )

def tweet_Rt_report(state):
    return ComputeEndpointOperator(
        task_id      = f"tweet_report_{state}",
        method       = "POST",
        endpoint     = "STEP_3_EXP-tweet-Rt-report",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_functions",
        data         = {"state_code": state}
    )

with models.DAG("Rt_pipeline", schedule_interval = datetime.timedelta(days = 1), catchup = False) as dag:
    get_timeseries = ComputeEndpointOperator(
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
