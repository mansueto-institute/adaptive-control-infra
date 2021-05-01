import datetime
import os

import requests
from airflow import models
from airflow.hooks.http_hook import HttpHook
from airflow.operators.http_operator import SimpleHttpOperator

AUDIENCE_ROOT = os.environ["GCF_URL"]
METADATA_ROOT = os.environ["METADATA"]

class ComputeEndpointOperator(SimpleHttpOperator):
    # see 
    # 1) https://github.com/salrashid123/composer_gcf
    # 2) https://github.com/salrashid123/composer_gcf/blob/master/composer_to_gcf/to_gcf.py
    def execute(self, context):
        token = requests.get(f"{METADATA_ROOT}{AUDIENCE_ROOT}/{self.endpoint}", headers = {"Metadata-Flavor": "Google"}).text
        HttpHook(self.method, http_conn_id = self.http_conn_id)\
            .run(self.endpoint, self.data, {"Authorization": f"Bearer {token}"}, self.extra_options)

with models.DAG("single_step", schedule_interval = datetime.timedelta(days = 1), catchup = False) as dag:
    get_timeseries = ComputeEndpointOperator(
        task_id      = "get_timeseries",
        method       = "POST",
        endpoint     = "STEP_0_RAW-get-state-case-timeseries",
        start_date   = datetime.datetime(2021, 4, 29),
        http_conn_id = "cloud_functions"
    )
