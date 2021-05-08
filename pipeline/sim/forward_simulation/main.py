from pathlib import Path

import pandas as pd
from epimargin.etl.covid19india import state_code_lookup, state_name_lookup
from epimargin.smoothing import notched_smoothing
from google.cloud import storage

# model details 
gamma     = 0.1 # 10 day infectious period
window    = 10
CI        = 0.95
lookback  = 120 # how many days back to start estimation
cutoff    = 2   # most recent data to use 
excluded = ["Unknown", "Other State", "Airport Quarantine", "Railway Quarantine"]
coalesce_states = ["Delhi", "Manipur", "Dadra And Nagar Haveli And Daman And Diu", "Andaman And Nicobar Islands"]
survey_date = "October 23, 2020"
columns  = ["state_code", "state", "district", "sero_0", "N_0", "sero_1", "N_1", "sero_2", "N_2", "sero_3", "N_3", "sero_4", "N_4", "sero_5", "N_5", "sero_6", "N_6", "N_tot", "Rt", "S0", "I0", "R0", "D0", "dT0", "dD0", "V0", "pandemic_start"]
# cloud details 
bucket_name = "daily_pipeline"

def get(request, key):
    request_json = request.get_json()
    if request.args and key in request.args:
        return request.args.get(key)
    elif request_json and key in request_json:
        return request_json[key]
    else:
        return None

def run(request):
    state_code = get(request, 'state_code')
    state = state_code_lookup[state_code]

    print(f"Downloading initial conditions for {state_code} ({state}).")
    
    bucket = storage.Client().bucket(bucket_name)
    data = Path("/tmp")

    bucket.blob(f"pipeline/sim/input/{state_code}_simulation_initial_conditions.csv")\
        .download_to_filename(data / f"{state_code}_simulation_initial_conditions.csv")

    return "OK!"
