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

def assemble_data(request):
    state_code = get(request, 'state_code')
    state = state_code_lookup[state_code]

    print(f"Assembling initial conditions for {state_code} ({state}).")
    
    bucket = storage.Client().bucket(bucket_name)
    data = Path("/tmp")

    bucket.blob("pipeline/commons/refs/all_india_sero_pop.csv")\
        .download_to_filename(data / "all_india_sero_pop.csv")

    bucket.blob("pipeline/raw/state_case_timeseries.csv")\
        .download_to_filename(data / "state_case_timeseries.csv")

    bucket.blob("pipeline/raw/district_case_timeseries.csv")\
        .download_to_filename(data / "district_case_timeseries.csv")

    bucket.blob("pipeline/raw/vaccine_doses_statewise.csv")\
        .download_to_filename(data / "vaccine_doses_statewise.csv")

    bucket.blob(f"pipeline/est/{state_code}_district_Rt.csv")\
        .download_to_filename(data / f"{state_code}_district_Rt.csv")

    bucket.blob(f"pipeline/est/{state_code}_state_Rt.csv")\
        .download_to_filename(data / f"{state_code}_state_Rt.csv")
    
    print(f"Downloaded simulation input data for {state_code} ({state}).")

    district_age_pop = pd.read_csv(data / "all_india_sero_pop.csv").set_index(["state", "district"])
    
    state_ts = pd.read_csv(data / "state_case_timeseries.csv")\
        .set_index(["detected_state", "status_change_date"])\
        .drop(columns = ["date", "time", "delta", "logdelta"])\
        .rename(columns = {
            "Deceased":     "dD",
            "Hospitalized": "dT",
            "Recovered":    "dR"
        })
    district_ts = pd.read_csv(data / "district_case_timeseries.csv")\
        .set_index(["detected_state", "detected_district", "status_change_date"]).loc[state]\
        .drop(columns = ["date", "time", "delta", "logdelta"])\
        .rename(columns = {
            "Deceased":     "dD",
            "Hospitalized": "dT",
            "Recovered":    "dR"
        })
    
    state_Rt = pd.read_csv(data / f"{state_code}_state_Rt.csv",    index_col = 0, parse_dates = ["dates"])\
        [["dates", "Rt_pred"]]\
        .assign(district = state)\
        .drop_duplicates(subset = "district", keep = "last")\
        [["district", "Rt_pred"]]\
        .set_index("district")
    district_Rt = pd.read_csv(data / f"{state_code}_district_Rt.csv", index_col = 0, parse_dates = ["dates"])\
        [["district", "dates", "Rt_pred"]]\
        .drop_duplicates(subset = "district", keep = "last")\
        [["district", "Rt_pred"]]\
        .set_index("district")
    
    vax = pd.read_csv(data / "vaccine_doses_statewise.csv").set_index("State").T.dropna()
    vax.columns = vax.columns.str.title()
    vax.set_index(pd.to_datetime(vax.index), inplace = True)

    smooth = notched_smoothing(window = window)
    simulation_start = pd.Timestamp.today() - pd.Timedelta(days = cutoff)

    districts_to_run = district_age_pop.loc[state]
    # if time series data not available at the district level, coalesce to state/UT level
    if state in coalesce_states:
        districts_to_run = districts_to_run\
            .assign(**{f"infected_{i}": (lambda i: lambda _: _[f"sero_{i}"] * _[f"N_{i}"])(i) for i in range(7)})\
            .drop(columns = [f"sero_{i}" for i in range(7)])\
            .sum(axis = 0)\
            .to_frame().T\
            .assign(**{f"sero_{i}": (lambda i: lambda _: _[f"infected_{i}"] / _[f"N_{i}"])(i) for i in range(7)})\
            [districts_to_run.columns]\
            .assign(district = state)\
            .set_index("district")
        ts = state_ts
        districts_to_run = districts_to_run.join(state_Rt)
    else: 
        ts = district_ts
        districts_to_run = districts_to_run.join(district_Rt)
    

    print(f"Done reading input data for {state_code} ({state}).")
    print(f"Running seroprevalence scaling for districts.")

    rows = []
    for _ in districts_to_run.dropna().itertuples():
        district, sero_0, sero_1, sero_2, sero_3, sero_4, sero_5, sero_6, N_0, N_1, N_2, N_3, N_4, N_5, N_6, N_tot, Rt = _
        print(f"Scaling for {state_code}/{district}.")

        dR_conf = ts.loc[district].dR
        dR_conf = dR_conf.reindex(pd.date_range(dR_conf.index.min(), dR_conf.index.max()), fill_value = 0)
        if len(dR_conf) >= window + 1:
            dR_conf_smooth = pd.Series(smooth(dR_conf), index = dR_conf.index).clip(0).astype(int)
        else: 
            dR_conf_smooth = dR_conf

        R_conf_smooth  = dR_conf_smooth.cumsum().astype(int)
        R_conf = R_conf_smooth[survey_date if survey_date in R_conf_smooth.index else -1]
        R_sero = (sero_0*N_0 + sero_1*N_1 + sero_2*N_2 + sero_3*N_3 + sero_4*N_4 + sero_5*N_5 + sero_6*N_6)
        R_ratio = R_sero/R_conf if R_conf != 0 else 1 
        R0 = R_conf_smooth[simulation_start if simulation_start in R_conf_smooth.index else -1] * R_ratio
        print("Scaled recoveries.")
        
        dD_conf = ts.loc[district].dD
        dD_conf = dD_conf.reindex(pd.date_range(dD_conf.index.min(), dD_conf.index.max()), fill_value = 0)
        if len(dD_conf) >= window + 1:
            dD_conf_smooth = pd.Series(smooth(dD_conf), index = dD_conf.index).clip(0).astype(int)
        else:
            dD_conf_smooth = dD_conf
        D_conf_smooth  = dD_conf_smooth.cumsum().astype(int)
        D0 = D_conf_smooth[simulation_start if simulation_start in D_conf_smooth.index else -1]
        print("Scaled deaths.")
        
        dT_conf = ts.loc[district].dT
        pandemic_start = dT_conf.index.min()
        dT_conf = dT_conf.reindex(pd.date_range(dT_conf.index.min(), dT_conf.index.max()), fill_value = 0)
        if len(dT_conf) >= window + 1:
            dT_conf_smooth = pd.Series(smooth(dT_conf), index = dT_conf.index).clip(0).astype(int)
        else:
            dT_conf_smooth = dT_conf
        T_conf_smooth  = dT_conf_smooth.cumsum().astype(int)
        T_conf = T_conf_smooth[survey_date if survey_date in T_conf_smooth.index else -1]
        T_sero = R_sero + D0 
        T_ratio = T_sero/T_conf if T_conf != 0 else 1 
        T0 = T_conf_smooth[simulation_start if simulation_start in T_conf_smooth.index else -1] * T_ratio
        print("Scaled cases.")

        S0 = max(0, N_tot - T0)
        dD0 = dD_conf_smooth[simulation_start if simulation_start in dD_conf_smooth.index else -1]
        dT0 = dT_conf_smooth[simulation_start if simulation_start in dT_conf_smooth.index else -1] * T_ratio
        I0 = max(0, (T0 - R0 - D0))

        V0 = vax[state][simulation_start if simulation_start in vax.index else -1] * N_tot / districts_to_run.N_tot.sum()
        print("Resolved vaccination data.")

        rows.append((state_code, state, district, 
            sero_0, N_0, sero_1, N_1, sero_2, N_2, sero_3, N_3, sero_4, N_4, sero_5, N_5, sero_6, N_6, N_tot, 
            Rt, S0, I0, R0, D0, dT0, dD0, V0, pandemic_start
        ))
    
    pd.DataFrame(rows, columns = columns).to_csv(data / f"{state_code}_simulation_initial_conditions.csv")
    bucket.blob(f"pipeline/sim/input/{state_code}_simulation_initial_conditions.csv")\
        .upload_from_filename(str(data / f"{state_code}_simulation_initial_conditions.csv"), content_type = "text/csv")

    return "OK!"
