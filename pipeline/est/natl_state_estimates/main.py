from warnings import simplefilter

import numpy as np
import pandas as pd
from epimargin.estimators import analytical_MPVS
from epimargin.smoothing import notched_smoothing
from google.cloud import storage

simplefilter("ignore")

# model details 
gamma     = 0.1 # 10 day infectious period
smoothing = 7
CI        = 0.95
lookback  = 120 # how many days back to start estimation
cutoff    = 2   # most recent data to use 


# cloud details 
bucket_name = "daily_pipeline"


def run_estimates(_):
    storage.Client()\
        .bucket(bucket_name)\
        .blob("pipeline/raw/india_case_timeseries.csv")\
        .download_to_filename("/tmp/india_case_timeseries.csv")

    india_ts = pd.read_csv("/tmp/india_case_timeseries.csv")

    # country level
    (dates, RR_pred, RR_CI_upper, RR_CI_lower, *_) =\
        analytical_MPVS(india_ts["Hospitalized"].iloc[-(lookback+cutoff):-cutoff], CI = CI, smoothing = notched_smoothing(window = smoothing)) 


    # state level rt estimates
    state_time_series = get_time_series(state_df, 'state')
    states = list(state_time_series.index.get_level_values(level=0).unique())
    for state in states:
        state_code = state_name_lookup[state]
        try: 
            (dates, RR_pred, RR_CI_upper, RR_CI_lower, *_) = gamma_prior(state_time_series.loc[state]['Hospitalized'], CI = CI, smoothing = notched_smoothing(window = smoothing))
            for row in zip(dates, RR_pred, RR_CI_upper, RR_CI_lower):
                timeseries.append((state_code, *row))
            estimates.append((state_code, RR_pred[-1], RR_CI_lower[-1], RR_CI_upper[-1], project(dates, RR_pred, smoothing)))
            print(f"{state_code}: success")
        except (IndexError, ValueError): 
            estimates.append((state, np.nan, np.nan, np.nan, np.nan))
            print(f"{state_code}: error")

    # save out estimates 
    estimates = pd.DataFrame(estimates)
    estimates.columns = ["state", "Rt", "Rt_CI_lower", "Rt_CI_upper", "Rt_proj"]
    estimates.set_index("state", inplace=True)
    estimates.to_csv(data/"Rt_estimates.csv")

    timeseries = pd.DataFrame(timeseries)
    timeseries.columns = ["state", "date", "Rt", "Rt_upper", "Rt_lower"]
    timeseries.set_index("state", inplace=True)
    timeseries.to_csv(data/"Rt_timeseries_india.csv")

    # upload to cloud 
    
    bucket.blob("estimates/Rt_estimates.csv")       .upload_from_filename(str(data/"Rt_estimates.csv"),        content_type = "text/csv")
    bucket.blob("estimates/Rt_timeseries_india.csv").upload_from_filename(str(data/"Rt_timeseries_india.csv"), content_type = "text/csv")
