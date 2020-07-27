import sys
from pathlib import Path
from typing import Dict, Optional, Sequence
from warnings import simplefilter

from google.cloud import storage
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant

from adaptive.estimators import gamma_prior
from adaptive.etl.covid19india import (download_data, get_time_series,
                                       load_statewise_data, state_name_lookup)
from adaptive.plots import plot_RR_est
from adaptive.smoothing import convolution
from adaptive.utils import cwd, days

simplefilter("ignore")

# model details 
gamma     = 0.2
smoothing = 21
CI        = 0.95

# cloud details 
bucket_name = "adaptive-control-daily-pipeline"

def project(dates, R_values, smoothing, period = 7*days):
    julian_dates = [_.to_julian_date() for _ in dates[-smoothing//2:None]]
    return OLS(
        R_values[-smoothing//2:None], 
        add_constant(julian_dates)
    )\
    .fit()\
    .predict([1, julian_dates[-1] + period])[0]

def run_estimates(_):
    root = Path("/tmp")
    data = root/"data"

    download_data(data, 'state_wise_daily.csv')

    state_df = load_statewise_data(data/"state_wise_daily.csv")
    country_time_series = get_time_series(state_df)

    estimates  = []
    timeseries = []

    # country level
    (dates, RR_pred, RR_CI_upper, RR_CI_lower, *_) = gamma_prior(country_time_series["Hospitalized"].iloc[:-1], CI = CI, smoothing = convolution(window = smoothing)) 

    country_code = state_name_lookup["India"]
    for row in zip(dates, RR_pred, RR_CI_upper, RR_CI_lower):
        timeseries.append((country_code, *row))

    # state level rt estimates
    state_time_series = get_time_series(state_df, 'state')
    states = list(state_time_series.index.get_level_values(level=0).unique())
    for state in states:
        state_code = state_name_lookup[state]
        try: 
            (dates, RR_pred, RR_CI_upper, RR_CI_lower, *_) = gamma_prior(state_time_series.loc[state]['Hospitalized'], CI = CI, smoothing = convolution(window = smoothing))
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
    bucket = storage.Client().bucket(bucket_name)
    estimate_blob   = bucket.blob("estimates/Rt_estimates.csv").upload_from_filename(str(data/"Rt_estimates.csv"), content_type = "text/csv")
    timeseries_blob = bucket.blob("estimates/Rt_timeseries_india.csv").upload_from_filename(str(data/"Rt_timeseries_india.csv"), content_type = "text/csv")
