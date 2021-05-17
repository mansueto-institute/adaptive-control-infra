import pandas as pd
from google.cloud import storage
import matplotlib.pyplot as plt

def generate_vax_report(_):

    # URL of the csv used directly

    df = pd.read_csv("https://www.dropbox.com/sh/y949ncp39towulf/AACd3YxzfB0fHkjQ1YJG-W2ba/covid/csv/covid_vaccination.csv?dl=1")

    # convert date to proper format
    df["date"] = pd.to_datetime(df["date"])

    statesList = df["state"].unique()
    for state in statesList:
        stateDF = df.loc[df["state"] == state]

        # state level total aggregates
        totalStateDailyAgg = stateDF.groupby("date").sum()

        plt.subplots(1,1)
        totalStateDailyAgg["first_dose_admin"].plot()
        plt.savefig("/tmp/first_dose_admin_{}.png".format(state))
        plt.close()
        plt.subplots(1,1)
        totalStateDailyAgg["total_individuals_registered"].plot()
        plt.savefig("/tmp/total_individuals_registered_{}.png".format(state))
        plt.close()

        plt.subplots(1,1)
        totalStateDailyAgg["second_dose_admin"].plot()
        plt.savefig("/tmp/second_dose_adming_{}.png".format(state))
        plt.close()
    # top 10 districts nationwide based on number of vaccines administered in a given day

    df["total_vac"] = df["male_vac"] + df["female_vac"] + df["trans_vac"]
    todayDF = df.loc[df["date"] == "2021-05-10", ["district", "state","total_vac"]].copy().reset_index()
    yesterdayDF = df.loc[df["date"] == "2021-05-09", ["district","state", "total_vac"]].copy().reset_index()

    differenceDF = todayDF.copy()
    differenceDF["total_vac"] -= yesterdayDF["total_vac"]
    top10Districts = differenceDF.sort_values(by = "total_vac", ascending = False)[:10]
    print(top10Districts)
    return "OK!"
