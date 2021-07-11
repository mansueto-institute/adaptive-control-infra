import pandas as pd
from google.cloud import storage
import matplotlib.pyplot as plt
import os

bucket_name = "daily_pipeline"
bucket = storage.Client().bucket(bucket_name)

state_lookup = {
 'Andaman and Nicobar Islands': 'AN',
 'Andhra Pradesh': 'AP',
 'Arunachal Pradesh': 'AR',
 'Assam': 'AS',
 'Bihar': 'BR',
 'Chandigarh': 'CH',
 'Chhattisgarh': 'CT',
 'Daman and Diu': 'DD',
 'Dadra and Nagar Haveli and Daman and Diu': 'DDDN',
 'Delhi': 'DL',
 'Dadra and Nagar Haveli': 'DN',
 'Goa': 'GA',
 'Gujarat': 'GJ',
 'Himachal Pradesh': 'HP',
 'Haryana': 'HR',
 'Jharkhand': 'JH',
 'Jammu and Kashmir': 'JK',
 'Karnataka': 'KA',
 'Kerala': 'KL',
 'Ladakh': 'LA',
 'Lakshadweep': 'LD',
 'Maharashtra': 'MH',
 'Meghalaya': 'ML',
 'Manipur': 'MN',
 'Madhya Pradesh': 'MP',
 'Mizoram': 'MZ',
 'Nagaland': 'NL',
 'Odisha': 'OR',
 'Punjab': 'PB',
 'Puducherry': 'PY',
 'Rajasthan': 'RJ',
 'Sikkim': 'SK',
 'Telangana': 'TG',
 'Tamil Nadu': 'TN',
 'Tripura': 'TR',
 'India': 'TT',
 'State Unassigned': 'UN',
 'Uttar Pradesh': 'UP',
 'Uttarakhand': 'UT',
 'West Bengal': 'WB'}

def transfer_to_bucket(file_path):
    size_kb = os.stat(file_path).st_size / 1000
    print("Timeseries artifact size : {} KB".format(size_kb))
    assert size_kb > 15
    file_name = file_path.split("/")[2]
    bucket.blob("pipeline/rpt/{}".format(file_name)).upload_from_filename(file_path, content_type="image/png")

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
        state_code = state_lookup[state]
        totalStateDailyAgg["first_dose_admin"].plot()
        plt.title("Number of first doses administered - {}".format(state))
        plt.savefig("/tmp/first_dose_admin_{}.png".format(state_code))
        plt.close()
        print("Generated first dose statistics plot for {}".format(state))

        totalStateDailyAgg["total_individuals_registered"].plot()
        plt.title("Number of individuals registered - {}".format(state))
        plt.savefig("/tmp/total_individuals_registered_{}.png".format(state_code))
        plt.close()
        print("Generated total individuals registered plot for {}".format(state))

        totalStateDailyAgg["second_dose_admin"].plot()
        plt.title("Number of second doses administered - {}".format(state))
        plt.savefig("/tmp/second_dose_admin_{}.png".format(state_code))
        plt.close()
        print("Generated second dose statistics plot for {}".format(state))

        # Check if the outputs are at least 50 kb and transfer them to buckets
        transfer_to_bucket("/tmp/first_dose_admin_{}.png".format(state_code))
        transfer_to_bucket("/tmp/total_individuals_registered_{}.png".format(state_code))
        transfer_to_bucket("/tmp/second_dose_admin_{}.png".format(state_code))

    # top 10 districts nationwide based on number of vaccines administered in a given day

    df["total_vac"] = df["male_vac"] + df["female_vac"] + df["trans_vac"]
    todayDF = df.loc[df["date"] == "2021-05-10", ["district", "state","total_vac"]].copy().reset_index()
    yesterdayDF = df.loc[df["date"] == "2021-05-09", ["district","state", "total_vac"]].copy().reset_index()

    differenceDF = todayDF.copy()
    differenceDF["total_vac"] -= yesterdayDF["total_vac"]
    top10Districts = differenceDF.sort_values(by = "total_vac", ascending = False)[:10]
    print(top10Districts)
    return "OK!"
