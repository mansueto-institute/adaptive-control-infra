from google.oauth2 import id_token, service_account
from google.auth.transport.requests import AuthorizedSession
from threading import Thread

import pandas as pd
import requests


def estimate_1_state_rt(state):

    # Get authentication from service account
    url = f'https://hello-world-us-estimates-sipjq3uhla-uc.a.run.app/{state}'
    creds = service_account.IDTokenCredentials.from_service_account_file('service_account_credentials.json', 
                                                                          target_audience=url)
    authed_session = AuthorizedSession(creds)

    # Make authenticated request and return status code
    resp = authed_session.get(url, timeout=900)
    statcode = resp.status_code

    # If estimation failed, complain and try again
    if statcode != 200:
        print(f"Something went wrong estimating Rt for {state}! Trying again...")
        resp = authed_session.get(url, timeout=900)
        statcode = resp.status_code
        if statcode != 200:
            print(f"\tFailed at estimating Rt again for {state}. Giving up...")
        else:
            print(f"\tSuccesfully estimated Rt for {state} on 2nd try")
    else:
        print(f"Success estimating Rt for {state}!")


def estimate_every_state_rt(_):

    # Get latest covidtracking data
    res = requests.get("https://covidtracking.com/api/v1/states/daily.json")
    df  = pd.read_json(res.text)

    # Just get list of states (excluding certain areas)
    excluded_areas = set(['PR','MP','AS','GU','VI'])
    df = df[~df['state'].isin(excluded_areas)]
    states = df['state'].unique()

    # Do estimation for each state
    for state in states:
        thr = Thread(target=estimate_1_state_rt, args=(state,))
        thr.start()






