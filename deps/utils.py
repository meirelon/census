import requests
import json
from datetime import datetime

import pandas as pd
import numpy as np

import gcsfs

def get_states():
    url = "https://api.census.gov/data/2010/dec/responserate?get=GEO_ID,FSRR2010&for=state:*"
    JSONContent = requests.get(url).json()
    states = pd.DataFrame(JSONContent)
    states.columns = states.iloc[0]
    states = states.iloc[1:,2]
    states = [int(i) for i in states]

    return states

def get_responses(states):
    columns=['GEO_ID','FSRR2010']

    tract_responses = pd.DataFrame(columns=columns)

    for i in states:
        if i < 10:
            url = "https://api.census.gov/data/2010/dec/responserate?get=GEO_ID,FSRR2010&for=tract:*&in=state:0" + str(i)
        else:
            url = "https://api.census.gov/data/2010/dec/responserate?get=GEO_ID,FSRR2010&for=tract:*&in=state:" + str(i)
        JSONContent = requests.get(url).json()
        temp = pd.DataFrame(JSONContent)
        temp = temp.iloc[1:,[0,1]]
        temp.columns = columns
        tract_responses = pd.concat([tract_responses,temp])
        return tract_responses

def get_county_responses(states):
    county_responses = pd.DataFrame(columns=columns)

    for i in states:
        if i < 10:
            url = "https://api.census.gov/data/2010/dec/responserate?get=GEO_ID,FSRR2010&for=county:*&in=state:0" + str(i)
        else:
            url = "https://api.census.gov/data/2010/dec/responserate?get=GEO_ID,FSRR2010&for=county:*&in=state:" + str(i)
        JSONContent = requests.get(url).json()
        temp = pd.DataFrame(JSONContent)
        temp = temp.iloc[1:,[0,1]]
        temp.columns = columns
        county_responses = pd.concat([county_responses,temp])

        return county_responses

def get_state_responses():
    url = "https://api.census.gov/data/2010/dec/responserate?get=GEO_ID,FSRR2010&for=state:*"
    JSONContent = requests.get(url).json()
    state_responses = pd.DataFrame(JSONContent)
    state_responses.columns = state_responses.iloc[0]
    state_responses = state_responses.iloc[1:,0:2]

    return state_responses


def run(bucket_name):
    dt = datetime.now().strftime("%Y%m%d")
    states = get_states()
    tract_responses = get_responses(states)
    tract_responses.to_csv(f"gs://{bucket_name}/tract_responses_{dt}.csv", index=False)

    state_responses = get_state_responses()
    state_responses.to_csv(f"gs://{bucekt_name}/state_responses_{dt}.csv", index=False)

    county_responses = get_county_responses(states)
    county_responses.to_csv(f"gs://{bucket_name}/county_responses_{dt}.csv", index=False)


    return "True"
