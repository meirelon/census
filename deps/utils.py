import requests
import json
from datetime import datetime

import pandas as pd
import numpy as np

import gcsfs

import requests
import json
from datetime import datetime

import pandas as pd
import numpy as np

import gcsfs

def get_states(key, year=2020):
    url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CRRALL,CRRINT,DAVG,DINTAVG,DRRALL,DRRINT&key={key}&for=state:*"
    JSONContent = requests.get(url).json()
    temp = pd.DataFrame(JSONContent)
    temp.columns = temp.iloc[0]
    temp = temp.iloc[1:,-1]
    states = [int(i) for i in temp]

    labels = pd.DataFrame(JSONContent).iloc[0,:-1]

    return [states, labels]

def get_responses(states, key, labels, year=2020):
    # columns=['GEO_ID','FSRR2010']
    columns=labels

    tract_responses = pd.DataFrame(columns=columns)

    for i in states:
        if i < 10:
            url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CRRALL,CRRINT,DAVG,DINTAVG,DRRALL,DRRINT&key={key}&for=tract:*&in=state:0"\
            + str(i)
        else:
            url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CRRALL,CRRINT,DAVG,DINTAVG,DRRALL,DRRINT&key={key}&for=tract:*&in=state:"\
            + str(i)
        JSONContent = requests.get(url).json()
        temp = pd.DataFrame(JSONContent)
        temp.columns = temp.iloc[0]
        temp = temp.iloc[1:,0:-3]
        tract_responses = pd.concat([tract_responses,temp],sort=True)
    return tract_responses

def get_county_responses(states, key, labels, year=2020):
    county_responses = pd.DataFrame(columns=labels)

    for i in states:
        if i < 10:
            url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CRRALL,CRRINT,DAVG,DINTAVG,DRRALL,DRRINT&key={key}&key={key}&for=county:*&in=state:0"\
            + str(i)
        else:
            url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CRRALL,CRRINT,DAVG,DINTAVG,DRRALL,DRRINT&key={key}&for=county:*&in=state:"\
            + str(i)
        JSONContent = requests.get(url).json()
        temp = pd.DataFrame(JSONContent)
        temp.columns = temp.iloc[0]
        temp = temp.iloc[1:,0:-2]
        county_responses = pd.concat([county_responses,temp],sort=True)

    return county_responses

def get_state_responses(key, year=2020):
    url = f"https://api.census.gov/data/2020/dec/responserate?get=GEO_ID,CRRALL,CRRINT,DAVG,DINTAVG,DRRALL,DRRINT&key={key}&for=state:*"
    JSONContent = requests.get(url).json()
    state_responses = pd.DataFrame(JSONContent)
    state_responses.columns = state_responses.iloc[0]
    state_responses = state_responses.iloc[1:,0:-1]

    return state_responses



def run(bucket_name):
    year=2020
    key = '2988f01f5e86175bda8beae2b5035e1ccef2d052'
    dt = datetime.now().strftime("%Y%m%d")

    states, labels = get_states(key, year)
    tract_responses = get_responses(states, key, labels, year)
    tract_responses.to_csv(f"gs://{bucket_name}/{year}/tract_responses_{dt}.csv", index=False)

    state_responses = get_state_responses(key, year)
    state_responses.to_csv(f"gs://{bucekt_name}/{year}/state_responses_{dt}.csv", index=False)

    county_responses = get_county_responses(states, key, labels, year)
    county_responses.to_csv(f"gs://{bucket_name}/{year}/county_responses_{dt}.csv", index=False)


    return "True"
