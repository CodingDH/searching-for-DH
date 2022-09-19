import time
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import sys
sys.path.append("..")
from data_generation_scripts.utils import *


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def get_languages(row):
    response = requests.get(row.languages_url, headers=auth_headers)
    if response.status_code != 200:
        time.sleep(120)
        response = requests.get(row.languages_url, headers=auth_headers)
    return response.json()

def get_repo_languages(repo_df, output_path, rates_df):
    calls_remaining = rates_df['resources.core.remaining'].values[0]
    if os.path.exists(output_path):
        repo_df = pd.read_csv(output_path)
    else:
        while len(repo_df[repo_df.languages_url.notna()]) > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.core.remaining'].values[0]
        else:
            tqdm.pandas(desc="Getting Languages")
            repo_df['languages'] = repo_df.progress_apply(get_languages, axis=1)
            repo_df.to_csv(output_path, index=False)
    return repo_df