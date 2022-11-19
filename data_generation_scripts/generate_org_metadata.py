import os
import sys
import time

import apikey
import pandas as pd
import requests
from tqdm import tqdm

sys.path.append("..")
import shutil
import warnings

from data_generation_scripts.utils import *

warnings.filterwarnings('ignore')

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}


def get_orgs(org_df, org_output_path, get_url_field, error_file_path, overwrite_existing_temp_files):
    temp_org_dir = f"../data/temp/{org_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    
    if (os.path.exists(temp_org_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_org_dir)
    
    if not os.path.exists(temp_org_dir):
        os.makedirs(temp_org_dir)
    org_progress_bar = tqdm(total=len(org_df), desc="Cleaning Orgs", position=0)
    for _, row in org_df.iterrows():
        try:
            # Create the temporary directory path to store the data
            temp_org_path =  F"{row.login.replace('/','')}_org_{get_url_field}.csv"

            # Check if the org_df has already been saved to the temporary directory
            if os.path.exists(temp_org_dir + temp_org_path):
                org_progress_bar.update(1)
                continue
            # Create the url to get the org
            url = row.url.replace('user', 'orgs')

            # Make the first request
            response = requests.get(url, headers=auth_headers)
            response_data = get_response_data(response, url)
            if len(response_data) == 0:
                response_df = pd.read_csv('../data/metadata_files/org_headers.csv')
            else:
                response_df = pd.DataFrame(response_data)
            response_df.to_csv(temp_org_dir + temp_org_path, index=False)
            org_progress_bar.update(1)
        except:
            org_progress_bar.total = org_progress_bar.total - 1
            # print(f"Error on getting orgs for {row.login}")
            error_df = pd.DataFrame([{'login': row.login, 'error_time': time.time(), 'error_url': url}])
            
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            org_progress_bar.update(1)
            continue
    org_df = read_combine_files(temp_org_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_org_dir)
    # Close the progress bars
    org_progress_bar.close()
    return org_df

def clean_orgs(org_df, org_output_path):
