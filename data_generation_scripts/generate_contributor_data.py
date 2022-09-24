import math
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

def get_contributors(repo_df, output_path):
    contributors_rows = []
    for _, row in tqdm(repo_df.iterrows(), total=repo_df.shape[0], desc="Getting Contributors"):
        try: 
            url = row.contributors_url
            response = requests.get(url, headers=auth_headers)
            if response.status_code != 200: 
                time.sleep(120)
                response = requests.get(url, headers=auth_headers)
            response_data = response.json()
            df = pd.json_normalize(response_data)
            df['repo_id'] = row.id
            df['html_url'] = row.html_url
            df['full_name'] = row.full_name
            expanded_response = requests.get(df.url.values[0], headers=auth_headers)
            if expanded_response.status_code != 200:
                time.sleep(120)
                expanded_response = requests.get(df.url.values[0], headers=auth_headers)
            expanded_df = pd.json_normalize(expanded_response.json())
            cols = list(set(expanded_df.columns) & set(df.columns))
            merged_df = df.merge(expanded_df, on=cols, how='left')
            contributors_rows.append(merged_df)
        except:
            print(f"Error on getting contributors for {row.full_name}")
            continue
    contributors_df = pd.concat(contributors_rows)
    contributors_df.to_csv(output_path, index=False)
    return contributors_df

def get_repo_contributors(repo_df, output_path, rates_df):
    calls_remaining = rates_df['resources.core.remaining'].values[0]
    while len(repo_df[repo_df.contributors_url.notna()]) > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.core.remaining'].values[0]
    else:
        if os.path.exists(output_path):
            contributors_df = pd.read_csv(output_path)
            if len(contributors_df[contributors_df.login.isna()]) > 0:
                existing_contributors = contributors_df[contributors_df.login.isna() == False]
                missing_repos = contributors_df[contributors_df.login.isna()].html_url.unique().tolist()
                missing_repos_df = repo_df[repo_df.html_url.isin(missing_repos)]
                missing_repos_df = get_contributors(missing_repos_df, output_path)
                contributors_df = pd.concat([existing_contributors, missing_repos_df])
                contributors_df.to_csv(output_path, index=False)
        else:
            contributors_df = get_contributors(repo_df, output_path)
    return contributors_df

def organize_data_from_response(response_data, row):

    data = pd.json_normalize(response_data)
    # Add contributor data for easier linking
    data['owner'] = row.login
    data['owner_id'] = row.id
    data['owner_url'] = row.url

    return data

def get_connected_repos(df, column_name, output_path):
    """
    Working from a dataframe of contributors to DH projects, generates repo data for all repositories connected to those users.

    :param df: Dataframe of all contributors to the initial search for DH related repositories
    :param column_name: String containing the columns with URL to connected data. Expected values are `repos_url`, `organizations_url`, `followers_url`, `subscriptions_url`
    :param output_path: Location to save table of contributor repos
    :type df: dataframe
    :type column_name: str
    :type output_path: str
    :return: Dataframe of all contributor related respositories
    :rtype: dataframe
    """

    for _, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Getting {column_name} data"):
        
        expanded_rows = []

        url = row[column_name]
        response = requests.get(url, headers=auth_headers)
        # get_response_data() function in utils
        response_data = get_response_data(response, url)
        # organize_data_from_response() in utils
        data = organize_data_from_response(response_data, row)
        expanded_rows.append(data)

        while "next" in response.links.keys():
            print(f"Getting next page for {url}")
            time.sleep(5)
            query = response.links['next']['url']
            response = requests.get(query, headers=auth_headers)
            
            response_data = get_response_data(response, query)
            data = organize_data_from_response(response_data, row)
            expanded_rows.append(data)

        user_repos = pd.concat(expanded_rows)
        user_repos.to_csv(output_path, mode='a', index=False, header=False)

        # check status before continuing to next row
        api_calls = check_api_calls('jerielizabeth')
        calls_remaining = api_calls['remaining_calls']
        if int(calls_remaining) < 5:
            print(f'Remaining queries: {calls_remaining}')
            reset_time = api_calls['reset_time']
            current_time = time.time()
            print(f'Sleeping for {int(reset_time) - math.trunc(current_time)}')
            time.sleep(int(reset_time) - math.trunc(current_time))
        else:
            continue
    
    print(f"{column_name} queries completed")