import math
from syslog import LOG_NEWS
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

# def get_contributors(repo_df, repo_contributors_output_path, users_output_path):
#     """Function to get all contributors to a list of repositories and also update final list of users.
#     :param repo_df: dataframe of repositories
#     :param repo_contributors_output_path: path to repo contributors file
#     :param users_output_path: path to users file
#     returns: dataframe of repo contributors and unique users"""
#     if os.path.exists('../data/error_logs/repo_contributors_errors.csv'):
#         os.remove('../data/error_logs/repo_contributors_errors.csv')
#     contributors_rows = []
#     repo_contributors = []
#     for _, row in tqdm(repo_df.iterrows(), total=repo_df.shape[0], desc="Getting Contributors"):
#         try: 
#             url = row.contributors_url
#             response = requests.get(url, headers=auth_headers)
#             if response.status_code != 200: 
#                 time.sleep(120)
#                 response = requests.get(url, headers=auth_headers)
#             response_data = response.json()
#             df = pd.json_normalize(response_data)
#             repo_contributors_df = df[['login', 'id', 'node_id', 'url', 'html_url']].copy()
#             repo_contributors_df['repo_id'] = row.id
#             repo_contributors_df['repo_url'] = row.url
#             repo_contributors_df['repo_html_url'] = row.html_url
#             repo_contributors_df['repo_full_name'] = row.full_name
#             repo_contributors_df['contributors_url'] = row.contributors_url
#             repo_contributors.append(repo_contributors_df)
#             for _, row in df.iterrows():
#                 expanded_response = requests.get(row.url, headers=auth_headers)
#                 if expanded_response.status_code != 200:
#                     time.sleep(120)
#                     expanded_response = requests.get(row.url, headers=auth_headers)
#                 expanded_df = pd.json_normalize(expanded_response.json())
#                 cols = list(set(expanded_df.columns) & set(df.columns))
#                 merged_df = df.merge(expanded_df, on=cols, how='left')
#                 contributors_rows.append(merged_df)
#             rates_df = check_rate_limit()

#             calls_remaining = rates_df['resources.core.limit']
#             if int(calls_remaining) < 0:
#                 print(f'Remaining queries: {calls_remaining}')
#                 reset_time = rates_df['resources.core.reset']
#                 current_time = time.time()
#                 print(f'Sleeping for {int(reset_time) - math.trunc(current_time)}')
#                 time.sleep(int(reset_time) - math.trunc(current_time))
#             else:
#                 continue
#         except:
#             print(f"Error on getting contributors for {row.full_name}")
#             error_df = pd.DataFrame([{'repo_full_name': row.full_name, 'error_time': time.time(), 'contributors_url': row.contributors_url}])
#             if os.path.exists('../data/error_logs/repo_contributors_errors.csv'):
#                 error_df.to_csv('../data/error_logs/repo_contributors_errors.csv', mode='a', header=False, index=False)
#             else:
#                 error_df.to_csv('../data/error_logs/repo_contributors_errors.csv', index=False)
#             continue
#     contributors_df = pd.concat(contributors_rows)
#     contributors_df = contributors_df.drop_duplicates(subset=['login', 'id'])
#     users_df = check_add_users(contributors_df, users_output_path)
#     repo_contributors_df = pd.concat(repo_contributors)
#     repo_contributors_df.to_csv(repo_contributors_output_path, index=False)
#     return repo_contributors_df, users_df


# def get_repo_contributors(repo_df,repo_contributors_output_path, users_output_path, rates_df, load_existing=True):
#     """Function to get all contributors to a list of repositories and also update final list of users.
#     :param repo_df: dataframe of repositories
#     :param repo_contributors_output_path: path to repo contributors file
#     :param users_output_path: path to users file
#     :param rates_df: dataframe of rate limits
#     :param load_existing: boolean to load existing data
#     returns: dataframe of repo contributors and unique users"""
#     if load_existing:
#         repo_contributors_df = pd.read_csv(repo_contributors_output_path, low_memory=False)
#         users_df = pd.read_csv(users_output_path, low_memory=False)
#     else:
#         calls_remaining = rates_df['resources.core.remaining'].values[0]
#         while len(repo_df[repo_df.contributors_url.notna()]) > calls_remaining:
#             time.sleep(3700)
#             rates_df = check_rate_limit()
#             calls_remaining = rates_df['resources.core.remaining'].values[0]
#         else:
#             if os.path.exists(repo_contributors_output_path):
                
#                 repo_contributors_df = pd.read_csv(repo_contributors_output_path, low_memory=False)
#                 users_df = pd.read_csv(users_output_path, low_memory=False)
#                 error_df = pd.read_csv("../data/error_logs/repo_contributors_errors.csv")
#                 unprocessed_contributors = repo_df[~repo_df.contributors_url.isin(repo_contributors_df.contributors_url)]
#                 unprocessed_contributors = unprocessed_contributors[~unprocessed_contributors.contributors_url.isin(error_df.contributors_url)]
#                 if len(unprocessed_contributors) > 0:
#                     new_contributors_df, users_df = get_contributors(unprocessed_contributors, repo_contributors_output_path, users_output_path)
#                     repo_contributors_df = pd.concat([unprocessed_contributors, new_contributors_df])
#                     repo_contributors_df.to_csv(repo_contributors_output_path, index=False)
#             else:
#                 repo_contributors_df, users_df = get_contributors(repo_df, repo_contributors_output_path, users_output_path)
        
#     return repo_contributors_df, users_df

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
        
        # organize_data_from_response() is local
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
        rates_df = check_rate_limit()

        calls_remaining = rates_df['resources.core.limit']
        if int(calls_remaining) < 0:
            print(f'Remaining queries: {calls_remaining}')
            reset_time = rates_df['resources.core.reset']
            current_time = time.time()
            print(f'Sleeping for {int(reset_time) - math.trunc(current_time)}')
            time.sleep(int(reset_time) - math.trunc(current_time))
        else:
            continue
    
    print(f"{column_name} queries completed")
