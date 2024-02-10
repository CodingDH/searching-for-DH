import apikey
from tqdm import tqdm
import re
import time
import requests
import sys
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
sys.path.append("..")
from data_generation_scripts.general_utils import check_rate_limit, make_request_with_rate_limiting

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}',
                'User-Agent': 'request'}

def process_response(response):
    if len(response.links) == 0:
        total_stars = len(response.json())
    else:
        total_stars = re.search("\d+$", response.links['last']['url']).group()
    return total_stars

def get_total_results(response, query):
    """Function to get response data from api call
    :param response: response from api call
    :param query: query used to make api call
    :return: response data"""
    # Check if response is valid
    total_results = 0
    if response.status_code != 200:
        if response.status_code == 401:
            print("response code 401 - unauthorized access. check api key")
        elif response.status_code == 204:
            print(f'No data for {query}')
        else:
            print(
                f'response code: {response.status_code}. hit rate limiting. trying to sleep...')
            time.sleep(120)
            response = requests.get(query, headers=auth_headers)

            # Check if response is valid a second time after sleeping
            if response.status_code != 200:
                print(
                    f'query failed twice with code {response.status_code}. Failing URL: {query}')

                # If failed again, check the rate limit and sleep for the amount of time needed to reset rate limit
                rates_df = check_rate_limit()
                if rates_df['resources.core.remaining'].values[0] == 0:
                    print('rate limit reached. sleeping for 1 hour')
                    time.sleep(3600)
                    response = requests.get(
                        query, headers=auth_headers)
                    if response.status_code != 200:
                        print(
                            f'query failed third time with code {response.status_code}. Failing URL: {query}')
                    else:
                        total_results = process_response(response)
            else:
                total_results = process_response(response)
    else:
        total_results = process_response(response)
    return total_results


def get_user_results(row, count_field, url_field, overwrite_existing):
    """Function to get total results for each user"""
    url = f"{row[url_field].split('{')[0]}?per_page=1"
    response = requests.get(url, headers=auth_headers)
    total_results = get_total_results(response, url)
    row[count_field] = total_results
    if (row.name == 0) and (overwrite_existing == True):
        pd.DataFrame(row).T.to_csv(f'../data/temp/{count_field}.csv', header=True, index=False)
    else:
        pd.DataFrame(row).T.to_csv(f'../data/temp/{count_field}.csv', mode='a', header=False, index=False)
    return row


def check_total_results(user_df, count_field, url_field, overwrite_existing):
    """Function to check total results for each user
    :param user_df: dataframe of users
    :return: dataframe of users with total results"""
    tqdm.pandas(desc="Getting total results for each user")
    user_df = user_df.reset_index(drop=True)
    user_df = user_df.progress_apply(get_user_results, axis=1, count_field=count_field, url_field=url_field, overwrite_existing=overwrite_existing)
    return user_df

def get_counts(user_df, url_type, count_type, overwrite_existing_temp_files = False):
        if count_type in user_df.columns:
            needs_counts = user_df[user_df[count_type].isna()]
            has_counts = user_df[user_df[count_type].notna()]
        else:
            needs_counts = user_df
            has_counts = pd.DataFrame()
            
        if len(has_counts) == len(user_df):
            user_df = has_counts
        else:
            needs_counts = check_total_results(needs_counts, count_type, url_type, overwrite_existing_temp_files)
            user_df = pd.concat([needs_counts, has_counts])
        return user_df

if __name__ == "__main__":
    core_users_path = "../data/derived_files/firstpass_core_users.csv"
    core_users = pd.read_csv(core_users_path)

    if os.path.exists("../data/metadata_files/user_url_cols.csv"):
        cols_df = pd.read_csv("../data/metadata_files/user_url_cols.csv")
    else:
        cols_dict ={'followers': 'followers', 'following': 'following', 'public_repos': 'public_repos', 'public_gists': 'public_gists', 'star_count': 'starred_url', 'subscription_count': 'subscriptions_url', 'organization_count': 'organizations_url'}
        cols_df = pd.DataFrame(cols_dict.items(), columns=['col_name', 'col_url'])
        cols = cols_df.col_name.tolist()
        reverse_cols = cols[::-1]
        cols_df.to_csv("../data/metadata_files/user_url_cols.csv", index=False)
    for index, row in cols_df.iterrows():
        if (row['col_name'] not in core_users.columns) or (core_users[core_users[row.col_name].isna()].shape[0] > 0):
            if 'url' in row.col_url:
                print(f'Getting {row.col_name} for core users')
                core_users = get_counts(core_users, row.col_url, row.col_name, overwrite_existing_temp_files=False)
                core_users.to_csv(core_users_path, index=False)
            else:
                print(f'Issues with {row.col_name} for core users')
                