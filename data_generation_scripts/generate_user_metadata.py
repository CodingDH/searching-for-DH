import apikey
from tqdm import tqdm
import re
import time
import requests
import sys
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
sys.path.append("..")
from data_generation_scripts.utils import check_rate_limit, get_core_users_repos

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


def get_user_results(row, count_field, url_field):
    """Function to get total results for each user"""

    url = f"{row[url_field].split('{')[0]}?per_page=1"
    response = requests.get(url, headers=auth_headers)
    total_results = get_total_results(response, url)
    row[count_field] = total_results
    if row.name == 0:
        pd.DataFrame(row).T.to_csv(f'../data/temp/{count_field}.csv', header=True, index=False)
    else:
        pd.DataFrame(row).T.to_csv(f'../data/temp/{count_field}.csv', mode='a', header=False, index=False)
    return row


def check_total_results(user_df, count_field, url_field):
    """Function to check total results for each user
    :param user_df: dataframe of users
    :return: dataframe of users with total results"""
    tqdm.pandas(desc="Getting total results for each user")
    user_df = user_df.reset_index(drop=True)
    user_df = user_df.progress_apply(get_user_results, axis=1, count_field=count_field, url_field=url_field)
    return user_df

if __name__ == "__main__":
    core_users, core_repos = get_core_users_repos()
    user_df = check_total_results(core_users, 'star_count')
    user_df.to_csv('../data/derived_files/core_users.csv', index=False)