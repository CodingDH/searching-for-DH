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

def get_total_stars(response, query):
    """Function to get response data from api call
    :param response: response from api call
    :param query: query used to make api call
    :return: response data"""
    # Check if response is valid
    total_stars = 0
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
                        total_stars = process_response(response)
            else:
                total_stars = process_response(response)
    else:
        total_stars = process_response(response)
    return total_stars


def get_user_stars(row):
    """Function to get total stars for each user"""
    url = f"{row.starred_url.split('{')[0]}?per_page=1"
    response = requests.get(url, headers=auth_headers)
    total_stars = get_total_stars(response, url)
    row['star_count'] = total_stars
    if row.name == 0:
        pd.DataFrame(row).T.to_csv('../data/temp/star_counts.csv', header=True, index=False)
    else:
        pd.DataFrame(row).T.to_csv('../data/temp/star_counts.csv', mode='a', header=False, index=False)
    return row


def check_total_stars(user_df):
    """Function to check total stars for each user
    :param user_df: dataframe of users
    :return: dataframe of users with total stars"""
    tqdm.pandas(desc="Getting total stars for each user")
    user_df = user_df.reset_index(drop=True)
    user_df = user_df.progress_apply(get_user_stars, axis=1)
    return user_df

if __name__ == "__main__":
    core_users, core_repos = get_core_users_repos()
    user_df = check_total_stars(core_users)
    user_df.to_csv('../data/derived_files/core_users.csv', index=False)