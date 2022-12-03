import apikey
from tqdm import tqdm
import re
import time
import requests
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.append("..")
from data_generation_scripts.utils import check_rate_limit

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}',
                'User-Agent': 'request'}

def check_total_stars(response, query):
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
                    response = requests.get(query, headers=auth_headers)
                    if response.status_code != 200:
                        print(
                            f'query failed third time with code {response.status_code}. Failing URL: {query}')
                    else:
                        total_stars = 0 if len(response.links) == 0 else re.search(
                            '\d+$', response.links['last']['url']).group()
            else:
                total_stars = 0 if len(response.links) == 0 else re.search(
                    '\d+$', response.links['last']['url']).group()
    else:
        total_stars = 0 if len(response.links) == 0 else re.search(
            '\d+$', response.links['last']['url']).group()

    return total_stars


def get_user_stars(row):
    """Function to get total stars for each user"""
    url = f"{row.starred_url.split('{')[0]}?per_page=1"
    response = requests.get(url, headers=auth_headers)
    total_stars = check_total_stars(response, url)
    row['star_count'] = total_stars
    return row


def check_total_stars(user_df):
    """Function to check total stars for each user
    :param user_df: dataframe of users
    :return: dataframe of users with total stars"""
    tqdm.pandas("Getting total stars for each user")
    user_df = user_df.progress_apply(get_user_stars, axis=1)
    return user_df