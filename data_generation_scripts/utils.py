import re
import time
from time import sleep
import pandas as pd
import requests
import apikey
import os
import math
import shutil
from tqdm import tqdm


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def check_rate_limit():
    """Function to check rate limit status
    :return: data from rate limit api call"""
    # Checks for rate limit so that you don't hit issues with Github API. Mostly for search API that has a 30 requests per minute https://docs.github.com/en/rest/rate-limit
    url = 'https://api.github.com/rate_limit'
    response = requests.get(url, headers=auth_headers)
    rates_df = pd.json_normalize(response.json())
    return rates_df

def check_total_pages(url):
    # Check total number of pages to get from search. Useful for not going over rate limit
    response = requests.get(f'{url}?per_page=1', headers=auth_headers)
    if response.status_code != 200:
        print('hit rate limiting. trying to sleep...')
        time.sleep(120)
        response = requests.get(url, headers=auth_headers)
        total_pages = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
    else:
        total_pages = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
    return total_pages

def check_total_results(url):
    """Function to check total number of results from API. Useful for not going over rate limit. Differs from check_total_pages because this returns all results, not just total number of pagination."""
    response = requests.get(url, headers=auth_headers)
    if response.status_code != 200:
        print('hit rate limiting. trying to sleep...')
        time.sleep(120)
        response = requests.get(url, headers=auth_headers)
        data = response.json()
    else:
        data = response.json()
    return data['total_count']

def get_response_data(response, query):
    """Function to get response data from api call
    :param response: response from api call
    :param query: query used to make api call
    :return: response data"""
    if response.status_code != 200:
        if response.status_code == 401:
            print("response code 401 - unauthorized access. check api key")
        else:
            print(f'response code: {response.status_code}. hit rate limiting. trying to sleep...')
            time.sleep(120)
            response = requests.get(query, headers=auth_headers)
            if response.status_code != 200:
                print(f'query failed twice with code {response.status_code}. Failing URL: {query}')
                rates_df = check_rate_limit()
                if rates_df['resources.core.remaining'].values[0] == 0:
                    print('rate limit reached. sleeping for 1 hour')
                    time.sleep(3600)
                    response = requests.get(query, headers=auth_headers)
                    if response.status_code != 200:
                        print(f'query failed third time with code {response.status_code}. Failing URL: {query}')
                        response_data = []
                    else:
                        response_data = response.json()
                else:
                    response_data = []
            else:
                response_data = response.json()
    else:
        response_data = response.json()
    
    return response_data

# def get_api_data(query):
#     # Thanks https://stackoverflow.com/questions/33878019/how-to-get-data-from-all-pages-in-github-api-with-python

#     try:
#         response = requests.get(f"{query}", headers=auth_headers)
#         if response.status_code != 200:
#             time.sleep(200)
#             response = requests.get(f"{query}", headers=auth_headers)
#         response_data = response.json()

#         while "next" in response.links.keys():
#             url = response.links['next']['url']
#             response = requests.get(url, headers=auth_headers)
#             if response.status_code != 200:
#                 time.sleep(200)
#                 response = requests.get(url, headers=auth_headers)
#             response_data.extend(response.json())
            
#     except:
#         print(f"Error with URL: {url}")

#     return response_data

def read_combine_files(dir_path):
    """Function to get combined users dataframe. Run this after all users have been added to the temp directory
    :param dir_path: path to users temp folder
    :return: combined users dataframe"""
    rows = []
    for subdir, _, files in os.walk(dir_path):
        for f in files:
            try:
                temp_df = pd.read_csv(subdir + '/' + f)
                rows.append(temp_df)
            except pd.errors.EmptyDataError:
                print(f'Empty dataframe for {f}')
    combined_df = pd.concat(rows) if len(rows) > 0 else pd.DataFrame()
    return combined_df

def get_new_users(potential_new_users_df, temp_users_dir, users_progress_bar):
    """Function to get new users from the users file
    :param potential_new_users_df: dataframe of new identified users
    :param temp_users_dir: path to temp users directory
    :param users_progress_bar: progress bar for users (Not sure this is working though)
    :return: new users dataframe
    """
    # Check if temp users directory exists. If it does, delete it and recreate it. Otherwise create it.
    user_cols = pd.read_csv('../data/metadat_files/users_dataset_cols.csv')
    if os.path.exists(temp_users_dir):
        shutil.rmtree(temp_users_dir)
        os.makedirs(temp_users_dir)  
    else:
        os.makedirs(temp_users_dir)
   
    # Check if users error files exists. If it does, delete it. Otherwise create it. This does mean that we might try and get the same user more than once, and it will error out each time. We could change this to being time based or query based error, but for now leaving this as is.
    error_file_path = "../data/error_logs/potential_users_errors.csv"
    if os.path.exists(error_file_path):
        os.remove(error_file_path)
    # Update and refresh progress bar with the length of the potential new users dataframe
    users_progress_bar.total = len(potential_new_users_df)
    users_progress_bar.refresh()
    # Loop through each user in the potential new users dataframe
    for _, user_row in potential_new_users_df.iterrows():
        try:
        
            # Create temporary file name for user
            temp_users_path = f"{user_row.login.replace('/', '')}_potential_users.csv"
            expanded_response = requests.get(user_row.url, headers=auth_headers)
            expanded_response_data = get_response_data(expanded_response, user_row.url)
            if len(expanded_response_data) == 0:
                users_progress_bar.update(1)
                continue
            expanded_df = pd.json_normalize(expanded_response_data)
            # Only get the columns we want from the user dataframe. Primarily do this because we will get much more data for our user profiles than any other ones
            expanded_df = expanded_df[user_cols.columns]
            expanded_df.to_csv(temp_users_dir+temp_users_path, index=False)
            # Only continue if sufficient rate limit
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
            users_progress_bar.update(1)
        except:
            print(f"Error on getting users for {user_row.login}")
            error_df = pd.DataFrame([{'login': user_row.login, 'error_time': time.time()}])
            
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            users_progress_bar.update(1)
            continue
    # Combine all users in temp directory into one dataframe
    new_users_df = read_combine_files(temp_users_dir)
    # Delete temp directory
    shutil.rmtree(temp_users_dir)
    return new_users_df

def check_add_users(potential_new_users_df, users_output_path, temp_users_dir, users_progress_bar, return_df):
    """Function to check if users are already in the users file and add them if not (Might need to add this to utils.py)
    :param potential_new_users_df: dataframe of new identified users
    :param users_output_path: path to users file
    :param temp_users_dir: path to temp users directory to store initial files
    :param users_progress_bar: progress bar for users (Not sure this is working though)
    :param return_df: boolean to return the dataframe or not
    """
    if os.path.exists(users_output_path):
        users_df = pd.read_csv(users_output_path)
        new_users_df = potential_new_users_df[~potential_new_users_df.login.isin(users_df.login)]
        if len(new_users_df) > 0:
            expanded_new_users = get_new_users(new_users_df, temp_users_dir, users_progress_bar)
        else:
            expanded_new_users = new_users_df
        users_df = pd.concat([users_df, expanded_new_users])
        users_df = users_df.drop_duplicates(subset=['login', 'id'])
        users_df.to_csv(users_output_path, index=False)
    else:
        users_df = get_new_users(potential_new_users_df, temp_users_dir, users_progress_bar)
        users_df.to_csv(users_output_path, index=False)
    if return_df:
        return users_df
    

def check_add_repo(potential_new_repo_df, repo_output_path):
    """Function to check if repo are already in the repo file and add them if not (Might need to add this to utils.py)
    :param potential_new_repo_df: dataframe of contributors
    :param repo_output_path: path to repo file
    """
    if os.path.exists(repo_output_path):
        repo_df = pd.read_csv(repo_output_path)
        new_repo_df = potential_new_repo_df[~potential_new_repo_df.id.isin(repo_df.id)]
        repo_df = pd.concat([repo_df, new_repo_df])
        repo_df.to_csv(repo_output_path, index=False)
    else:
        repo_df = potential_new_repo_df
        repo_df.to_csv(repo_output_path, index=False)
    return repo_df

def get_repo_df(output_path):
    """Function to get repo dataframe
    :param output_path: path to output file
    :return: repo dataframe"""
    repo_df = pd.read_csv(output_path)
    return repo_df

def get_user_df(output_path):
    """Function to get user dataframe
    :param output_path: path to output file
    :return: user dataframe"""
    user_df = pd.read_csv(output_path)
    return user_df

def check_return_error_file(error_file_path):
    """Function to check if error file exists and return it if it does
    :param error_file_path: path to error file
    :return: error dataframe"""
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        return error_df
    else:
        return []