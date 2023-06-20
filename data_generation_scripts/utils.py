import re
import time
import pandas as pd
import requests
import apikey
import os
import math
import shutil
from tqdm import tqdm
from datetime import datetime
import altair as alt
import warnings
warnings.filterwarnings('ignore')
import vl_convert as vlc
from typing import Optional, List, Any
import numpy as np



auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

"""Rate Limit and Response Functions
1. Check rate limit
2. Check total pages
3. Check total results
4. Get response data"""

def check_rate_limit() -> pd.DataFrame:
    """Function to check rate limit status
    :return: data from rate limit api call"""
    # Checks for rate limit so that you don't hit issues with Github API. Mostly for search API that has a 30 requests per minute https://docs.github.com/en/rest/rate-limit
    url = 'https://api.github.com/rate_limit'
    response = requests.get(url, headers=auth_headers, timeout=10)
    if response.status_code != 200:
        print(f'Failed to retrieve rate limit with status code: {response.status_code}')
        return pd.DataFrame()
    rates_df = pd.json_normalize(response.json())
    return rates_df

def check_total_pages(url: str, auth_headers: dict) -> int:
    # Check total number of pages to get from search. Useful for not going over rate limit
    response = requests.get(f'{url}?per_page=1', headers=auth_headers, timeout=10)
    if response.status_code != 200:
        print('hit rate limiting. trying to sleep...')
        time.sleep(120)
        response = requests.get(url, headers=auth_headers, timeout=10)
        if response.status_code != 200:
            rates_df = check_rate_limit()
            if rates_df['resources.core.remaining'].values[0] == 0:
                print('rate limit reached. sleeping for 1 hour')
                time.sleep(3600)
                response = requests.get(url, headers=auth_headers, timeout=10)
                if response.status_code != 200:
                    print(f'query failed third time with code {response.status_code}. Failing URL: {url}')
                    total_pages = 1
    if len(response.links) == 0:
        total_pages = 1
    else:
        match = re.search(r'\d+$', response.links['last']['url'])
        total_pages = match.group() if match is not None else 1
    return int(total_pages)


def check_total_results(url: str) -> Optional[int]:
    """Function to check total number of results from API. Useful for not going over rate limit. Differs from check_total_pages because this returns all results, not just total number of pagination.

    :param url: URL to check.
    :type url: str
    :return: Total count of results.
    :rtype: int, optional
    """
    response = requests.get(url, headers=auth_headers, timeout=10)
    if response.status_code != 200:
        print('hit rate limiting. trying to sleep...')
        time.sleep(120)
        response = requests.get(url, headers=auth_headers, timeout=10)
        if response.status_code != 200:
            rates_df = check_rate_limit()
            if rates_df['resources.core.remaining'].values[0] == 0:
                print('rate limit reached. sleeping for 1 hour')
                time.sleep(3600)
                response = requests.get(url, headers=auth_headers, timeout=10)
                if response.status_code != 200:
                    print(f'query failed third time with code {response.status_code}. Failing URL: {url}')
                    data = {'total_count': None}
        data = response.json()
    else:
        data = response.json()
    return data['total_count']

def get_response_data(response, query):
    """Function to get and process response data from GitHub API call
    :param response: response from initial API call
    :param query: query used to make initial API call
    :return: response data"""
    # First, check if response is valid
    if response.status_code == 401:
        print("response code 401 - unauthorized access. check api key")
        return []
    elif response.status_code == 204:
        print(f'No data for {query}')
        return []
    elif response.status_code == 200:
        return response.json()
    else:
        print(f'response code: {response.status_code}. hit rate limiting. trying to sleep...')
        time.sleep(10)
        response = requests.get(query, headers=auth_headers, timeout=10)

        # Check if response is valid a second time after sleeping
        if response.status_code != 200:
            print(f'query failed twice with code {response.status_code}. Failing URL: {query}')

            # If failed again, check the rate limit and sleep for the amount of time needed to reset rate limit
            rates_df = check_rate_limit()
            if rates_df['resources.core.remaining'].values[0] == 0:
                print('rate limit reached. sleeping for 1 hour')
                time.sleep(3600)
                response = requests.get(query, headers=auth_headers, timeout=10)
                if response.status_code != 200:
                    print(f'query failed third time with code {response.status_code}. Failing URL: {query}')
                    return []
                else:
                    return response.json()
            else:
                return response.json()

"""Manipulate Files Functions
1. Read csv file
2. Read and combine files
2. Check return error file
3. Check if older file exists and move it to archive
4. Check if older entity files exist and grab any missing entities to add to our most recent file"""

def read_csv_file(directory: str, file_name: str, file_path_contains: Optional[str]) -> Optional[pd.DataFrame]:
    """Reads a CSV file into a pandas DataFrame.
    
    :param directory: Directory of the file.
    :type directory: str
    :param file_name: Name of the file.
    :type file_name: str
    :param file_path_contains: Only files containing this string will be read.
    :type file_path_contains: str, optional
    :return: Data from the file as a pandas DataFrame, or None if the file is empty or does not contain the specified string.
    :rtype: pandas.DataFrame, optional
    """
    if file_path_contains is None or file_path_contains in file_name:
        try:
            return pd.read_csv(os.path.join(directory, file_name), low_memory=False, encoding='utf-8')
        except pd.errors.EmptyDataError:
            print(f'Empty dataframe for {file_name}')
            return None
    return None

def read_combine_files(dir_path: str, check_all_dirs: bool=False, file_path_contains: Optional[str]=None, large_files: bool=False) -> pd.DataFrame:
    """Combines all files in a directory.
    
    :param dir_path: Path to directory with files.
    :type dir_path: str
    :param check_all_dirs: Whether to check all directories.
    :type check_all_dirs: bool
    :param file_path_contains: Only files containing this string will be read.
    :type file_path_contains: str, optional
    :param large_files: Whether to treat the files as large or not.
    :type large_files: bool, optional
    :return: Combined data from all relevant files.
    :rtype: pandas.DataFrame
    """
    excluded_dirs = ['temp', 'derived_files', 'metadata_files', 'repo_data', 'user_data', 'derived_files', 'archived_data', 'error_logs', 'archived_files']
    rows = []
    relevant_files = []
    for directory, _, files in os.walk(dir_path):
        if check_all_dirs:
            if directory == '../data':
                continue
            excluded_exists = [excluded_dir for excluded_dir in excluded_dirs if excluded_dir in directory]
            if len(excluded_exists) == 0:
                for file_name in files:
                    
                    if large_files:
                        file_dict = {}
                        loaded_file = os.path.join(directory, file_name)
                        file_size = os.path.getsize(loaded_file)
                        file_dict['file_name'] = file_name
                        file_dict['file_size'] = file_size
                        file_dict['directory'] = directory
                        relevant_files.append(file_dict)
                    else:
                        row = read_csv_file(directory, file_name, file_path_contains)
                        if row is not None:
                            rows.append(row)
        else:
            for file_name in files:
                if large_files:
                    file_dict = {}
                    loaded_file = os.path.join(directory, file_name)
                    file_size = os.path.getsize(loaded_file)
                    file_dict['file_name'] = file_name
                    file_dict['file_size'] = file_size
                    file_dict['directory'] = directory
                    relevant_files.append(file_dict)
                else:
                    row = read_csv_file(directory, file_name, file_path_contains)
                    if row is not None:
                        rows.append(row)
    if large_files:
        files_df = pd.DataFrame(relevant_files)
        files_df['date'] = "202" + files_df['file_name'].str.split('202').str[1].str.split('.').str[0]
        files_df.date = files_df.date.str.replace("_", "-")
        files_df.date = pd.to_datetime(files_df.date)
        top_files = files_df.sort_values(by=['file_size', 'date'], ascending=[False, False]).head(2)
        rows = []
        for _, row in top_files.iterrows():
            df = read_csv_file(row.directory, row.file_name, file_path_contains)
            if df is not None:
                rows.append(df)
    combined_df = pd.concat(rows) if len(rows) > 0 else pd.DataFrame()
    return combined_df


def check_return_error_file(error_file_path):
    """Function to check if error file exists and return it if it does
    :param error_file_path: path to error file
    :return: error dataframe"""
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        return error_df
    else:
        return pd.DataFrame()

def clean_write_error_file(error_file_path, drop_field):
    """Function to clean error file and write it
    :param error_file_path: path to error file
    :param drop_field: field to drop from error file"""
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        if 'error_time' in error_df.columns:
            error_df = error_df.sort_values(by=['error_time']).drop_duplicates(subset=[drop_field], keep='last')
        else: 
            error_df = error_df.drop_duplicates(subset=[drop_field], keep='last')
        error_df.to_csv(error_file_path, index=False)
    else:
        print('no error file to clean')

def check_if_older_file_exists(file_path):
    """Function to check if older file exists and move it to older_files folder
    :param file_path: path to file"""
    if os.path.exists(file_path):
        src = file_path 
        new_file_path = file_path.replace('/data/','/data/older_files/')
        time_stamp = datetime.now().strftime("%Y_%m_%d")
        dst = new_file_path.replace('.csv', f'_{time_stamp}.csv')
        
        new_dir = os.path.dirname(new_file_path)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)

        if not os.path.exists(dst):
            shutil.copy2(src, dst)  


def check_for_entity_in_older_queries(entity_path, entity_df, is_large=True):
    """Function to check if entity exists in older queries and add it to our most recent version of the file
    :param entity_path: path to entity file
    :param entity_df: entity dataframe"""
    entity_type = entity_path.split('/')[-1].split('_dataset')[0]

    older_entity_file_path = entity_path.replace('data/', 'data/older_files/')
    older_entity_file_dir = os.path.dirname(older_entity_file_path) + '/'


    older_entity_df = read_combine_files(dir_path=older_entity_file_dir, check_all_dirs=True, file_path_contains=entity_type, large_files=is_large)
    print(f'older entity df shape: {older_entity_df.shape}')
    if len(older_entity_df) > 0:
        entity_field = 'full_name' if entity_type == 'repos' else 'login'
        missing_entities = older_entity_df[~older_entity_df[entity_field].isin(entity_df[entity_field])]

        if entity_type == 'users':
            user_headers = pd.read_csv('../data/metadata_files/users_dataset_cols.csv')
            if set(missing_entities.columns) != set(user_headers.columns):
                error_file_path = "../data/error_logs/potential_users_errors.csv"
                error_df = check_return_error_file(error_file_path)
                now = pd.Timestamp('now')
                error_df['error_time'] = pd.to_datetime(error_df['error_time'], errors='coerce')
                error_df = error_df.dropna(subset=['error_time'])  # Drop any rows where 'error_time' is NaT
                error_df['time_since_error'] = (now - error_df['error_time']).dt.days
                error_df = error_df[error_df.time_since_error > 7]
                missing_entities = missing_entities[~missing_entities.login.isin(error_df.login)]
                missing_entities = missing_entities[user_headers.columns]
        if entity_type == 'repos':
            repo_headers = pd.read_csv('../data/metadata_files/repo_headers.csv')
            if set(missing_entities.columns) != set(repo_headers.columns):
                error_file_path = "../data/error_logs/potential_repos_errors.csv"
                error_df = check_return_error_file(error_file_path)
                now = pd.Timestamp('now')
                error_df['error_time'] = pd.to_datetime(error_df['error_time'], errors='coerce')
                error_df = error_df.dropna(subset=['error_time'])  # Drop any rows where 'error_time' is NaT
                error_df['time_since_error'] = (now - error_df['error_time']).dt.days
                error_df = error_df[error_df.time_since_error > 7]
                missing_entities = missing_entities[~missing_entities.full_name.isin(error_df.full_name)]
                missing_entities = missing_entities[repo_headers.columns]
        if len(missing_entities) > 0:
            missing_entities = missing_entities[missing_entities.id.notna()]
            entity_df = pd.concat([entity_df, missing_entities])
            cleaned_field = 'cleaned_repo_query_time' if entity_type == 'repos' else 'cleaned_user_query_time'
            time_field = 'repo_query_time' if entity_type == 'repos' else 'user_query_time'
            entity_df[cleaned_field] = pd.to_datetime(entity_df[time_field], errors='coerce')
            entity_field = 'full_name' if 'repo' in entity_type else 'login'
            entity_df = entity_df.sort_values(by=[cleaned_field], ascending=False).drop_duplicates(subset=[entity_field], keep='first').drop(columns=[cleaned_field])
    check_if_older_file_exists(entity_path)
    entity_df.to_csv(entity_path, index=False)
    return entity_df

def check_file_size_and_move(file_dir):
    """Function to check if file size is too large and move it
    :param file_dir: path to file directory"""
    for dir, _, files in os.walk(file_dir):
        for file in files:
            file_path = os.path.join(dir, file)
            size = os.path.getsize(file_path)
            size = round(size/(1024*1024), 2)
            if size > 50:
            
                new_file_path = file_path.replace('data/', 'data/large_files/')
            
                if not os.path.exists(new_file_path):
                    shutil.copy2(file_path, new_file_path)
                    os.remove(file_path)

def check_file_created(file_path, existing_df):
    df = pd.read_csv(file_path, low_memory=False)
    if len(df) == len(existing_df):
        return True
    else:
        print(f'File {file_path} not created correctly')
        return False

"""User Functions
1. Get new users data
2. Check add user
4. Get user"""

def get_new_users(potential_new_users_df, temp_users_dir, users_progress_bar,  error_file_path, overwrite_existing_temp_files = True):
    """Function to get new users from the users file
    :param potential_new_users_df: dataframe of new identified users
    :param temp_users_dir: path to temp users directory
    :param users_progress_bar: progress bar for users (Not sure this is working though)
    :param error_file_path: path to error file
    :param overwrite_existing_temp_files: boolean to overwrite existing temp files or not
    :return: new users dataframe
    """
    # Check if temp users directory exists. If it does, delete it and recreate it. Otherwise create it.
    user_cols = pd.read_csv('../data/metadata_files/users_dataset_cols.csv')
    
    if (os.path.exists(temp_users_dir)) and (overwrite_existing_temp_files):
        shutil.rmtree(temp_users_dir)
          
    if not os.path.exists(temp_users_dir):
        os.makedirs(temp_users_dir)
    # Update and refresh progress bar with the length of the potential new users dataframe
    users_progress_bar.total = len(potential_new_users_df)
    users_progress_bar.refresh()
    # Loop through each user in the potential new users dataframe
    for _, user_row in potential_new_users_df.iterrows():
        try:
        
            # Create temporary file name for user
            temp_users_path = f"{user_row.login.replace('/', '')}_potential_users.csv"
            if (os.path.exists(temp_users_dir + temp_users_path)):
                users_progress_bar.update(1)
                continue
            expanded_response = requests.get(user_row.url, headers=auth_headers)
            expanded_response_data = get_response_data(expanded_response, user_row.url)
            if expanded_response_data is None:
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
            users_progress_bar.total = users_progress_bar.total - 1
            error_df = pd.DataFrame([{'login': user_row.login, 'error_time': time.time(), 'error_url': user_row.url}])
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            # users_progress_bar.update(1)
            continue
    # Combine all users in temp directory into one dataframe
    new_users_df = read_combine_files(dir_path=temp_users_dir)
    users_progress_bar.close()
    # Delete temp directory
    if overwrite_existing_temp_files:
        shutil.rmtree(temp_users_dir)
    return new_users_df


def check_add_users(potential_new_users_df, users_output_path, return_df, overwrite_existing_temp_files):
    """Function to check if users are already in the users file and add them if not
    :param potential_new_users_df: dataframe of new identified users
    :param users_output_path: path to users file
    :param return_df: boolean to return the dataframe or not
    :param overwrite_existing_temp_files: boolean to overwrite existing temp files or not
    """
    # Also define temporary directory path for users
    temp_users_dir = "../data/temp/temp_users/"
    excluded_users = pd.read_csv('../data/metadata_files/excluded_users.csv')
    potential_new_users_df = potential_new_users_df[~potential_new_users_df.login.isin(excluded_users.login)]
    error_file_path = "../data/error_logs/potential_users_errors.csv"
    error_df = check_return_error_file(error_file_path)
    if os.path.exists(users_output_path):
        users_df = pd.read_csv(users_output_path)
        new_users_df = potential_new_users_df[~potential_new_users_df.login.isin(users_df.login)]
        print(f"Number of new users: {len(new_users_df)}")
        if len(error_df) > 0:
            new_users_df = new_users_df[~new_users_df.login.isin(error_df.login)]
        if len(new_users_df) > 0:
            users_progress_bar = tqdm(total=len(new_users_df), desc='Users', position=1)
            print(len(new_users_df))
            expanded_new_users = get_new_users(new_users_df, temp_users_dir, users_progress_bar, error_file_path, overwrite_existing_temp_files)
        else:
            expanded_new_users = new_users_df
        users_df = pd.concat([users_df, expanded_new_users])
        users_df = users_df.drop_duplicates(subset=['login', 'id'])
    else:
        new_users_df = potential_new_users_df.copy()
        users_progress_bar = tqdm(total=len(new_users_df), desc='Users', position=1)
        users_df = get_new_users(potential_new_users_df, temp_users_dir, users_progress_bar, error_file_path, overwrite_existing_temp_files)
    
    clean_write_error_file(error_file_path, 'login')
    check_if_older_file_exists(users_output_path)
    users_df['user_query_time'] = datetime.now().strftime("%Y-%m-%d")
    users_df.to_csv(users_output_path, index=False)
    # check_for_entity_in_older_queries(users_output_path, users_df)
    if return_df:
        users_df = get_user_df(users_output_path)
        return users_df

def get_user_df(output_path):
    """Function to get user dataframe
    :param output_path: path to output file
    :return: user dataframe"""
    user_df = pd.read_csv(output_path)
    return user_df

def combined_updated_users(user_output_path, updated_user_output_path, overwrite_existing_temp_files, return_df):
    if (os.path.exists(user_output_path)) and (os.path.exists(updated_user_output_path)):
        users_df = pd.read_csv(user_output_path, low_memory=False)
        check_if_older_file_exists(user_output_path)
        updated_user_df = pd.read_csv(updated_user_output_path, low_memory=False)
        existing_users_df = users_df[~users_df.login.isin(updated_user_df.login)]
        combined_users_df = pd.concat([updated_user_df, existing_users_df])
        combined_users_df = combined_users_df[updated_user_df.columns.tolist()]
        combined_users_df = combined_users_df.fillna(np.nan).replace([np.nan], [None])
        combined_users_df = combined_users_df.sort_values(by=['user_query_time']).drop_duplicates(subset=['login'], keep='first')
        cleaned_users_df = check_for_entity_in_older_queries(user_output_path, combined_users_df)
        if overwrite_existing_temp_files:
            double_check = check_file_created(user_output_path, cleaned_users_df)
            if double_check:
                os.remove(updated_user_output_path)
        if return_df:
            return combined_users_df
    

"""Repo Functions
1. Get new repos data
2. Check if repos are already in the repos file and add them if not
3. Get repo dataset"""
    
def get_new_repos(potential_new_repos_df, temp_repos_dir, repos_progress_bar,  error_file_path, overwrite_existing_temp_files=True):
    """Function to get new repos from the repos file (currently not using anywhere but keeping just in case we need it)
    :param potential_new_repos_df: dataframe of new identified repos
    :param temp_repos_dir: path to temp repos directory
    :param repos_progress_bar: progress bar for repos 
    :param error_file_path: path to error file
    :param overwrite_existing_temp_files: boolean to overwrite existing temp files or not
    :return: new repos dataframe
    """
    repo_headers = pd.read_csv('../data/metadata_files/repo_headers.csv')
    # Check if temp users directory exists. If it does, delete it and recreate it. Otherwise create it.
    if (os.path.exists(temp_repos_dir)) and (overwrite_existing_temp_files):
        shutil.rmtree(temp_repos_dir)

    if not os.path.exists(temp_repos_dir):
        os.makedirs(temp_repos_dir)  

    # Create temporary file name for repo
    repos_progress_bar.total = len(potential_new_repos_df)
    repos_progress_bar.refresh()

    for _, row in potential_new_repos_df.iterrows():
        try:
            # Create temporary file name for user
            temp_repos_path = f"{row.full_name.replace('/', '')}_potential_repos.csv"
            if os.path.exists(temp_repos_dir + temp_repos_path):
                repos_progress_bar.update(1)
                continue
            response = requests.get(row.url, headers=auth_headers)
            response_data = get_response_data(response, row.url)
            if response_data is None:
                repos_progress_bar.update(1)
                continue
            response_df = pd.json_normalize(response_data)
            missing_headers = list(set(repo_headers.columns.tolist()) - set(response_df.columns.tolist()))
            if len(missing_headers) > 0:
                merged_df = pd.concat([response_df, repo_headers])
                merged_df = merged_df[repo_headers.columns.tolist()]
                merged_df = merged_df.dropna(subset=['full_name'])
                response_df = merged_df[repo_headers.columns.tolist()]
            else:
                response_df = response_df[repo_headers.columns.tolist()]
                response_df = response_df[repo_headers.columns]
            response_df.to_csv(temp_repos_dir + temp_repos_path, index=False)
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
            repos_progress_bar.update(1)
        except:
            repos_progress_bar.total = repos_progress_bar.total - 1
            # print(f"Error on getting repo for {row.full_name}")
            error_df = pd.DataFrame([{'full_name': row.full_name, 'error_time': time.time(), 'error_url': row.url}])
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            # repos_progress_bar.update(1)
            continue
    new_repos_df = read_combine_files(dir_path =temp_repos_dir)
    new_repos_df = new_repos_df.drop_duplicates(subset=['full_name', 'id'])
    new_repos_df = new_repos_df[repo_headers.columns]
    repos_progress_bar.close()
    if overwrite_existing_temp_files:
        shutil.rmtree(temp_repos_dir)
    return new_repos_df

def check_add_repos(potential_new_repo_df, repo_output_path, return_df):
    """Function to check if repo are already in the repo file and add them if not 
    :param potential_new_repo_df: dataframe of contributors
    :param repo_output_path: path to repo file
    :param return_df: boolean to return dataframe or not
    :return: repo dataframe
    """
    error_file_path = '../data/error_logs/potential_repos_errors.csv'
    repo_headers = pd.read_csv('../data/metadata_files/repo_headers.csv')
    if os.path.exists(repo_output_path):
        repo_df = pd.read_csv(repo_output_path)
        print(f"Number of repos: {len(repo_df)}", time.time())
        new_repo_df = potential_new_repo_df[~potential_new_repo_df.id.isin(repo_df.id)]
        error_df = check_return_error_file(error_file_path)
        if len(error_df) > 0:
            new_repo_df = new_repo_df[~new_repo_df.full_name.isin(error_df.full_name)]
        print(f"Number of new repos: {len(new_repo_df)}", time.time())
        if len(new_repo_df) > 0:
            new_repo_df = new_repo_df[repo_headers.columns]
            repo_df = pd.concat([repo_df, new_repo_df])
            repo_df = repo_df.drop_duplicates(subset=['id'])
            print(f"Number of repos: {len(repo_df)}", time.time())
        else:
            repo_df = repo_df[repo_headers.columns]
            print(f"Number of repos: {len(repo_df)}", time.time(), "inner else statement")
    else:
        repo_df = potential_new_repo_df

    print(f"Number of repos: {len(repo_df)}, checking if older file exists", time.time())
    check_if_older_file_exists(repo_output_path)
    repo_df['repo_query_time'] = datetime.now().strftime("%Y-%m-%d")
    repo_df.to_csv(repo_output_path, index=False)
    print("Repo file updated", time.time())
    repo_df = check_for_entity_in_older_queries(repo_output_path, repo_df, is_large=True)
    if return_df:
        # repo_df = get_repo_df(repo_output_path)
        return repo_df

def get_repo_df(output_path):
    """Function to get repo dataframe
    :param output_path: path to output file
    :return: repo dataframe"""
    repo_df = pd.read_csv(output_path, low_memory=False)
    return repo_df

def combined_updated_repos(repo_output_path, updated_repo_output_path, overwrite_existing_temp_files, return_df):
    if (os.path.exists(repo_output_path)) and (os.path.exists(updated_repo_output_path)):
        repos_df = pd.read_csv(repo_output_path, low_memory=False)
        check_if_older_file_exists(repo_output_path)
        updated_repo_df = pd.read_csv(updated_repo_output_path, low_memory=False)
        existing_repos_df = repos_df[~repos_df.full_name.isin(updated_repo_df.full_name)]
        combined_repos_df = pd.concat([updated_repo_df, existing_repos_df])
        combined_repos_df = combined_repos_df[updated_repo_df.columns.tolist()]
        combined_repos_df = combined_repos_df.fillna(np.nan).replace([np.nan], [None])
        combined_repos_df = combined_repos_df.sort_values(by=['repo_query_time']).drop_duplicates(subset=['full_name'], keep='first')
        cleaned_repos_df = check_for_entity_in_older_queries(repo_output_path, combined_repos_df)
        if overwrite_existing_temp_files:
            double_check = check_file_created(repo_output_path, cleaned_repos_df)
            if double_check:
                os.remove(updated_repo_output_path)
        if return_df:
            return combined_repos_df

"""Org Functions"""

def get_orgs(org_df, org_output_path, error_file_path, overwrite_existing_temp_files):
    temp_org_dir = f"../data/temp/{org_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    
    if (os.path.exists(temp_org_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_org_dir)
    
    if not os.path.exists(temp_org_dir):
        os.makedirs(temp_org_dir)
    org_progress_bar = tqdm(total=len(org_df), desc="Cleaning Orgs", position=0)
    org_headers = pd.read_csv('../data/metadata_files/org_headers.csv')
    for _, row in org_df.iterrows():
        try:
            # Create the temporary directory path to store the data
            temp_org_path =  F"{row.login.replace('/','')}_org.csv"

            # Check if the org_df has already been saved to the temporary directory
            if os.path.exists(temp_org_dir + temp_org_path):
                org_progress_bar.update(1)
                continue
            # Create the url to get the org
            url = row.url.replace('/users/', '/orgs/')

            # Make the first request
            response = requests.get(url, headers=auth_headers)
            response_data = get_response_data(response, url)
            if response_data is None:
                response_df = pd.read_csv('../data/metadata_files/org_headers.csv')
            else:
                response_df = pd.json_normalize(response_data)
            response_df = response_df[org_headers.columns]
            response_df.to_csv(temp_org_dir + temp_org_path, index=False)
            org_progress_bar.update(1)
        except:
            org_progress_bar.total = org_progress_bar.total - 1
            # print(f"Error on getting orgs for {row.login}")
            error_df = pd.DataFrame([{'login': row.login, 'error_time': time.time(), 'error_url': row.url}])
            
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            org_progress_bar.update(1)
            continue
    org_df = read_combine_files(dir_path=temp_org_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_org_dir)
    # Close the progress bars
    org_progress_bar.close()
    return org_df

def check_add_orgs(potential_new_org_df, org_output_path, return_df, overwrite_existing_temp_files):
    """Function to check if orgs are already in the org file and add them if not 
    :param org_df: dataframe of orgs
    :param org_output_path: path to org file
    :param return_df: boolean to return dataframe or not
    :return: org dataframe
    """
    error_file_path = '../data/error_logs/org_errors.csv'
    # org_headers = pd.read_csv('../data/metadata_files/org_headers.csv')
    if os.path.exists(org_output_path):
        org_df = pd.read_csv(org_output_path)
        new_org_df = potential_new_org_df[~potential_new_org_df.login.isin(org_df.login)]
        error_df = check_return_error_file(error_file_path)
        if len(error_df) > 0:
            new_org_df = new_org_df[~new_org_df.login.isin(error_df.login)]
        if len(new_org_df) > 0:
            # new_org_df = new_org_df[org_headers.columns]
            cleaned_orgs = get_orgs(new_org_df, org_output_path, error_file_path, overwrite_existing_temp_files)
            org_df = pd.concat([org_df, cleaned_orgs])
            org_df = org_df.drop_duplicates(subset=['id'])
    else:
        org_df = get_orgs(potential_new_org_df, org_output_path, error_file_path, overwrite_existing_temp_files)

    check_if_older_file_exists(org_output_path)
    org_df['org_query_time'] = datetime.now().strftime("%Y-%m-%d")
    org_df.to_csv(org_output_path, index=False)
    check_for_entity_in_older_queries(org_output_path, org_df)
    if return_df:
        org_df = get_org_df(org_output_path)
        return org_df

def get_org_df(output_path):
    """Function to get org dataframe
    :param output_path: path to output file
    :return: org dataframe"""
    org_df = pd.read_csv(output_path, low_memory=False)
    return org_df

def combined_updated_orgs(org_output_path, updated_org_output_path, overwrite_existing_temp_files, return_df):
    if (os.path.exists(org_output_path)) and (os.path.exists(updated_org_output_path)):
        orgs_df = pd.read_csv(org_output_path, low_memory=False)
        check_if_older_file_exists(org_output_path)
        updated_org_df = pd.read_csv(updated_org_output_path, low_memory=False)
        existing_orgs_df = orgs_df[~orgs_df.login.isin(updated_org_df.login)]
        combined_orgs_df = pd.concat([updated_org_df, existing_orgs_df])
        combined_orgs_df = combined_orgs_df[updated_org_df.columns.tolist()]
        combined_orgs_df = combined_orgs_df.fillna(np.nan).replace([np.nan], [None])
        combined_orgs_df = combined_orgs_df.sort_values(by=['org_query_time']).drop_duplicates(subset=['login'], keep='first')
        cleaned_orgs_df = check_for_entity_in_older_queries(org_output_path, combined_orgs_df)
        if overwrite_existing_temp_files:
            double_check = check_file_created(org_output_path, cleaned_orgs_df)
            if double_check:
                os.remove(updated_org_output_path)
        if return_df:
            return combined_orgs_df

"""Join Functions
1. Check if older join entities exist and add them to our latest join"""

def get_missing_entries(df: pd.DataFrame, older_df: pd.DataFrame, subset_fields: List) -> pd.DataFrame:
    """Function to check for missing entries in a dataframe
    :param df: dataframe
    :param older_df: older dataframe
    :param subset_fields: fields to subset on
    :return: missing values dataframe"""
    merged_df = pd.merge(df[subset_fields], older_df[subset_fields], on=subset_fields, how='outer', indicator=True)
    missing_values = merged_df[merged_df._merge == 'right_only']
    double_check = missing_values[subset_fields]
    combined_condition = np.ones(len(older_df), dtype=bool)
    for field in subset_fields:
        combined_condition = combined_condition & older_df[field].isin(double_check[field])
    older_df['double_check'] = np.where(combined_condition, 1, 0)
    final_missing_values = older_df[(older_df.double_check == 1) & (older_df[subset_fields[0]].isin(double_check[subset_fields[0]]))]
    if len(final_missing_values) > 0:
        final_missing_values = final_missing_values.drop(columns=['double_check'])
        return final_missing_values
    else:
        return pd.DataFrame()



def check_for_joins_in_older_queries(join_file_path: str, join_files_df: pd.DataFrame, join_unique_field: str, filter_fields: List, subset_terms: Optional[List]=[], is_large: bool=False) -> pd.DataFrame:
    """Function to check if joins exist in older queries and add them to our most recent version of the file
    :param join_file_path: path to join file
    :param join_files_df: join dataframe
    :param join_unique_field: unique field to join on
    :param filter_fields: fields to filter on
    :param is_large: boolean to check if file is large or not
    :return: join dataframe"""
    # Needs to check if older repos exist and then find their values in older join_files_df
    join_type = join_file_path.split("/")[-1].split("_dataset")[0]

    older_join_file_path = join_file_path.replace("data/", "data/older_files/")
    older_join_file_dir = os.path.dirname(older_join_file_path) + "/"

    older_join_df = read_combine_files(dir_path=older_join_file_dir, check_all_dirs=True, file_path_contains=join_type, large_files=is_large) 
    
    # entity_type = "" if "search" in join_file_path else "repo" if "repo" in join_file_path else "user"
    entity_type = join_file_path.split('/')[-1].split('_')[0]

    if 'comments' in join_file_path:
        entity_type = "repo"

    if "search" in join_file_path:
        entity_type = ""

    if "search" in join_file_path:
        join_files_df = join_files_df[join_files_df.search_term_source.isin(subset_terms)]
        older_join_df = older_join_df[older_join_df.search_term_source.isin(subset_terms)]

    if len(older_join_df) > 0:
        if join_unique_field in older_join_df.columns:
            older_join_df = older_join_df[older_join_df[join_unique_field].notna()]
            
            combined_join_df = pd.concat([join_files_df, older_join_df])
            time_field = 'search_query_time' if 'search_query' in join_unique_field else f'{entity_type}_query_time'
            cleaned_field = 'cleaned_search_query_time' if 'search_query' in join_unique_field else f'cleaned_{entity_type}_query_time'
            combined_join_df[cleaned_field] = None
            combined_join_df.loc[combined_join_df[time_field].isna(), cleaned_field] = '2022-10-10'
            combined_join_df[cleaned_field] = pd.to_datetime(combined_join_df[time_field], errors='coerce')
            combined_join_df = combined_join_df.sort_values(by=[cleaned_field]).drop_duplicates(subset=filter_fields, keep='first').drop(columns=[cleaned_field])

            missing_values = get_missing_entries(join_files_df, older_join_df, filter_fields)

            if len(missing_values) > 0:
                join_files_df = pd.concat([join_files_df, missing_values])
                # join_files_df.to_csv(join_file_path, index=False)

    return join_files_df

def get_core_users_repos():
    """Function to get core users and repos
    :return: core users and repos
    """
    core_repos_path = "../data/derived_files/core_repos.csv"
    if os.path.exists(core_repos_path):
        core_repos = pd.read_csv(core_repos_path)
        
    else:
        repo_df = pd.read_csv("../data/large_files/entity_files/repos_dataset.csv", low_memory=False)
        search_queries_repo_join_df = pd.read_csv("../data/join_files/search_queries_repo_join_dataset.csv")
        core_repos = repo_df[repo_df["id"].isin(search_queries_repo_join_df["id"].unique())]
    core_users = pd.read_csv("../data/derived_files/core_users.csv")
    return core_users, core_repos


def save_chart(chart, filename, scale_factor=1):
    '''
    Save an Altair chart using vl-convert
    
    Parameters
    ----------
    chart : altair.Chart
        Altair chart to save
    filename : str
        The path to save the chart to
    scale_factor: int or float
        The factor to scale the image resolution by.
        E.g. A value of `2` means two times the default resolution.
    '''
    with alt.data_transformers.enable("default"), alt.data_transformers.disable_max_rows():
        if filename.split('.')[-1] == 'svg':
            with open(filename, "w") as f:
                f.write(vlc.vegalite_to_svg(chart.to_dict()))
        elif filename.split('.')[-1] == 'png':
            with open(filename, "wb") as f:
                f.write(vlc.vegalite_to_png(chart.to_dict(), scale=scale_factor))
        else:
            raise ValueError("Only svg and png formats are supported")