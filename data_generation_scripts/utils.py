from hashlib import new
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
from datetime import datetime


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

"""Rate Limit and Response Functions
1. Check rate limit
2. Check total pages
3. Check total results
4. Get response data"""

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
    # Check if response is valid
    response_data = []
    if response.status_code != 200:
        if response.status_code == 401:
            print("response code 401 - unauthorized access. check api key")
        elif response.status_code == 204:
            print(f'No data for {query}')
        else:
            print(f'response code: {response.status_code}. hit rate limiting. trying to sleep...')
            time.sleep(120)
            response = requests.get(query, headers=auth_headers)

            # Check if response is valid a second time after sleeping
            if response.status_code != 200:
                print(f'query failed twice with code {response.status_code}. Failing URL: {query}')

                # If failed again, check the rate limit and sleep for the amount of time needed to reset rate limit
                rates_df = check_rate_limit()
                if rates_df['resources.core.remaining'].values[0] == 0:
                    print('rate limit reached. sleeping for 1 hour')
                    time.sleep(3600)
                    response = requests.get(query, headers=auth_headers)
                    if response.status_code != 200:
                        print(f'query failed third time with code {response.status_code}. Failing URL: {query}')
                    else:
                        response_data = response.json()
            else:
                response_data = response.json()
    else:
        response_data = response.json()
    
    return response_data

"""Manipulate Files Functions
1. Read and combine files
2. Check return error file
3. Check if older file exists and move it to archive
4. Check if older entity files exist and grab any missing entities to add to our most recent file"""

def read_combine_files(dir_path, file_path_contains=None):
    """Function to get combined users dataframe. Run this after all users have been added to the temp directory
    :param dir_path: path to users temp folder
    :param file_path_contains: string to search for in file path (useful for join files that all exist in same repo)
    :return: combined users dataframe"""
    rows = []
    for subdir, _, files in os.walk(dir_path):
        for f in files:
            try:
                if file_path_contains is not None:
                    if file_path_contains in f:
                        rows.append(pd.read_csv(os.path.join(subdir, f), low_memory=False))
                else:
                    rows.append(pd.read_csv(os.path.join(subdir, f), low_memory=False))
            except pd.errors.EmptyDataError:
                print(f'Empty dataframe for {f}')
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


def check_for_entity_in_older_queries(entity_path, entity_df):
    """Function to check if entity exists in older queries and add it to our most recent version of the file
    :param entity_path: path to entity file
    :param entity_df: entity dataframe"""
    entity_type = entity_path.split('/')[-1].split('_dataset')[0]

    older_entity_file_path = entity_path.replace('data/', 'data/older_files/')
    older_entity_file_dir = os.path.dirname(older_entity_file_path) + '/'

    older_entity_df = read_combine_files(older_entity_file_dir, entity_type)
    if len(older_entity_df) > 0:
        missing_entities = older_entity_df[~older_entity_df.id.isin(entity_df.id)]

        if entity_type == 'users':
            user_headers = pd.read_csv('../data/metadata_files/users_dataset_cols.csv')
            if set(missing_entities.columns) != set(user_headers.columns):
                error_file_path = "../data/error_logs/potential_users_errors.csv"
                error_df = check_return_error_file(error_file_path)
                now = pd.Timestamp('now')
                error_df['error_time'] = pd.to_datetime(error_df['error_time'])
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
                error_df['error_time'] = pd.to_datetime(error_df['error_time'])
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
            entity_df = entity_df.sort_values(by=[cleaned_field]).drop_duplicates(subset=['id'], keep='first').drop(columns=[cleaned_field])
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
            users_progress_bar.total = users_progress_bar.total - 1
            error_df = pd.DataFrame([{'login': user_row.login, 'error_time': time.time(), 'error_url': user_row.url}])
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            # users_progress_bar.update(1)
            continue
    # Combine all users in temp directory into one dataframe
    new_users_df = read_combine_files(temp_users_dir)
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
    temp_users_dir = f"../data/temp/temp_users/"
    excluded_users = pd.read_csv('../data/metadata_files/excluded_users.csv')
    potential_new_users_df = potential_new_users_df[~potential_new_users_df.login.isin(excluded_users.login)]
    error_file_path = "../data/error_logs/potential_users_errors.csv"
    error_df = check_return_error_file(error_file_path)
    if os.path.exists(users_output_path):
        users_df = pd.read_csv(users_output_path)
        new_users_df = potential_new_users_df[~potential_new_users_df.login.isin(users_df.login)]
        if len(error_df) > 0:
            new_users_df = new_users_df[~new_users_df.login.isin(error_df.login)]
        if len(new_users_df) > 0:
            users_progress_bar = tqdm(total=len(new_users_df), desc='Users', position=1)
            expanded_new_users = get_new_users(new_users_df, temp_users_dir, users_progress_bar, error_file_path, overwrite_existing_temp_files)
        else:
            expanded_new_users = new_users_df
        users_df = pd.concat([users_df, expanded_new_users])
        users_df = users_df.drop_duplicates(subset=['login', 'id'])
    else:
        users_progress_bar = tqdm(total=len(new_users_df), desc='Users', position=1)
        users_df = get_new_users(potential_new_users_df, temp_users_dir, users_progress_bar, error_file_path, overwrite_existing_temp_files)
    
    clean_write_error_file(error_file_path, 'login')
    check_if_older_file_exists(users_output_path)
    users_df['user_query_time'] = datetime.now().strftime("%Y-%m-%d")
    users_df.to_csv(users_output_path, index=False)
    check_for_entity_in_older_queries(users_output_path, users_df)
    if return_df:
        users_df = get_user_df(users_output_path)
        return users_df

def get_user_df(output_path):
    """Function to get user dataframe
    :param output_path: path to output file
    :return: user dataframe"""
    user_df = pd.read_csv(output_path)
    return user_df

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
            if len(response_data) == 0:
                repos_progress_bar.update(1)
                continue
            response_df = pd.json_normalize(response_data)
            response_df = response_df[repo_headers.columns]
            response_df.to_csv(temp_repos_path, index=False)
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
    new_repos_df = read_combine_files(temp_repos_dir)
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
        new_repo_df = potential_new_repo_df[~potential_new_repo_df.id.isin(repo_df.id)]
        error_df = check_return_error_file(error_file_path)
        if len(error_df) > 0:
            new_repo_df = new_repo_df[~new_repo_df.full_name.isin(error_df.full_name)]
        if len(new_repo_df) > 0:
            new_repo_df = new_repo_df[repo_headers.columns]
            repo_df = pd.concat([repo_df, new_repo_df])
            repo_df = repo_df.drop_duplicates(subset=['id'])
    else:
        repo_df = potential_new_repo_df

    check_if_older_file_exists(repo_output_path)
    repo_df['repo_query_time'] = datetime.now().strftime("%Y-%m-%d")
    repo_df.to_csv(repo_output_path, index=False)
    check_for_entity_in_older_queries(repo_output_path, repo_df)
    if return_df:
        repo_df = get_repo_df(repo_output_path)
        return repo_df

def get_repo_df(output_path):
    """Function to get repo dataframe
    :param output_path: path to output file
    :return: repo dataframe"""
    repo_df = pd.read_csv(output_path, low_memory=False)
    return repo_df

"""Org Functions"""

def get_orgs(org_df, org_output_path, error_file_path, overwrite_existing_temp_files):
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
            if len(response_data) == 0:
                response_df = pd.read_csv('../data/metadata_files/org_headers.csv')
            else:
                response_df = pd.json_normalize(response_data.json())
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
        new_org_df = potential_new_org_df[~potential_new_org_df.id.isin(org_df.id)]
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

"""Join Functions
1. Check if older join entities exist and add them to our latest join"""

def check_for_joins_in_older_queries(entity_df, join_file_path, join_files_df, join_unique_field):
    """Function to check if older join entities exist and add them to our latest join
    :param entity_df: dataframe of entities
    :param join_file_path: path to join file
    :param join_files_df: dataframe of join files
    :param join_unique_field: unique field for join
    :return: None
    """
    # Needs to check if older repos exist and then find their values in older join_files_df
    join_type = join_file_path.split('/')[-1].split('_dataset')[0]

    older_join_file_path = join_file_path.replace('data/', 'data/older_files/')
    older_join_file_dir = os.path.dirname(older_join_file_path) + '/'

    older_join_df = read_combine_files(older_join_file_dir, join_type)
    
    if len(older_join_df) > 0:
        if join_unique_field in older_join_df.columns:
            older_join_df = older_join_df[older_join_df[join_unique_field].notna()]

            missing_join = older_join_df[older_join_df.id.isin(entity_df.id)] if 'search_query' in join_unique_field else older_join_df[older_join_df.repo_id.isin(entity_df.id)]

            if len(missing_join) > 0:
                time_field = 'search_query_time' if 'search_query' in join_unique_field else 'repo_query_time'
                cleaned_field = 'cleaned_search_query_time' if 'search_query' in join_unique_field else 'cleaned_repo_query_time'
                missing_join[cleaned_field] = None
                missing_join.loc[missing_join[time_field].isna(), cleaned_field] = '2022-10-10'
                missing_join[cleaned_field] = pd.to_datetime(missing_join[time_field], errors='coerce')
                missing_join = missing_join.sort_values(by=[cleaned_field]).drop_duplicates(subset=['id'], keep='first').drop(columns=[cleaned_field])

                join_files_df = pd.concat([join_files_df, missing_join])
                join_files_df = join_files_df.drop_duplicates(subset=['id',join_unique_field])
                join_files_df.to_csv(join_file_path, index=False)

            
    return join_files_df

def get_core_users_repos():
    """Function to get core users and repos
    :return: core users and repos
    """
    user_df = pd.read_csv("../data/entity_files/users_dataset.csv")
    repo_df = pd.read_csv("../data/large_files/entity_files/repos_dataset.csv", low_memory=False)
    search_queries_repo_join_df = pd.read_csv("../data/join_files/search_queries_repo_join_dataset.csv")
    search_queries_user_join_df = pd.read_csv("../data/join_files/search_queries_user_join_dataset.csv")
    contributors_df = pd.read_csv('../data/join_files/repo_contributors_join_dataset.csv')
    contributors_counts = contributors_df.groupby(['login']).size().reset_index(name='counts')
    top_contributors = contributors_counts[contributors_counts.counts > 1]
    core_repos = repo_df[repo_df["id"].isin(search_queries_repo_join_df["id"].unique())]
    core_users = user_df[(user_df.login.isin(top_contributors.login)) | (user_df.login.isin(search_queries_user_join_df.login)) | (user_df.login.isin(core_repos['owner.login']))].drop_duplicates(subset=['login'])
    return core_users, core_repos

if __name__ == '__main__':
    file_dir = '../data/join_files/'
    check_file_size_and_move(file_dir)