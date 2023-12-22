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
import sys
sys.path.append("..")
from data_generation_scripts.general_utils import  read_csv_file, check_total_pages, check_total_results, check_rate_limit, make_request_with_rate_limiting, read_combine_files, check_return_error_file, clean_write_error_file, check_if_older_file_exists, check_file_created

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}


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
        users_df = users_df.drop_duplicates(subset=['login'])
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
            if 'message' in response_df.columns:
                print(response_df.message.values[0])
                repos_progress_bar.update(1)
                continue
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
        new_repo_df = potential_new_repo_df[~potential_new_repo_df.full_name.isin(repo_df.full_name)]
        error_df = check_return_error_file(error_file_path)
        if len(error_df) > 0:
            new_repo_df = new_repo_df[~new_repo_df.full_name.isin(error_df.full_name)]
        print(f"Number of new repos: {len(new_repo_df)}", time.time())
        if len(new_repo_df) > 0:
            new_repo_df = new_repo_df[repo_headers.columns]
            repo_df = pd.concat([repo_df, new_repo_df])
            repo_df = repo_df.drop_duplicates(subset=['full_name'])
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
    user_cols = ['bio', 'followers_url', 'following_url', 'gists_url', 'gravatar_id', 'hireable', 'organizations_url','received_events_url', 'site_admin', 'starred_url','subscriptions_url','user_query_time', 'login',]
    for _, row in org_df.iterrows():
        try:
            # Create the temporary directory path to store the data
            temp_org_path =  F"{row.login.replace('/','')}_org.csv"

            # Check if the org_df has already been saved to the temporary directory
            if os.path.exists(temp_org_dir + temp_org_path):
                org_progress_bar.update(1)
                continue
            user_url = row.url if '/users/' in row.url else row.url.replace('/orgs/', '/users/')
            print(user_url)
            user_response = requests.get(user_url, headers=auth_headers)
            user_response_data = get_response_data(user_response, user_url)
            if user_response_data is None:
                user_response_df = pd.DataFrame(columns=user_cols, data=None, index=None)
            else:
                user_response_df = pd.json_normalize(user_response_data)
            user_response_df = user_response_df[user_cols]
            # Create the url to get the org
            url = row.url.replace('/users/', '/orgs/') if '/users/' in row.url else row.url
            print(url)
            # Make the first request
            response = requests.get(url, headers=auth_headers)
            response_data = get_response_data(response, url)
            print(response_data)
            if response_data is None:
                response_df = pd.read_csv('../data/metadata_files/org_headers.csv')
            else:
                response_df = pd.json_normalize(response_data)
            print(response_df.columns.tolist())
            response_df = response_df[org_headers.columns]
            common_columns = list(set(response_df.columns.tolist()).intersection(set(user_response_df.columns.tolist())))
            final_response_df = pd.merge(response_df, user_response_df, on=common_columns, how='left')
            print(final_response_df.columns.tolist())
            final_response_df.to_csv(temp_org_dir + temp_org_path, index=False)
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

def get_core_users_repos(combine_files=True):
    """Function to get core users and repos
    :return: core users and repos
    """
    initial_core_users = pd.read_csv("../data/derived_files/initial_core_users.csv")
    initial_core_users['origin'] = 'initial_core'
    initial_core_repos = pd.read_csv("../data/derived_files/initial_core_repos.csv")
    initial_core_repos['origin'] = 'initial_core'
    initial_core_orgs = pd.read_csv("../data/derived_files/initial_core_orgs.csv")
    initial_core_orgs['origin'] = 'initial_core'

    firstpass_core_users = pd.read_csv("../data/derived_files/firstpass_core_users.csv")
    firstpass_core_users['origin'] = 'firstpass_core'
    firstpass_core_repos = pd.read_csv("../data/derived_files/firstpass_core_repos.csv")
    firstpass_core_repos['origin'] = 'firstpass_core'
    firstpass_core_orgs = pd.read_csv("../data/derived_files/firstpass_core_orgs.csv")
    firstpass_core_orgs['origin'] = 'firstpass_core'

    finalpass_core_users = pd.read_csv("../data/derived_files/finalpass_core_users.csv")
    finalpass_core_users['origin'] = 'finalpass_core'
    finalpass_core_repos = pd.read_csv("../data/large_files/derived_files/finalpass_core_repos.csv", low_memory=False, on_bad_lines='skip')
    finalpass_core_repos['origin'] = 'finalpass_core'
    finalpass_core_orgs = pd.read_csv("../data/derived_files/finalpass_core_orgs.csv")
    finalpass_core_orgs['origin'] = 'finalpass_core'

    if combine_files:
        core_users = pd.concat([initial_core_users, firstpass_core_users, finalpass_core_users])
        core_repos = pd.concat([initial_core_repos, firstpass_core_repos, finalpass_core_repos])
        core_orgs = pd.concat([initial_core_orgs, firstpass_core_orgs, finalpass_core_orgs])
        return core_users, core_repos, core_orgs
    else:
        return initial_core_users, initial_core_repos, initial_core_orgs, firstpass_core_users, firstpass_core_repos, firstpass_core_orgs, finalpass_core_users, finalpass_core_repos, finalpass_core_orgs


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