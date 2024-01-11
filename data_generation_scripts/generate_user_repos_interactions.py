import os
import sys
import time
import shutil
import warnings

import apikey
import pandas as pd
import requests
from tqdm import tqdm
from datetime import datetime

sys.path.append("..")

from data_generation_scripts.utils import *

warnings.filterwarnings('ignore')


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}



def get_user_repos(user_df, user_repos_output_path, get_url_field, error_file_path, overwrite_existing_temp_files, threshold_row, filter_fields):
    # Create the temporary directory path to store the data
    temp_user_repos_dir = f"../data/temp/{user_repos_output_path.split('/')[-1].split('.csv')[0]}/"

    too_many_results = f"../data/error_logs/{user_repos_output_path.split('/')[-1].split('.csv')[0]}_{get_url_field}_too_many_results.csv"

    # Delete existing temporary directory and create it again
    if (os.path.exists(temp_user_repos_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_user_repos_dir)
    if not os.path.exists(temp_user_repos_dir):
        os.makedirs(temp_user_repos_dir)

    # Create our progress bars for getting Repo Contributors and Users (not sure the user one works properly in Jupyter though)
    user_progress_bar = tqdm(total=len(user_df), desc="Getting User's Repos", position=0)

    threshold_check = threshold_row['col_name'].values[0]
    threshold = 2500 if 'starred' in get_url_field else 1000
    for _, row in user_df.iterrows():
        try:
            print(f"Getting {row.login}'s repos, total {threshold_check} = {row[threshold_check]}")
            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_user_repos_path =  F"{row.login.replace('/','')}_user_repos_{get_url_field}.csv"
            
            if row[threshold_check] == 0:
                print(f"Skipping {row.login} due to {threshold_check} == 0")
                user_progress_bar.update(1)
                continue

            if row[threshold_check] > threshold:
                print(f"Skipping {row.login} due to {threshold_check} > {threshold}")
                user_progress_bar.update(1)
                over_threshold_df = pd.DataFrame([row])
                if os.path.exists(too_many_results):
                    over_threshold_df.to_csv(too_many_results, mode='a', header=False, index=False)
                else:
                    over_threshold_df.to_csv(too_many_results, index=False)
                continue

            # Check if the user_repos_df has already been saved to the temporary directory
            if os.path.exists(temp_user_repos_dir + temp_user_repos_path):
                existing_df = pd.read_csv(temp_user_repos_dir + temp_user_repos_path)
                if len(existing_df) == row[threshold_check]:
                    user_progress_bar.update(1)
                    continue
            else:
                existing_df = pd.DataFrame()

            # Create the url to get the repo actors
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Make the first request
            print(f"Making request to {url}", time.time())
            response = requests.get(url, headers=auth_headers, timeout=10)
            print(f"Request to {url} complete", time.time())
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next repo
            if response_data is None:
                user_progress_bar.update(1)
                continue

            # Else append the response data to the list of dfs
            response_df = pd.json_normalize(response_data)
            if 'message' in response_df.columns:
                print(response_df.message.values[0])
                user_progress_bar.update(1)
                continue
            dfs.append(response_df)
            # Check if there is a next page and if so, keep making requests until there is no next page
            while "next" in response.links.keys():
                time.sleep(10)
                print(f"Making request to {response.links['next']['url']}", time.time())
                query = response.links['next']['url']
                response = requests.get(query, headers=auth_headers, timeout=5)
                response_data = get_response_data(response, query)
                if response_data is None:
                    user_progress_bar.update(1)
                    continue
                response_df = pd.json_normalize(response_data)
                dfs.append(response_df)
            # Concatenate the list of dfs into a single dataframe
            data_df = pd.concat(dfs)
            # If the dataframe is empty, skip to the next user
            if len(data_df) == 0:
                user_progress_bar.update(1)
                continue
            else:
                # Copy the dataframe to user_repos_df
                user_repos_df = data_df.copy()

                # Add metadata from the requesting user to the user_repos_df
                user_repos_df['user_login'] = row.login
                user_repos_df['user_url'] = row.url
                user_repos_df['user_html_url'] = row.html_url
                user_repos_df['user_id'] = row.id
                user_repos_df[f'user_{get_url_field}'] = row[get_url_field]

                if len(existing_df) > 0:
                    existing_df = existing_df[~existing_df['id'].isin(user_repos_df['id'])]
                    user_repos_df = pd.concat([user_repos_df, existing_df])
                    user_repos_df = user_repos_df.drop_duplicates(subset=filter_fields)

                # Save the user_repos_df to the temporary directory
                user_repos_df.to_csv(temp_user_repos_dir + temp_user_repos_path, index=False)
                
                user_progress_bar.update(1)
        except requests.exceptions.RequestException as e:
            print(f"Request failed with error: {e}")
            user_progress_bar.total = user_progress_bar.total - 1
            # print(f"Error on getting users for {row.login}")
            error_df = pd.DataFrame([{'login': row.login, 'error_time': time.time(), 'error_url': row.url}])
            
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            user_progress_bar.update(1)
            continue
    user_repos_df = read_combine_files(dir_path=temp_user_repos_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_user_repos_dir)
    # Close the progress bars
    user_progress_bar.close()
    return user_repos_df

def get_user_repo_activities(user_df,user_repos_output_path, repos_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields, retry_errors):
    """Function to take a list of repositories and get any user activities that are related to a repo, save that into a join table, and also update final list of users.
    :param user_df: The dataframe of users to get the repo activities for
    :param user_repos_output_path: The path to the output file for the user_repos_df
    :param repos_output_path: The path to the output file for the repo_df
    :param get_url_field: field in repo_df that contains the url to get the actors
    :param load_existing: boolean to load existing data
    :param overwrite_existing_temp_files: boolean to overwrite existing temporary files
    returns: dataframe of repo contributors and unique users"""
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        user_repos_df = pd.read_csv(user_repos_output_path, low_memory=False)
        repos_df = pd.read_csv(repos_output_path, low_memory=False)
    else:
        updated_repos_output_path = f"../data/temp/entity_files/{repos_output_path.split('/')[-1].split('.csv')[0]}_updated.csv"
        # Now create the path for the error logs
        error_file_path = f"../data/error_logs/{user_repos_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"

        cols_df = pd.read_csv("../data/metadata_files/user_url_cols.csv")
        threshold_row = cols_df[cols_df['col_url'] == get_url_field]
        threshold_check = threshold_row['col_name'].values[0]
        # If we want to rerun our code, first check if the join file exists
        if os.path.exists(user_repos_output_path):
            # If it does, load it
            print("Loading existing user repos file", time.time())
            user_repos_df = pd.read_csv(user_repos_output_path, low_memory=False)
            # Then check from our repo_df which repos are missing from the join file, using either the field we are grabing (get_url_field) or the the repo id
            if threshold_check in user_df.columns:
                subset_users = user_df[['login', threshold_check]]
                subset_user_repos_df = user_repos_df[join_unique_field].value_counts().reset_index().rename(columns={'index': 'login', join_unique_field: f'new_{threshold_check}'})
                merged_df = pd.merge(subset_users, subset_user_repos_df, on='login', how='left')
                merged_df[f'new_{threshold_check}'] = merged_df[f'new_{threshold_check}'].fillna(0)
                missing_actors = merged_df[merged_df[threshold_check] > merged_df[f'new_{threshold_check}']]
                unprocessed_repos = user_df[user_df.login.isin(missing_actors.login)]
            else:  
                unprocessed_repos = user_df[~user_df['login'].isin(user_repos_df['user_login'])]

            if retry_errors == False:
                # Check if the error log exists
                if os.path.exists(error_file_path):
                    # If it does, load it and also add the repos that were in the error log to the unprocessed repos so that we don't keep trying to grab errored repos
                    error_df = pd.read_csv(error_file_path)
                    print("Getting unprocessed repos from error log", time.time())
                    if len(error_df) > 0:
                        unprocessed_repos = unprocessed_repos[~unprocessed_repos[get_url_field].isin(error_df.error_url)]
                        print("Getting unprocessed repos from error log", time.time())
            
            # If there are unprocessed repos, run the get_actors code to get them or return the existing data if there are no unprocessed repos
            if len(unprocessed_repos) > 0:
                print("Getting user repos", time.time())
                new_repos_df = get_user_repos(unprocessed_repos, user_repos_output_path,  get_url_field, error_file_path, overwrite_existing_temp_files, threshold_row, filter_fields)
            else:
                new_repos_df = unprocessed_repos
            # Finally combine the existing join file with the new data and save it
            user_repos_df = pd.concat([user_repos_df, new_repos_df])
            
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            user_repos_df = get_user_repos(user_df, user_repos_output_path, get_url_field, error_file_path, overwrite_existing_temp_files, threshold_row, filter_fields)
        
        clean_write_error_file(error_file_path, 'login')
        check_if_older_file_exists(user_repos_output_path)
        user_repos_df['user_query_time'] = datetime.now().strftime("%Y-%m-%d")
        is_large = True if 'star' in get_url_field else False
        check_for_joins_in_older_queries(user_repos_output_path, user_repos_df, join_unique_field, filter_fields, is_large=is_large)
        user_repos_df.to_csv(user_repos_output_path, index=False)
        
        # Finally, get the unique users which is updated in the get_actors code and return it

        data_df = user_repos_df.copy()

        check_add_repos(data_df, repos_output_path,  return_df=False)
        overwrite_existing_temp_files = True
        return_df = True
        repos_df = combined_updated_repos(repos_output_path, updated_repos_output_path, overwrite_existing_temp_files, return_df)
        

    return user_repos_df, repos_df


if __name__ == '__main__':
    # Get the data
    core_users_path = "../data/derived_files/firstpass_core_users.csv"
    core_users = pd.read_csv(core_users_path, low_memory=False)
    user_subscriptions_output_path = "../data/large_files/join_files/user_subscriptions_join_dataset.csv"
    users_output_path = "../data/large_files/entity_files/users_dataset.csv"
    get_url_field = "subscriptions_url"
    load_existing_files = False
    overwrite_existing_temp_files = False
    join_unique_field = 'user_login'
    filter_fields = ['user_login', 'full_name']
    retry_errors = False

    users_subscriptions_df, user_df = get_user_repo_activities(core_users,user_subscriptions_output_path, users_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields, retry_errors)
