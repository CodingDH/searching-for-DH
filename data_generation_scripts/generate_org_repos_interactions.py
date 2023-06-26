# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=wildcard-import
# pylint: disable=W0614
import os
import sys
import time

import apikey
import pandas as pd
import requests
from tqdm import tqdm

sys.path.append("..")
import shutil
import warnings

from data_generation_scripts.utils import *

warnings.filterwarnings('ignore')

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def get_org_repos(org_df, org_repos_output_path, get_url_field, error_file_path, org_cols_metadata, overwrite_existing_temp_files, filter_fields):
    # Create the temporary directory path to store the data
    temp_org_repos_dir = f"../data/temp/{org_repos_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    if (os.path.exists(temp_org_repos_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_org_repos_dir)
    
    if not os.path.exists(temp_org_repos_dir):
        os.makedirs(temp_org_repos_dir)

    # Create our progress bars for getting Org Repos 
    org_progress_bar = tqdm(total=len(org_df), desc="Getting Org's Repos", position=0)
    too_many_results = f"../data/error_logs/{org_repos_output_path.split('/')[-1].split('.csv')[0]}_{get_url_field}_too_many_results.csv"
    for _, row in org_df.iterrows():
        try:

            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_org_repos_path =  F"{row.login.replace('/','')}_org_repos_{get_url_field}.csv"
            counts_exist = org_cols_metadata.col_name.values[0]

            if counts_exist != 'None':
                if (row[counts_exist] == 0):
                    org_progress_bar.update(1)
                    continue
                if (row[counts_exist] > 1000):
                    org_progress_bar.update(1)
                    
                    print(f"Skipping {row.login} as it has over 1000 members of {counts_exist}")
                    over_threshold_df = pd.DataFrame([row])
                    if os.path.exists(too_many_results):
                        over_threshold_df.to_csv(
                            too_many_results, mode='a', header=False, index=False)
                    else:
                        over_threshold_df.to_csv(too_many_results, index=False)
                    continue
            # Check if the org_repos_df has already been saved to the temporary directory
            if os.path.exists(temp_org_repos_dir + temp_org_repos_path):
                existing_df = pd.read_csv(temp_org_repos_dir + temp_org_repos_path)
                if len(existing_df) == row[counts_exist]:
                    org_progress_bar.update(1)
                    continue
            else:
                existing_df = pd.DataFrame()

            # Create the url to get the repo actors
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Make the first request
            response = requests.get(url, headers=auth_headers, timeout=10)
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next org
            if response_data is None:
                org_progress_bar.update(1)
                continue

            # Else append the response data to the list of dfs
            response_df = pd.json_normalize(response_data)
            if 'message' in response_df.columns:
                print(response_df.message.values[0])
                org_progress_bar.update(1)
                continue
            dfs.append(response_df)
            # Check if there is a next page and if so, keep making requests until there is no next page
            while "next" in response.links.keys():
                time.sleep(120)
                query = response.links['next']['url']
                response = requests.get(query, headers=auth_headers, timeout=10)
                response_data = get_response_data(response, query)
                if response_data is None:
                    org_progress_bar.update(1)
                    continue
                response_df = pd.json_normalize(response_data)
                dfs.append(response_df)
            # Concatenate the list of dfs into a single dataframe
            data_df = pd.concat(dfs)

            # If the dataframe is empty, skip to the next org
            if len(data_df) == 0:
                org_progress_bar.update(1)
                continue
            else:
                # Copy the dataframe to org_repos_df
                org_repos_df = data_df.copy()

                # Add metadata from the requesting org to the org_repos_df
                org_repos_df['org_login'] = row.login
                org_repos_df['org_url'] = row.url
                org_repos_df['org_html_url'] = row.html_url
                org_repos_df['org_id'] = row.id
                org_repos_df[f'org_{get_url_field}'] = row[get_url_field]
                if len(existing_df) > 0:
                    existing_df = existing_df[~existing_df.id.isin(org_repos_df.id)]
                    org_repos_df = pd.concat([existing_df, org_repos_df])
                    org_repos_df = org_repos_df.drop_duplicates(subset=filter_fields)
                # Save the org_repos_df to the temporary directory
                org_repos_df.to_csv(temp_org_repos_dir + temp_org_repos_path, index=False)

                # Get the unique repos from the data_df
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
    org_repos_df = read_combine_files(dir_path=temp_org_repos_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_org_repos_dir)
    # Close the progress bars
    org_progress_bar.close()
    return org_repos_df

def get_org_repo_activities(org_df,org_repos_output_path, repos_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields, retry_error):
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        org_repos_df = pd.read_csv(org_repos_output_path, low_memory=False)
        repos_df = pd.read_csv(repos_output_path, low_memory=False)
    else:
        updated_repo_output_path = f"../data/temp/entity_files/{repo_output_path.split('/')[-1].split('.csv')[0]}_updated.csv"
        # Now create the path for the error logs
        error_file_path = f"../data/error_logs/{org_repos_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
        cols_df = pd.read_csv("../data/metadata_files/user_url_cols.csv")
        add_cols = pd.DataFrame({'col_name': ['members_count'], 'col_url': ['members_url']})
        cols_df = pd.concat([cols_df, add_cols])
        cols_metadata = cols_df[cols_df.col_url == get_url_field]
        counts_exist = cols_metadata.col_name.values[0]
        # If we want to rerun our code, first check if the join file exists
        if os.path.exists(org_repos_output_path):
            # If it does, load it
            org_repos_df = pd.read_csv(org_repos_output_path, low_memory=False)
            # Then check from our repo_df which repos are missing from the join file, using either the field we are grabing (get_url_field) or the the repo id
            if counts_exist in org_df.columns:
                subset_org_df = org_df[['login', counts_exist]]
                subset_org_repos_df = org_repos_df[join_unique_field].value_counts().reset_index().rename(columns={'index': 'login', join_unique_field: f'new_{counts_exist}'})
                merged_df = pd.merge(subset_org_df, subset_org_repos_df, on='login', how='left')
                merged_df[f'new_{counts_exist}'] = merged_df[f'new_{counts_exist}'].fillna(0)
                missing_actors = merged_df[merged_df[counts_exist] > merged_df[f'new_{counts_exist}']]
                unprocessed_org_repos = org_df[org_df.login.isin(missing_actors.login)]
            else:
                unprocessed_org_repos = org_df[~org_df.login.isin(org_repos_df.org_login)] 
            
            if retry_error == False:
                # Check if the error log exists
                if os.path.exists(error_file_path):
                    # If it does, load it and also add the repos that were in the error log to the unprocessed repos so that we don't keep trying to grab errored repos
                    error_df = pd.read_csv(error_file_path)
                    if len(error_df) > 0:
                        unprocessed_org_repos = unprocessed_org_repos[~unprocessed_org_repos[get_url_field].isin(error_df.error_url)]
            
            # If there are unprocessed repos, run the get_actors code to get them or return the existing data if there are no unprocessed repos
            if len(unprocessed_org_repos) > 0:
                new_repos_df = get_org_repos(unprocessed_org_repos, org_repos_output_path, get_url_field, error_file_path, cols_metadata, overwrite_existing_temp_files, filter_fields)
            else:
                new_repos_df = unprocessed_org_repos
            # Finally combine the existing join file with the new data and save it
            org_repos_df = pd.concat([org_repos_df, new_repos_df])
            
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            org_repos_df = get_org_repos(org_df, org_repos_output_path, get_url_field, error_file_path, cols_metadata,overwrite_existing_temp_files, filter_fields)
        
        clean_write_error_file(error_file_path, 'login')
        check_if_older_file_exists(org_repos_output_path)
        org_repos_df['org_query_time'] = datetime.now().strftime("%Y-%m-%d")
        org_repos_df = check_for_joins_in_older_queries(org_repos_output_path, org_repos_df, join_unique_field, filter_fields)
        org_repos_df.to_csv(org_repos_output_path, index=False)
        
        original_repo_df = pd.read_csv(repo_output_path, low_memory=False)
        data_df = org_repos_df.copy()
        data_df = data_df[(data_df.org_login.isin(org_df.login)) & (~data_df.full_name.isin(original_repo_df.full_name))]
        return_df =False
        check_add_repos(data_df, updated_repo_output_path, return_df)
        overwrite_existing_temp_files = True
        return_df = True
        repos_df = combined_updated_repos(repos_output_path, updated_repo_output_path, overwrite_existing_temp_files, return_df)

    return org_repos_df, repos_df

if __name__ == '__main__':
    # initial_core_orgs = pd.read_csv("../data/derived_files/initial_core_orgs.csv")
    # firstpass_core_orgs_path = "../data/derived_files/firstpass_core_orgs.csv"
    # firstpass_core_orgs = pd.read_csv(firstpass_core_orgs_path)
    # core_orgs = pd.concat([initial_core_orgs, firstpass_core_orgs])
    # org_repos_output_path = "../data/join_files/org_repos_join_dataset.csv"
    # repo_output_path = "../data/large_files/entity_files/repos_dataset.csv"
    # get_url_field = "repos_url"
    # load_existing_files = False
    # overwrite_existing_temp_files = False
    # join_unique_field = "org_login"
    # filter_fields = ["org_login", "full_name"]
    # retry_error = False
    # org_repos_df, user_df = get_org_repo_activities(core_orgs,org_repos_output_path, repo_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields, retry_error)
    # temp_user_file = "../data/temp/missing_repos.csv"
    # missing_repos = pd.read_csv(temp_user_file)
    # return_df =False
    # temp_repos_dir = "../data/temp/temp_repos/"
    # repos_progress_bar = tqdm(total=len(missing_repos), desc="Getting Repos")
    # error_file_path = '../data/error_logs/potential_repos_errors.csv'
    # updated_repos = get_new_repos(missing_repos, temp_repos_dir, repos_progress_bar,  error_file_path, overwrite_existing_temp_files=True)
    # overwrite_existing_temp_files = True
    # return_df = True
    # clean_write_error_file(error_file_path, 'full_name')
    # check_if_older_file_exists(temp_user_file)
    # updated_repos['repo_query_time'] = datetime.now().strftime("%Y-%m-%d")
    # updated_repos.to_csv(temp_user_file, index=False)
    # repo_df = combined_updated_repos("../data/large_files/entity_files/repos_dataset.csv", temp_user_file, overwrite_existing_temp_files, return_df)
    # updated_repo_output_path = f"../data/temp/entity_files/{repo_output_path.split('/')[-1].split('.csv')[0]}_updated.csv"

    # repo_df = pd.read_csv(updated_repo_output_path, low_memory=False)
    # repo_df = check_for_entity_in_older_queries(updated_repo_output_path, repo_df, is_large=True)
    # overwrite_existing_temp_files = True
    # return_df = True
    # repos_df = combined_updated_repos(repo_output_path, updated_repo_output_path, overwrite_existing_temp_files, return_df)
    temp_user_file = "../data/temp/missing_users.csv"
    missing_users = pd.read_csv(temp_user_file)
    missing_users["url"] = missing_users.login.apply(
        lambda x: f"https://api.github.com/users/{x}")
    
    temp_user_file_updated = "../data/temp/missing_users_updated.csv"
    
    check_add_users(missing_users, users_output_path=temp_user_file_updated, return_df=False, overwrite_existing_temp_files=False)

    user_df = combined_updated_users(user_output_path="../data/large_files/entity_files/users_dataset.csv", updated_user_output_path=temp_user_file_updated, overwrite_existing_temp_files=True, return_df=True)