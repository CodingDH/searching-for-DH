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

def get_org_repos(org_df, org_repos_output_path, repos_output_path, get_url_field, error_file_path, overwrite_existing_temp_files):
    # Create the temporary directory path to store the data
    temp_org_repos_dir = f"../data/temp/{org_repos_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    
    if (os.path.exists(temp_org_repos_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_org_repos_dir)
    
    if not os.path.exists(temp_org_repos_dir):
        os.makedirs(temp_org_repos_dir)

    # Create our progress bars for getting Org Repos 
    org_progress_bar = tqdm(total=len(org_df), desc="Getting Org's Repos", position=0)

    for _, row in org_df.iterrows():
        try:

            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_org_repos_path =  F"{row.login.replace('/','')}_org_repos_{get_url_field}.csv"

            # Check if the org_repos_df has already been saved to the temporary directory
            if os.path.exists(temp_org_repos_dir + temp_org_repos_path):
                org_progress_bar.update(1)
                continue

            # Create the url to get the repo actors
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Make the first request
            response = requests.get(url, headers=auth_headers)
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next org
            if len(response_data) == 0:
                org_progress_bar.update(1)
                continue

            # Else append the response data to the list of dfs
            response_df = pd.json_normalize(response_data)
            dfs.append(response_df)
            # Check if there is a next page and if so, keep making requests until there is no next page
            while "next" in response.links.keys():
                time.sleep(120)
                query = response.links['next']['url']
                response = requests.get(query, headers=auth_headers)
                response_data = get_response_data(response, query)
                if len(response_data) == 0:
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

                # Save the org_repos_df to the temporary directory
                org_repos_df.to_csv(temp_org_repos_dir + temp_org_repos_path, index=False)

                # Get the unique repos from the data_df
                check_add_repos(data_df, repos_output_path,  return_df=False)
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
    org_repos_df = read_combine_files(temp_org_repos_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_org_repos_dir)
    # Close the progress bars
    org_progress_bar.close()
    return org_repos_df

def get_org_repo_activities(org_df,org_repos_output_path, repos_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files):
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        org_repos_df = pd.read_csv(org_repos_output_path, low_memory=False)
        repos_df = pd.read_csv(repos_output_path, low_memory=False)
    else:
        # Now create the path for the error logs
        error_file_path = f"../data/error_logs/{org_repos_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
        # If we want to rerun our code, first check if the join file exists
        if os.path.exists(org_repos_output_path):
            # If it does, load it
            org_repos_df = pd.read_csv(org_repos_output_path, low_memory=False)
            # Then check from our repo_df which repos are missing from the join file, using either the field we are grabing (get_url_field) or the the repo id
            
            unprocessed_org_repos = org_df[~org_df['login'].isin(org_repos_df['org_login'])]

            # Check if the error log exists
            if os.path.exists(error_file_path):
                # If it does, load it and also add the repos that were in the error log to the unprocessed repos so that we don't keep trying to grab errored repos
                error_df = pd.read_csv(error_file_path)
                if len(error_df) > 0:
                    unprocessed_org_repos = unprocessed_org_repos[~unprocessed_org_repos[get_url_field].isin(error_df.error_url)]
            
            # If there are unprocessed repos, run the get_actors code to get them or return the existing data if there are no unprocessed repos
            if len(unprocessed_org_repos) > 0:
                new_repos_df = get_org_repos(unprocessed_org_repos, org_repos_output_path, repos_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
            else:
                new_repos_df = unprocessed_org_repos
            # Finally combine the existing join file with the new data and save it
            org_repos_df = pd.concat([org_repos_df, new_repos_df])
            
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            org_repos_df = get_org_repos(org_df, org_repos_output_path, repos_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
        
        check_if_older_file_exists(org_repos_output_path)
        org_repos_df['org_query_time'] = datetime.now().strftime("%Y-%m-%d")
        org_repos_df.to_csv(org_repos_output_path, index=False)
        clean_write_error_file(error_file_path, 'login')
        join_unique_field = 'org_query'
        check_for_joins_in_older_queries(org_df, org_repos_output_path, org_repos_df, join_unique_field)
        repos_df = get_repo_df(repos_output_path)
    return org_repos_df, repos_df
