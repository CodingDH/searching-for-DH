import time
from urllib.parse import parse_qs
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import sys
sys.path.append("..")
from data_generation_scripts.utils import *
import shutil


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def get_members(org_df, org_members_output_path, users_output_path, get_url_field, error_file_path, overwrite_existing_temp_files):
    # Create the temporary directory path to store the data
    temp_org_members_dir = f"../data/temp/{org_members_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    if (os.path.exists(temp_org_members_dir)) and (overwrite_existing_temp_files):
        shutil.rmtree(temp_org_members_dir)
        
    if not os.path.exists(temp_org_members_dir):
        os.makedirs(temp_org_members_dir)

    # Create our progress bars for getting Org Members
    org_progress_bar = tqdm(total=len(org_df), desc="Getting Org Members", position=0)
    # It would be slightly faster to have this as .apply but for now leaving as a for loop to make it easier to debug
    for _, row in org_df.iterrows():
        try:
            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_org_members_path =  F"{row.login.replace('/','')}_org_members_{get_url_field}.csv" 

            # Check if the org_members_df has already been saved to the temporary directory
            if os.path.exists(temp_org_members_dir + temp_org_members_path):
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
                # Copy the dataframe to org_members_df
                org_members_df = data_df.copy()

                # Add metadata from the requesting repo to the org_members_df
                org_members_df['org_id'] = row.id
                org_members_df['org_url'] = row.url
                org_members_df['org_html_url'] = row.html_url
                org_members_df['org_full_name'] = row.name
                org_members_df[get_url_field] = row[get_url_field]

                # Save the org_members_df to the temporary directory
                org_members_df.to_csv(temp_org_members_dir + temp_org_members_path, index=False)

                
                return_df=False
                check_add_users(data_df, users_output_path, return_df, overwrite_existing_temp_files)
                org_progress_bar.update(1)
        except:
            # print(f"Error on getting actors for {row.full_name}")
            org_progress_bar.total = org_progress_bar.total - 1
            error_df = pd.DataFrame([{'org_name': row.name, 'error_time': time.time(), 'error_url': url}])
            # Write errors to relevant error log
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            # org_progress_bar.update(1)
            continue
    # Finally, merge all the temporary files into a single file
    org_members_df = read_combine_files(temp_org_members_dir)

    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_org_members_dir)
    # Close the progress bars
    org_progress_bar.close()
    return org_members_df

def get_org_users_activities(org_df, org_members_output_path, users_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files):
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        org_members_df = pd.read_csv(org_members_output_path, low_memory=False)
        users_df = pd.read_csv(users_output_path, low_memory=False)
    else:
        # If we want to rerun our code, first check if the join file exists

        # Create the path for the error logs
        error_file_path = f"../data/error_logs/{org_members_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
        if os.path.exists(org_members_output_path):
            # If it does, load it
            org_members_df = pd.read_csv(org_members_output_path, low_memory=False)
            # Then check from our org_df which orgs are missing from the join file, using either the field we are grabing (get_url_field) or the the org id
            if get_url_field in org_members_df.columns:
                unprocessed_org_members = org_df[~org_df[get_url_field].isin(org_members_df[get_url_field])]
            else:
                unprocessed_org_members = org_df[~org_df.url.isin(org_members_df.org_url)] 

            # Now create the path for the error logs
            error_file_path = f"../data/error_logs/{org_members_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"

            # Check if the error log exists
            if os.path.exists(error_file_path):
                # If it does, load it and also add the orgs that were in the error log to the unprocessed orgs so that we don't keep trying to grab errored orgs
                error_df = pd.read_csv(error_file_path)
                if len(error_df) > 0:
                    unprocessed_org_members = unprocessed_org_members[~unprocessed_org_members[get_url_field].isin(error_df[get_url_field])]
            
            # If there are unprocessed orgs, run the get_members code to get them or return the existing data if there are no unprocessed repos
            if len(unprocessed_org_members) > 0:
                new_members_df = get_members(unprocessed_org_members, org_members_output_path, users_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
            else:
                new_members_df = unprocessed_org_members
            # Finally combine the existing join file with the new data and save it
            org_members_df = pd.concat([org_members_df, new_members_df])
            
        else:
            # If the join file doesn't exist, run the get_members code to get them
            org_members_df = get_members(org_df, org_members_output_path, users_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
        
        check_if_older_file_exists(org_members_output_path)
        org_members_df['org_query_time'] = datetime.now().strftime("%Y-%m-%d")
        org_members_df.to_csv(org_members_output_path, index=False)
        # Finally, get the unique users which is updated in the get_members code and return it
        clean_write_error_file(error_file_path, 'org_name')
        join_unique_field = 'org_query'
        check_for_joins_in_older_queries(org_df, org_members_output_path, org_members_df, join_unique_field)
        users_df = get_user_df(users_output_path)
    return org_members_df, users_df
