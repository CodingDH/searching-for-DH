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


def get_user_orgs(user_df, user_orgs_output_path, orgs_output_path, get_url_field, error_file_path, overwrite_existing_temp_files=True):
    # Create the temporary directory path to store the data
    temp_user_orgs_dir = f"../data/temp/{user_orgs_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    
    if (os.path.exists(temp_user_orgs_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_user_orgs_dir)
    
    if not os.path.exists(temp_user_orgs_dir):
        os.makedirs(temp_user_orgs_dir)

    # Create our progress bars for getting org Contributors and Users (not sure the user one works properly in Jupyter though)
    user_progress_bar = tqdm(total=len(user_df), desc="Getting User's Orgs", position=0)

    for _, row in user_df.iterrows():
        try:

            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_user_orgs_path =  F"{row.login.replace('/','')}_user_orgs_{get_url_field}.csv"

            # Check if the user_orgs_df has already been saved to the temporary directory
            if os.path.exists(temp_user_orgs_dir + temp_user_orgs_path):
                user_progress_bar.update(1)
                continue

            # Create the url to get the orgs 
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Make the first request
            response = requests.get(url, headers=auth_headers)
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next org
            if len(response_data) == 0:
                user_progress_bar.update(1)
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
                # Copy the dataframe to user_orgs_df
                user_orgs_df = data_df.copy()

                # Add metadata from the requesting user to the user_orgs_df
                user_orgs_df['user_login'] = row.login
                user_orgs_df['user_url'] = row.url
                user_orgs_df['user_html_url'] = row.html_url
                user_orgs_df['user_id'] = row.id
                user_orgs_df[f'user_{get_url_field}'] = row[get_url_field]

                # Save the user_orgs_df to the temporary directory
                user_orgs_df.to_csv(temp_user_orgs_dir + temp_user_orgs_path, index=False)

                # Get the unique orgs from the data_df
                check_add_orgs(data_df, orgs_output_path,  return_df=False)
                user_progress_bar.update(1)
        except:
            user_progress_bar.total = user_progress_bar.total - 1
            # print(f"Error on getting users for {row.login}")
            error_df = pd.DataFrame([{'login': row.login, 'error_time': time.time(), 'error_url': url}])
            
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            user_progress_bar.update(1)
            continue
    user_orgs_df = read_combine_files(temp_user_orgs_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_user_orgs_dir)
    # Close the progress bars
    user_progress_bar.close()
    return user_orgs_df

def get_user_org_activities(user_df,user_orgs_output_path, orgs_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files):
    """Function to take a list of orgsitories and get any user activities that are related to a org, save that into a join table, and also update final list of users.
    :param user_df: The dataframe of users to get the org activities for
    :param user_orgs_output_path: The path to the output file for the user_orgs_df
    :param orgs_output_path: The path to the output file for the org_df
    :param get_url_field: field in org_df that contains the url to get the actors
    :param load_existing: boolean to load existing data
    :param overwrite_existing_temp_files: boolean to overwrite existing temporary files
    returns: dataframe of org contributors and unique users"""
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        user_orgs_df = pd.read_csv(user_orgs_output_path, low_memory=False)
        orgs_df = pd.read_csv(orgs_output_path, low_memory=False)
    else:
        # Now create the path for the error logs
        error_file_path = f"../data/error_logs/{user_orgs_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
        # If we want to rerun our code, first check if the join file exists
        if os.path.exists(user_orgs_output_path):
            # If it does, load it
            user_orgs_df = pd.read_csv(user_orgs_output_path, low_memory=False)
            # Then check from our org_df which orgs are missing from the join file, using either the field we are grabing (get_url_field) or the the org id
            
            unprocessed_orgs = user_df[~user_df['login'].isin(user_orgs_df['user_login'])]

            # Check if the error log exists
            if os.path.exists(error_file_path):
                # If it does, load it and also add the orgs that were in the error log to the unprocessed orgs so that we don't keep trying to grab errored orgs
                error_df = pd.read_csv(error_file_path)
                if len(error_df) > 0:
                    unprocessed_orgs = unprocessed_orgs[~unprocessed_orgs[get_url_field].isin(error_df.error_url)]
            
            # If there are unprocessed orgs, run the get_actors code to get them or return the existing data if there are no unprocessed orgs
            if len(unprocessed_orgs) > 0:
                new_orgs_df = get_user_orgs(unprocessed_orgs, user_orgs_output_path, orgs_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
            else:
                new_orgs_df = unprocessed_orgs
            # Finally combine the existing join file with the new data and save it
            user_orgs_df = pd.concat([user_orgs_df, new_orgs_df])
            
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            user_orgs_df = get_user_orgs(user_df, user_orgs_output_path, orgs_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
        
        check_if_older_file_exists(user_orgs_output_path)
        user_orgs_df['user_query_time'] = datetime.now().strftime("%Y-%m-%d")
        user_orgs_df.to_csv(user_orgs_output_path, index=False)
        clean_write_error_file(error_file_path, 'login')
        # Finally, get the unique users which is updated in the get_actors code and return it
        join_unique_field = 'user_query'
        check_for_joins_in_older_queries(user_df, user_orgs_output_path, user_orgs_df, join_unique_field)
        orgs_df = get_org_df(orgs_output_path)
    return user_orgs_df, orgs_df
