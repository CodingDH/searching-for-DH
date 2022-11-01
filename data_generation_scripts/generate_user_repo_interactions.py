# - get starred
# - get user repos

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
import ast


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}
stargazers_auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request', 'Accept': 'application/vnd.github.v3.star+json'}

def get_user_repos(user_df, user_repos_output_path, repos_output_path, get_url_field, load_existing_temp_files):
    # Create the path for the error logs
    error_file_path = f"../data/error_logs/{user_repos_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"

    # Delete existing error log 
    if load_existing_temp_files == False:
        if os.path.exists(error_file_path):
            os.remove(error_file_path)

    # Create the temporary directory path to store the data
    temp_user_repos_dir = f"../data/temp/{user_repos_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    if load_existing_temp_files == False:
        if os.path.exists(temp_user_repos_dir):
            shutil.rmtree(temp_user_repos_dir)
            os.makedirs(temp_user_repos_dir)
        else:
            os.makedirs(temp_user_repos_dir)

    # Also define temporary directory path for repos
    temp_repos_dir = f"../data/temp/temp_repos/"

    # Load in the Repo URLS metadata folder that contains relevant info on how to process various fields
    urls_df = pd.read_csv("../data/metadata_files/user_url_cols.csv")
    # Subset the urls df to only the relevant field (for example `stargazers_url` or `repos_url`)
    user_urls_metdata = urls_df[urls_df.url_type == get_url_field]

    # Create our progress bars for getting Repo Contributors and Users (not sure the user one works properly in Jupyter though)
    user_progress_bar = tqdm(total=len(user_df), desc="Getting User's Repos", position=0)
    repo_progress_bar = tqdm(total=0, desc="Getting Repos", position=1)

    for _, row in user_df.iterrows():
        try:

            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_user_repos_path =  F"{row.login.replace('/','')}_user_repos_{get_url_field}.csv"

            # Check if the user_repos_df has already been saved to the temporary directory
            if os.path.exists(temp_user_repos_dir + temp_user_repos_path):
                user_progress_bar.update(1)
                continue

            # Create the url to get the repo actors
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Make the first request
            response = requests.get(url, headers=active_auth_headers)
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next repo
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
                # Copy the dataframe to user_repos_df
                user_repos_df = data_df.copy()

            # Add metadata from the requesting user to the user_repos_df
            user_repos_df['user_login'] = row.login
            user_repos_df['user_url'] = row.url
            user_repos_df['user_html_url'] = row.html_url
            user_repos_df['user_id'] = row.id
            user_repos_df[get_url_field] = row[get_url_field]

            # Save the user_repos_df to the temporary directory
            user_repos_df.to_csv(temp_user_repos_dir + temp_user_repos_path, index=False)