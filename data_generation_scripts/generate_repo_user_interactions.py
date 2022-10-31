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

def get_actors(repo_df, repo_actors_output_path, users_output_path, get_url_field, load_existing_temp_files):
    """Function to get all contributors to a list of repositories and also update final list of users.
    :param repo_df: dataframe of repositories
    :param repo_contributors_output_path: path to repo contributors file
    :param users_output_path: path to users file
    :param get_url_field: field in repo_df to get url from
    :param load_existing_temp_files: boolean to indicate if we should load existing temp files or not
    returns: dataframe of repo contributors and unique users"""

    # Create the path for the error logs
    error_file_path = f"../data/error_logs/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"

    # Delete existing error log 
    if os.path.exists(error_file_path):
        os.remove(error_file_path)

    # Create the temporary directory path to store the data
    temp_repo_actors_dir = f"../data/temp/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    if load_existing_temp_files == False:
        if os.path.exists(temp_repo_actors_dir):
            shutil.rmtree(temp_repo_actors_dir)
            os.makedirs(temp_repo_actors_dir)
        else:
            os.makedirs(temp_repo_actors_dir)  
    
    # Also define temporary directory path for users
    temp_users_dir = f"../data/temp/temp_users/"

    # Load in the Repo URLS metadata folder that contains relevant info on how to process various fields
    urls_df = pd.read_csv("../data/metadata_files/repo_url_cols.csv")
    # Subset the urls df to only the relevant field (for example `stargazers_url` or `contributors_url`)
    repo_urls_metdata = urls_df[urls_df.url_type == get_url_field]
    # Determine what auth headers to use
    active_auth_headers = auth_headers.copy() if 'stargazers' not in get_url_field else stargazers_auth_headers.copy()

    # Create our progress bars for getting Repo Contributors and Users (not sure the user one works properly in Jupyter though)
    repo_progress_bar = tqdm(total=len(repo_df), desc="Getting Repo Actors", position=0)
    users_progress_bar = tqdm(total=0, desc="Getting Users", position=1)
    # It would be slightly faster to have this as .apply but for now leaving as a for loop to make it easier to debug
    for _, row in repo_df.iterrows():
        try:
            # Check if there is a counts value in the API and whether it is greater than 0. If 0, skip to the next repo
            counts_exist = repo_urls_metdata.count_type.values[0]
            if counts_exist != 'None':
                if (row[counts_exist] == 0):
                    repo_progress_bar.update(1)
                    continue

            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_repo_actors_path =  F"{row.full_name.replace('/','')}_repo_actors_{get_url_field}.csv" if 'full_name' in repo_df.columns else F"id_{str(row.id)}_repo_actors_{get_url_field}.csv"

            # Check if the repo_actors_df has already been saved to the temporary directory
            if os.path.exists(temp_repo_actors_dir + temp_repo_actors_path):
                repo_progress_bar.update(1)
                continue

            # Create the url to get the repo actors
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Check if we need to specify state (either open or close or all) for the URL
            url = url.replace('?', '?state=all&') if repo_urls_metdata.check_state.values[0] else url

            # Make the first request
            response = requests.get(url, headers=active_auth_headers)
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next repo
            if len(response_data) == 0:
                repo_progress_bar.update(1)
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
                    repo_progress_bar.update(1)
                    continue
                response_df = pd.json_normalize(response_data)
                dfs.append(response_df)

            # Concatenate the list of dfs into a single dataframe
            data_df = pd.concat(dfs)

            # If the dataframe is empty, skip to the next repo
            if len(data_df) == 0:
                repo_progress_bar.update(1)
                continue
            else:
                # Copy the dataframe to repo_actors_df
                repo_actors_df = data_df.copy()

                # Add metadata from the requesting repo to the repo_actors_df
                repo_actors_df['repo_id'] = row.id
                repo_actors_df['repo_url'] = row.url
                repo_actors_df['repo_html_url'] = row.html_url
                repo_actors_df['repo_full_name'] = row.full_name if 'full_name' in repo_df.columns else repo_df.repo_full_name
                repo_actors_df[get_url_field] = row[get_url_field]

                # Save the repo_actors_df to the temporary directory
                repo_actors_df.to_csv(temp_repo_actors_dir + temp_repo_actors_path, index=False)

                # If 'login' is in the repo_actors_df, then we need to get the user data for each of the users
                if 'login' in data_df.columns:
                    # Check how the API has coded user data
                    user_types = ['user.', 'owner.', 'author.']
                    for user_type in user_types:
                        if user_type in data_df.columns.tolist():
                            # Subset the columns of the data_df to be only the user relevant columns
                            subset_cols = data_df.columns
                            subset_cols = [col for col in subset_cols if col.startswith(user_type.replace('.', ''))]
                            data_df = data_df[subset_cols]
                            data_df.columns = [col.split('.')[-1] for col in data_df.columns]
                            break
                    # Get the unique users from the data_df
                    check_add_users(data_df, users_output_path, temp_users_dir, users_progress_bar, return_df=False)
                repo_progress_bar.update(1)
            
        except:
            print(f"Error on getting actors for {row.full_name}")
            error_df = pd.DataFrame([{'repo_full_name': row.full_name, 'error_time': time.time(), f'{get_url_field}': row[get_url_field]}])
            # Write errors to relevant error log
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            repo_progress_bar.update(1)
            continue

    # Finally, merge all the temporary files into a single file
    repo_actors_df = read_combine_files(temp_repo_actors_dir)
    # If the dataframe exists, save it to the output path. Otherwise create an empty one
    if len(repo_actors_df) > 0:
        repo_actors_df.to_csv(repo_actors_output_path, index=False)
    else:
        repo_actors_df = pd.DataFrame()

    # Delete the temporary directory
    shutil.rmtree(temp_repo_actors_dir)
    # Close the progress bars
    repo_progress_bar.close()
    users_progress_bar.close()
    return repo_actors_df


def get_repos_user_actors(repo_df,repo_actors_output_path, users_output_path, get_url_field, load_existing_files, load_existing_temp_files):
    """Function to take a list of repositories and get any user activities that are related to a repo, save that into a join table, and also update final list of users.
    :param repo_df: dataframe of repositories
    :param repo_actors_output_path: path to repo actors file (actors here could be subscribers, stargazers, etc...)
    :param users_output_path: path to users file
    :param get_url_field: field in repo_df that contains the url to get the actors
    :param load_existing: boolean to load existing data
    :param is_stargazers: boolean to indicate if the actors are stargazers because stargazers have a slightly different Auth Headers
    returns: dataframe of repo contributors and unique users"""
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        repo_actors_df = pd.read_csv(repo_actors_output_path, low_memory=False)
        users_df = pd.read_csv(users_output_path, low_memory=False)
    else:
        # If we want to rerun our code, first check if the join file exists
        if os.path.exists(repo_actors_output_path):
            # If it does, load it
            repo_actors_df = pd.read_csv(repo_actors_output_path, low_memory=False)
            # Then check from our repo_df which repos are missing from the join file, using either the field we are grabing (get_url_field) or the the repo id
            if get_url_field in repo_actors_df.columns:
                unprocessed_actors = repo_df[~repo_df[get_url_field].isin(repo_actors_df[get_url_field])]
            else:
                unprocessed_actors = repo_df[~repo_df.url.isin(repo_actors_df.repository_url)] 

            # Now create the path for the error logs
            error_file_path = f"../data/error_logs/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"

            # Check if the error log exists
            if os.path.exists(error_file_path):
                # If it does, load it and also add the repos that were in the error log to the unprocessed repos so that we don't keep trying to grab errored repos
                error_df = pd.read_csv(error_file_path)
                unprocessed_actors = unprocessed_actors[~unprocessed_actors[get_url_field].isin(error_df[get_url_field])]
            
            # If there are unprocessed repos, run the get_actors code to get them or return the existing data if there are no unprocessed repos
            if len(unprocessed_actors) > 0:
                new_actors_df = get_actors(unprocessed_actors, repo_actors_output_path, users_output_path, get_url_field, load_existing_temp_files)
            else:
                new_actors_df = unprocessed_actors
            # Finally combine the existing join file with the new data and save it
            repo_actors_df = pd.concat([repo_actors_df, new_actors_df])
            repo_actors_df.to_csv(repo_actors_output_path, index=False)
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            repo_actors_df = get_actors(repo_df, repo_actors_output_path, users_output_path, get_url_field, load_existing_temp_files)
    # Finally, get the unique users fil which is updated in the get_actors code and return it
    users_df = get_user_df(users_output_path)
    return repo_actors_df, users_df

if __name__ == "__main__":
    repo_df = pd.read_csv("../data/entity_files/repos_dataset.csv", low_memory=False)
    repo_actors_output_path = "../data/join_files/repo_stargazers_join_dataset.csv"
    users_output_path = "../data/entity_files/users_dataset.csv"
    rates_df = check_rate_limit()
    repo_actors_df, users_df = get_repos_user_actors(repo_df, repo_actors_output_path, users_output_path, rates_df, 'stargazers_url', load_existing=False)