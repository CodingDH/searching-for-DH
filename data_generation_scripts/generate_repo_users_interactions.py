import time
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
stargazers_auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request', 'Accept': 'application/vnd.github.v3.star+json'}

def get_additional_commit_data(response_df):
    """Function to get additional commit data from the commit url
    :param response_df: dataframe of commits
    returns: dataframe of commits with additional data"""
    dfs = []
    for _, row in response_df.iterrows():
        commit_response = requests.get(row.url, headers=auth_headers)
        commit_response_data = get_response_data(commit_response, row.url)
        response_commit_df = pd.json_normalize(commit_response_data)
        row['files'] = response_commit_df['files'].values[0]
        row['stats.total'] = response_commit_df['stats.total'].values[0]
        row['stats.additions'] = response_commit_df['stats.additions'].values[0]
        row['stats.deletions'] = response_commit_df['stats.deletions'].values[0]
        updated_df = pd.DataFrame([row.to_dict()])
        dfs.append(updated_df)
    response_df = pd.concat(dfs)
    return response_df

def get_actors(repo_df, repo_actors_output_path, users_output_path, get_url_field, error_file_path, overwrite_existing_temp_files):
    """Function to get all contributors to a list of repositories and also update final list of users.
    :param repo_df: dataframe of repositories
    :param repo_contributors_output_path: path to repo contributors file
    :param users_output_path: path to users file
    :param get_url_field: field in repo_df to get url from
    :param error_file_path: path to error file
    :param overwrite_existing_temp_files: boolean to indicate if we should overwrite existing temp files or not
    returns: dataframe of repo contributors and unique users"""

    # Create the temporary directory path to store the data
    temp_repo_actors_dir = f"../data/temp/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}/"
    print(temp_repo_actors_dir)

    too_many_results = f"../data/error_logs/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}_{get_url_field}_too_many_results.csv"

    # Delete existing temporary directory and create it again
    if (os.path.exists(temp_repo_actors_dir)) and (overwrite_existing_temp_files):
        shutil.rmtree(temp_repo_actors_dir)
        
    if not os.path.exists(temp_repo_actors_dir):
        os.makedirs(temp_repo_actors_dir)  

    # Load in the Repo URLS metadata folder that contains relevant info on how to process various fields
    urls_df = pd.read_csv("../data/metadata_files/repo_url_cols.csv")
    # Subset the urls df to only the relevant field (for example `stargazers_url` or `contributors_url`)
    repo_urls_metdata = urls_df[urls_df.url_type == get_url_field]
    # Determine what auth headers to use
    active_auth_headers = auth_headers.copy() if 'stargazers' not in get_url_field else stargazers_auth_headers.copy()

    # Create our progress bars for getting Repo Contributors and Users (not sure the user one works properly in Jupyter though)
    repo_progress_bar = tqdm(total=len(repo_df), desc="Getting Repo Actors", position=0)
    # It would be slightly faster to have this as .apply but for now leaving as a for loop to make it easier to debug
    for _, row in repo_df.iterrows():
        try:
            # Check if there is a counts value in the API and whether it is greater than 0. If 0, skip to the next repo
            repo_name = row.repo_full_name if 'repo_full_name' in repo_df.columns else row.full_name
            counts_exist = repo_urls_metdata.count_type.values[0]
            if counts_exist != 'None':
                if (row[counts_exist] == 0):
                    repo_progress_bar.update(1)
                    continue
                if (row[counts_exist] > 1000):
                    repo_progress_bar.update(1)
                    
                    print(f"Skipping {repo_name} as it has over 1000 users of {counts_exist}")
                    over_threshold_df = pd.DataFrame([row])
                    if os.path.exists(too_many_results):
                        over_threshold_df.to_csv(
                            too_many_results, mode='a', header=False, index=False)
                    else:
                        over_threshold_df.to_csv(too_many_results, index=False)
                    continue

            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_repo_actors_path =  F"{row.full_name.replace('/','')}_repo_actors_{get_url_field}.csv" if 'repo_full_name' not in repo_df.columns else F"id_{str(row.id)}_repo_actors_{get_url_field}.csv"

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
            if get_url_field == 'commits_url':
                response_df = get_additional_commit_data(response_df)
            dfs.append(response_df)
            # Check if there is a next page and if so, keep making requests until there is no next page
            while "next" in response.links.keys():
                time.sleep(120)
                query = response.links['next']['url']
                response = requests.get(query, headers=active_auth_headers)
                response_data = get_response_data(response, query)
                if len(response_data) == 0:
                    repo_progress_bar.update(1)
                    continue
                response_df = pd.json_normalize(response_data)
                if get_url_field == 'commits_url':
                    response_df = get_additional_commit_data(response_df)
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
                if 'repo_full_name' in repo_df.columns:
                    repo_actors_df['repo_full_name'] = row.repo_full_name  
                else: 
                    repo_actors_df['repo_full_name'] = row.full_name
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
                    return_df=False
                    check_add_users(data_df, users_output_path, return_df, overwrite_existing_temp_files)
                repo_progress_bar.update(1)
        except:
            # print(f"Error on getting actors for {row.full_name}")
            repo_progress_bar.total = repo_progress_bar.total - 1
            repo_name = 'repo_full_name' if 'repo_full_name' in repo_df.columns else 'full_name'
            repo_field = 'id' if repo_name == 'repo_full_name' else 'full_name'
            error_df = pd.DataFrame([{'repo_full_name': row[repo_field], 'error_time': time.time(), 'error_url': row[repo_name]}])
            # Write errors to relevant error log
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            # repo_progress_bar.update(1)
            continue

    # Finally, merge all the temporary files into a single file
    repo_actors_df = read_combine_files(dir_path=temp_repo_actors_dir)

    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_repo_actors_dir)
    # Close the progress bars
    repo_progress_bar.close()
    return repo_actors_df


def get_repos_user_actors(repo_df,repo_actors_output_path, users_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields):
    """Function to take a list of repositories and get any user activities that are related to a repo, save that into a join table, and also update final list of users.
    :param repo_df: dataframe of repositories
    :param repo_actors_output_path: path to repo actors file (actors here could be subscribers, stargazers, etc...)
    :param users_output_path: path to users file
    :param get_url_field: field in repo_df that contains the url to get the actors
    :param load_existing: boolean to load existing data
    :param overwrite_existing_temp_files: boolean to overwrite existing temporary files
    returns: dataframe of repo contributors and unique users"""
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        repo_actors_df = pd.read_csv(repo_actors_output_path, low_memory=False)
        users_df = pd.read_csv(users_output_path, low_memory=False)
    else:
        # If we want to rerun our code, first check if the join file exists

        # Create the path for the error logs
        error_file_path = f"../data/error_logs/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
        if os.path.exists(repo_actors_output_path):
            # If it does, load it
            repo_actors_df = pd.read_csv(repo_actors_output_path, low_memory=False)
            # Then check from our repo_df which repos are missing from the join file, using either the field we are grabing (get_url_field) or the the repo id
            if get_url_field in repo_actors_df.columns:
                unprocessed_actors = repo_df[~repo_df[get_url_field].isin(repo_actors_df[get_url_field])]
            else:
                unprocessed_actors = repo_df[~repo_df.url.isin(repo_actors_df.repo_url)] 

            # Now create the path for the error logs
            error_file_path = f"../data/error_logs/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"

            # Check if the error log exists
            if os.path.exists(error_file_path):
                # If it does, load it and also add the repos that were in the error log to the unprocessed repos so that we don't keep trying to grab errored repos
                error_df = pd.read_csv(error_file_path)
                if len(error_df) > 0:
                    unprocessed_actors = unprocessed_actors[~unprocessed_actors[get_url_field].isin(error_df[get_url_field])]
            
            # If there are unprocessed repos, run the get_actors code to get them or return the existing data if there are no unprocessed repos
            if len(unprocessed_actors) > 0:
                new_actors_df = get_actors(unprocessed_actors, repo_actors_output_path, users_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
            else:
                new_actors_df = unprocessed_actors
            # Finally combine the existing join file with the new data and save it
            repo_actors_df = pd.concat([repo_actors_df, new_actors_df])
            
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            repo_actors_df = get_actors(repo_df, repo_actors_output_path, users_output_path, get_url_field, error_file_path, overwrite_existing_temp_files)
        
        check_if_older_file_exists(repo_actors_output_path)
        repo_actors_df['repo_query_time'] = datetime.now().strftime("%Y-%m-%d")
        repo_actors_df.to_csv(repo_actors_output_path, index=False)
        # Finally, get the unique users which is updated in the get_actors code and return it
        clean_write_error_file(error_file_path, 'repo_full_name')
        check_for_joins_in_older_queries(repo_actors_output_path, repo_actors_df, join_unique_field, filter_fields)
        users_df = get_user_df(users_output_path)
    return repo_actors_df, users_df

if __name__ == "__main__":
    # Load the repo dataframe
    core_repos = pd.read_csv("../data/derived_files/initial_core_repos.csv", low_memory=False)
    get_url_field = 'pulls_url'
    load_existing_files = False
    overwrite_existing_temp_files = False
    filter_fields = ['id', 'repo_full_name', 'user.login', 'head.user.login']
    join_unique_field = 'repo_full_name'
    pulls_df, users_df = get_repos_user_actors(core_repos, '../data/large_files/join_files/repo_pulls_join_dataset.csv', '../data/entity_files/users_dataset.csv', get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields)
    pulls_errors_df = check_return_error_file('../data/error_logs/repo_pulls_join_dataset_errors.csv')

    get_url_field = 'review_comments_url'
    load_existing_files = False
    overwrite_existing_temp_files = False
    filter_fields = ['repo_full_name', 'user.login', 'url']
    join_unique_field = 'repo_full_name'
    pulls_comments_df, users_df = get_repos_user_actors(pulls_df, '../data/large_files/join_files/pulls_comments_join_dataset.csv', '../data/entity_files/users_dataset.csv', get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields)
    pulls_comments_errors_df = check_return_error_file('../data/error_logs/pulls_comments_join_dataset_errors.csv')
            