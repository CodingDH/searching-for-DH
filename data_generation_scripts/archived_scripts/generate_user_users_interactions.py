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
import warnings
warnings.filterwarnings('ignore')
from typing import Optional, List

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def get_user_users(user_df: pd.DataFrame, user_users_output_path: str, get_url_field: str, error_file_path: str, user_cols_metadata: pd.DataFrame, filter_fields: List, overwrite_existing_temp_files: bool =True) -> pd.DataFrame:
    # Create the temporary directory path to store the data
    temp_user_users_dir = f"../data/temp/{user_users_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    
    if (os.path.exists(temp_user_users_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_user_users_dir)
    
    if not os.path.exists(temp_user_users_dir):
        os.makedirs(temp_user_users_dir)

    too_many_results = f"../data/error_logs/{user_users_output_path.split('/')[-1].split('.csv')[0]}_{get_url_field}_too_many_results.csv"

    # Create our progress bars for getting users (not sure the user one works properly in Jupyter though)
    user_progress_bar = tqdm(total=len(user_df), desc="Getting User's Interactions", position=0)

    for _, row in user_df.iterrows():
        try:
            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_user_users_path =  F"{row.login.replace('/','')}_user_users_{get_url_field}.csv"
            counts_exist = user_cols_metadata.col_name.values[0]

            if counts_exist != 'None':
                if (row[counts_exist] == 0):
                    user_progress_bar.update(1)
                    continue
                if (row[counts_exist] > 1000):
                    user_progress_bar.update(1)
                    
                    print(f"Skipping {row.login} as it has over 1000 users of {counts_exist}")
                    over_threshold_df = pd.DataFrame([row])
                    if os.path.exists(too_many_results):
                        over_threshold_df.to_csv(
                            too_many_results, mode='a', header=False, index=False)
                    else:
                        over_threshold_df.to_csv(too_many_results, index=False)
                    continue
            # Check if the user_users_df has already been saved to the temporary directory
            if os.path.exists(temp_user_users_dir + temp_user_users_path):
                existing_df = pd.read_csv(temp_user_users_dir + temp_user_users_path)
                if len(existing_df) == row[counts_exist]:
                    user_progress_bar.update(1)
                    continue
            else:
                existing_df = pd.DataFrame()

            # Create the url to get the repo actors
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Make the first request
            response = requests.get(url, headers=auth_headers)
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next repo
            if response_data is None:
                user_progress_bar.update(1)
                continue

            # Else append the response data to the list of dfs
            response_df = pd.json_normalize(response_data)
            dfs.append(response_df)
            # Check if there is a next page and if so, keep making requests until there is no next page
            while "next" in response.links.keys():
                time.sleep(10)
                query = response.links['next']['url']
                response = requests.get(query, headers=auth_headers)
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
                # Copy the dataframe to user_users_df
                user_users_df = data_df.copy()

                # Add metadata from the requesting user to the user_users_df
                user_users_df['user_login'] = row.login
                user_users_df['user_url'] = row.url
                user_users_df['user_html_url'] = row.html_url
                user_users_df['user_id'] = row.id
                user_users_df[f'user_{get_url_field}'] = row[get_url_field]

                if len(existing_df) > 0:
                    existing_df = existing_df[~existing_df.id.isin(user_users_df.id)]
                    user_users_df = pd.concat([existing_df, user_users_df])
                    user_users_df = user_users_df.drop_duplicates(subset=filter_fields)
                # Save the user_users_df to the temporary directory
                user_users_df.to_csv(temp_user_users_dir + temp_user_users_path, index=False)
                
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
    user_users_df = read_combine_files(dir_path=temp_user_users_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_user_users_dir)
    # Close the progress bars
    user_progress_bar.close()
    return user_users_df

def get_user_users_activities(user_df,user_users_output_path, users_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields, retry_errors):
    
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        user_users_df = pd.read_csv(user_users_output_path, low_memory=False)
        users_df = pd.read_csv(users_output_path, low_memory=False)
    else:
        updated_users_output_path = f"../data/temp/entity_files/{users_output_path.split('/')[-1].split('.csv')[0]}_updated.csv"
        # Now create the path for the error logs
        error_file_path = f"../data/error_logs/{user_users_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
        cols_df = pd.read_csv("../data/metadata_files/user_url_cols.csv")
        cols_metadata = cols_df[cols_df.col_url == get_url_field]
        counts_exist = cols_metadata.col_name.values[0]
        # If we want to rerun our code, first check if the join file exists
        if os.path.exists(user_users_output_path):
            # If it does, load it
            user_users_df = pd.read_csv(user_users_output_path, low_memory=False)
            # Then check from our user_df which users are missing from the join file, using either the field we are grabing (get_url_field) or the the user id
            
            if counts_exist in user_df.columns:
                subset_user_df = user_df[['login', counts_exist]]
                subset_user_users_df = user_users_df[join_unique_field].value_counts().reset_index().rename(columns={'index': 'login', join_unique_field: f'new_{counts_exist}'})
                merged_df = pd.merge(subset_user_df, subset_user_users_df, on='login', how='left')
                merged_df[f'new_{counts_exist}'] = merged_df[f'new_{counts_exist}'].fillna(0)
                missing_actors = merged_df[merged_df[counts_exist] > merged_df[f'new_{counts_exist}']]
                unprocessed_users = user_df[user_df.login.isin(missing_actors.login)]  
            else:  
                unprocessed_users = user_df[~user_df['login'].isin(user_users_df['user_login'])]
            if retry_errors == False:
                # Check if the error log exists
                if os.path.exists(error_file_path):
                    # If it does, load it and also add the users that were in the error log to the unprocessed users so that we don't keep trying to grab errored users
                    error_df = pd.read_csv(error_file_path)
                    if len(error_df) > 0:
                        unprocessed_users = unprocessed_users[~unprocessed_users[get_url_field].isin(error_df.error_url)]
            
            # If there are unprocessed users, run the get_actors code to get them or return the existing data if there are no unprocessed users
            if len(unprocessed_users) > 0:
                new_users_df = get_user_users(unprocessed_users, user_users_output_path,  get_url_field, error_file_path, cols_metadata, filter_fields, overwrite_existing_temp_files)
            else:
                new_users_df = unprocessed_users
            # Finally combine the existing join file with the new data and save it
            user_users_df = pd.concat([user_users_df, new_users_df])
            
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            user_users_df = get_user_users(user_df, user_users_output_path, get_url_field, error_file_path, cols_metadata, filter_fields, overwrite_existing_temp_files)
        
        clean_write_error_file(error_file_path, 'login')
        check_if_older_file_exists(user_users_output_path)
        user_users_df['user_query_time'] = datetime.now().strftime("%Y-%m-%d")
        user_users_df = check_for_joins_in_older_queries(user_users_output_path, user_users_df, join_unique_field, filter_fields)
        user_users_df.to_csv(user_users_output_path, index=False)
        

        # Get the unique users from the data_df
        return_df=False
        data_df = user_users_df.copy()
        original_user_df = pd.read_csv("../data/large_files/entity_files/users_dataset.csv", low_memory=False)
        data_df = data_df[(data_df.user_login.isin(user_df.login)) & (~data_df.login.isin(original_user_df.login))]
        check_add_users(data_df, updated_users_output_path, return_df, overwrite_existing_temp_files)
        # Finally, get the unique users which is updated in the get_actors code and return it
        overwrite_existing_temp_files = True
        return_df = True
        users_df = combined_updated_users(users_output_path, updated_users_output_path, overwrite_existing_temp_files, return_df)
        # users_df = get_user_df(users_output_path)
    return user_users_df, users_df


if __name__ == '__main__':
    # Get the data
    core_user_path = "../data/derived_files/firstpass_core_users.csv"
    core_users = pd.read_csv(core_user_path, low_memory=False)
    user_users_output_path = "../data/large_files/join_files/user_followers_join_dataset.csv"
    users_output_path = "../data/large_files/entity_files/users_dataset.csv"
    get_url_field = "followers_url"
    load_existing_files = False
    overwrite_existing_temp_files = False
    join_unique_field = 'user_login'
    filter_fields = ['user_login', 'login']
    retry_errors = False

    users_followers_df, user_df = get_user_users_activities(core_users,user_users_output_path, users_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields, retry_errors)

    # user_users_output_path = "../data/large_files/join_files/user_followers_join_dataset.csv"
    # users_output_path = "../data/entity_files/users_dataset.csv"
    # get_url_field = "following_url"
    # load_existing_files = False
    # overwrite_existing_temp_files = False

    # users_following_df, user_df = get_user_users_activities(core_users,user_users_output_path, users_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files)