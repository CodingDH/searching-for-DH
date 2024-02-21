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


def get_repo_orgs(repo_df, repo_orgs_output_path, get_url_field, error_file_path, repo_cols_metadata, overwrite_existing_temp_files, filter_fields):
    # Create the temporary directory path to store the data
    temp_repo_orgs_dir = f"../data/temp/{repo_orgs_output_path.split('/')[-1].split('.csv')[0]}/"

    # Delete existing temporary directory and create it again
    if (os.path.exists(temp_repo_orgs_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_repo_orgs_dir) 
    if not os.path.exists(temp_repo_orgs_dir):
        os.makedirs(temp_repo_orgs_dir)

    # Create our progress bars for getting org Contributors and repos (not sure the repo one works properly in Jupyter though)
    repo_progress_bar = tqdm(total=len(repo_df), desc="Getting repo's Orgs", position=0)

    too_many_results = f"../data/error_logs/{repo_orgs_output_path.split('/')[-1].split('.csv')[0]}_{get_url_field}_too_many_results.csv"

    for _, row in repo_df.iterrows():
        try:

            # Create an empty list to hold all the response data
            dfs = []

            # Create the temporary directory path to store the data
            temp_repo_orgs_path =  F"{row.full_name.replace('/','')}_repo_orgs_{get_url_field}.csv"
            counts_exist = repo_cols_metadata.count_type.values[0]

            if counts_exist != 'None':
                if (row[counts_exist] == 0):
                    repo_progress_bar.update(1)
                    continue
                if (row[counts_exist] > 1000):
                    repo_progress_bar.update(1)
                    
                    print(f"Skipping {row.full_name} as it has over 1000 repos of {counts_exist}")
                    over_threshold_df = pd.DataFrame([row])
                    if os.path.exists(too_many_results):
                        over_threshold_df.to_csv(
                            too_many_results, mode='a', header=False, index=False)
                    else:
                        over_threshold_df.to_csv(too_many_results, index=False)
                    continue
            # Check if the repo_orgs_df has already been saved to the temporary directory
            if os.path.exists(temp_repo_orgs_dir + temp_repo_orgs_path):
                existing_df = pd.read_csv(temp_repo_orgs_dir + temp_repo_orgs_path)
                if len(existing_df) == row[counts_exist]:
                    repo_progress_bar.update(1)
                    continue
            else:
                existing_df = pd.DataFrame()

            # Create the url to get the orgs 
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'

            # Make the first request
            response = requests.get(url, headers=auth_headers)
            response_data = get_response_data(response, url)

            # If the response is empty, skip to the next org
            if response_data is None:
                repo_progress_bar.update(1)
                continue

            # Else append the response data to the list of dfs
            response_df = pd.json_normalize(response_data)
            if 'message' in response_df.columns:
                print(response_df.message.values[0])
                error_df = pd.DataFrame([{'full_name': row.full_name, 'error_time': time.time(), 'error_url': row.url}])
            
                if os.path.exists(error_file_path):
                    error_df.to_csv(error_file_path, mode='a', header=False, index=False)
                else:
                    error_df.to_csv(error_file_path, index=False)
                repo_progress_bar.update(1)
                continue
            dfs.append(response_df)
            # Check if there is a next page and if so, keep making requests until there is no next page
            while "next" in response.links.keys():
                time.sleep(10)
                query = response.links['next']['url']
                response = requests.get(query, headers=auth_headers)
                response_data = get_response_data(response, query)
                if response_data is None:
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
                # Copy the dataframe to repo_orgs_df
                repo_orgs_df = data_df.copy()

                # Add metadata from the requesting repo to the repo_orgs_df
                repo_orgs_df['repo_full_name'] = row.full_name
                repo_orgs_df['repo_url'] = row.url
                repo_orgs_df['repo_html_url'] = row.html_url
                repo_orgs_df['repo_id'] = row.id
                repo_orgs_df[f'repo_{get_url_field}'] = row[get_url_field]
                if len(existing_df) > 0:
                    existing_df = existing_df[~existing_df.id.isin(repo_orgs_df.id)]
                    repo_orgs_df = pd.concat([existing_df, repo_orgs_df])
                    repo_orgs_df = repo_orgs_df.drop_duplicates(subset=filter_fields)
                # Save the repo_orgs_df to the temporary directory
                repo_orgs_df.to_csv(temp_repo_orgs_dir + temp_repo_orgs_path, index=False)
                repo_progress_bar.update(1)
        except requests.exceptions.RequestException as e:
            print(f"Request failed with error: {e}")
            repo_progress_bar.total = repo_progress_bar.total - 1
            # print(f"Error on getting repos for {row.full_name}")
            error_df = pd.DataFrame([{'full_name': row.full_name, 'error_time': time.time(), 'error_url': row.url}])
            
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            repo_progress_bar.update(1)
            continue
    repo_orgs_df = read_combine_files(dir_path=temp_repo_orgs_dir)
    if overwrite_existing_temp_files:
        # Delete the temporary directory
        shutil.rmtree(temp_repo_orgs_dir)
    # Close the progress bars
    repo_progress_bar.close()
    return repo_orgs_df

def get_repo_org_activities(repo_df,repo_orgs_output_path, orgs_output_path, get_url_field, load_existing_files, overwrite_existing_temp_files, join_unique_field, filter_fields, retry_errors):
    """Function to take a list of orgsitories and get any repo activities that are related to a org, save that into a join table, and also update final list of repos.
    :param repo_df: The dataframe of repos to get the org activities for
    :param repo_orgs_output_path: The path to the output file for the repo_orgs_df
    :param orgs_output_path: The path to the output file for the org_df
    :param get_url_field: field in org_df that contains the url to get the actors
    :param load_existing: boolean to load existing data
    :param overwrite_existing_temp_files: boolean to overwrite existing temporary files
    returns: dataframe of org contributors and unique repos"""
    # Flag to check if we want to reload existing data or rerun our code
    if load_existing_files:
        # Load relevant datasets and return them
        repo_orgs_df = pd.read_csv(repo_orgs_output_path, low_memory=False)
        orgs_df = pd.read_csv(orgs_output_path, low_memory=False)
    else:
        # Now create the path for the error logs
        error_file_path = f"../data/error_logs/{repo_orgs_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
        updated_orgs_output_path = f"../data/temp/entity_files/{orgs_output_path.split('/')[-1].split('.csv')[0]}_updated.csv"
        cols_df = pd.read_csv("../data/metadata_files/repo_url_cols.csv")
        cols_metadata = cols_df[cols_df.url_type == get_url_field]
        counts_exist = cols_metadata.count_type.values[0]
        # If we want to rerun our code, first check if the join file exists
        if os.path.exists(repo_orgs_output_path):
            # If it does, load it
            repo_orgs_df = pd.read_csv(repo_orgs_output_path, low_memory=False)
            # Then check from our org_df which orgs are missing from the join file, using either the field we are grabing (get_url_field) or the the org id
            
            if counts_exist in repo_df.columns:
                subset_repo_df = repo_df[['full_name', counts_exist]]
                subset_repo_orgs_df = repo_orgs_df[join_unique_field].value_counts().reset_index().rename(columns={'index': 'full_name', join_unique_field: f'new_{counts_exist}'})
                merged_df = pd.merge(subset_repo_df, subset_repo_orgs_df, on='full_name', how='left')
                merged_df[f'new_{counts_exist}'] = merged_df[f'new_{counts_exist}'].fillna(0)
                missing_actors = merged_df[merged_df[counts_exist] > merged_df[f'new_{counts_exist}']]
                unprocessed_orgs = repo_df[repo_df.full_name.isin(missing_actors.full_name)]
            else:
                unprocessed_orgs = repo_df[~repo_df['full_name'].isin(repo_orgs_df['repo_full_name'])]

            if retry_errors == False:
                # Check if the error log exists
                if os.path.exists(error_file_path):
                    # If it does, load it and also add the orgs that were in the error log to the unprocessed orgs so that we don't keep trying to grab errored orgs
                    error_df = pd.read_csv(error_file_path)
                    if len(error_df) > 0:
                        unprocessed_orgs = unprocessed_orgs[~unprocessed_orgs[get_url_field].isin(error_df.error_url)]
            
            # If there are unprocessed orgs, run the get_actors code to get them or return the existing data if there are no unprocessed orgs
            if len(unprocessed_orgs) > 0:
                new_orgs_df = get_repo_orgs(unprocessed_orgs, repo_orgs_output_path, get_url_field, error_file_path, cols_metadata, overwrite_existing_temp_files, filter_fields)
            else:
                new_orgs_df = unprocessed_orgs
            # Finally combine the existing join file with the new data and save it
            repo_orgs_df = pd.concat([repo_orgs_df, new_orgs_df])
            
        else:
            # If the join file doesn't exist, run the get_actors code to get them
            repo_orgs_df = get_repo_orgs(repo_df, repo_orgs_output_path, get_url_field, error_file_path, cols_metadata,  overwrite_existing_temp_files, filter_fields)
        
        clean_write_error_file(error_file_path, 'full_name')
        check_if_older_file_exists(repo_orgs_output_path)
        repo_orgs_df['repo_query_time'] = datetime.now().strftime("%Y-%m-%d")
        repo_orgs_df =  check_for_joins_in_older_queries(repo_orgs_output_path, repo_orgs_df, join_unique_field, filter_fields)
        repo_orgs_df.to_csv(repo_orgs_output_path, index=False)
        
        # orgs_df = get_org_df(orgs_output_path)

        return_df = False
        # Get the unique orgs from the data_df
        data_df = repo_orgs_df.copy()
        check_add_orgs(data_df, orgs_output_path,  return_df, overwrite_existing_temp_files)

        overwrite_existing_temp_files = True
        return_df = True
        orgs_df = combined_updated_orgs(orgs_output_path, updated_orgs_output_path, overwrite_existing_temp_files, return_df)
        
    return repo_orgs_df, orgs_df

if __name__ == '__main__':
    # Get the data
    orgs_output_path = "../data/entity_files/orgs_dataset.csv"
    org_df = pd.read_csv(orgs_output_path)
    missing_user_orgs = pd.read_csv("../data/temp/missing_user_orgs.csv")
    missing_repo_orgs = pd.read_csv("../data/temp/missing_repo_orgs.csv")
    missing_orgs = pd.concat([missing_user_orgs, missing_repo_orgs])
    missing_orgs['url'] = missing_orgs.login.apply(lambda x: "https://api.github.com/orgs/" + x)

    overwrite_existing_temp_files = False
    return_df = False
    error_file_path = "../data/error_logs/potential_orgs_errors.csv"

    updated_orgs_output_path = '../data/temp/missing_orgs_dataset.csv'
    
    cleaned_orgs = get_orgs(missing_orgs, updated_orgs_output_path, error_file_path, overwrite_existing_temp_files)
    # cleaned_orgs['org_query_time'] = datetime.now().strftime("%Y-%m-%d")
    # org_df.to_csv(orgs_output_path, index=False)
    print(cleaned_orgs.columns)
    check_for_entity_in_older_queries(orgs_output_path, org_df)
    overwrite_existing_temp_files = True
    return_df = True
    # orgs_df = combined_updated_orgs(orgs_output_path, updated_orgs_output_path, overwrite_existing_temp_files, return_df)
        

