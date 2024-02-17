from syslog import LOG_NEWS
import time
from urllib.parse import parse_qs
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import sys
from ast import literal_eval
sys.path.append("..")
from data_generation_scripts.general_utils import *
import shutil
import ast
from rich import print
from rich.console import Console

console = Console()


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}



def turn_names_into_list(prefix, df):
    return pd.DataFrame([{prefix: df.name.tolist()}])

def get_metadata(repo_df, existing_repo_dir, error_file_path, check_column, url_column):
    """Function to get community profiles and health percentages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param existing_repo_dir: path to directory to write existing repos
    :param error_file_path: path to file to write errors
    """
    profile_bar = tqdm(total=len(repo_df), desc="Getting Metadata")
    for _, row in repo_df.iterrows():
        existing_entities_path = f"{row.full_name.replace('/', '_').replace(' ', '_')}_coding_dh_repo.csv"
        existing_output_path = existing_repo_dir + existing_entities_path
        if os.path.exists(existing_output_path):
            existing_df = pd.read_csv(existing_output_path)
            try:
                query = row[url_column] + '/community/profile' if 'health_percentage' in check_column else row[url_column]
                response, status_code = make_request_with_rate_limiting(query, auth_headers)
                if response is not None:
                    response_df = pd.json_normalize(response)
                    if 'message' in response_df.columns:
                        console.print(response_df.message.values[0], style="bold red")
                        additional_data = {'repo_full_name': row.full_name}
                        log_error_to_file(error_file_path, additional_data, status_code, query)
                        profile_bar.update(1)
                        continue
                    if 'health_percentage' in response_df.columns:
                        response_df = response_df.rename(columns={'updated_at': 'community_profile_updated_at'})
                    elif 'languages' in url_column:
                        # prefix languages to each column
                        response_df = response_df.add_prefix('languages.')
                    else:
                        prefixes = ['tags', 'labels']
                        for prefix in prefixes:
                            if prefix in url_column:
                                response_df = turn_names_into_list(prefix, response_df)
                    # convert the 'updated_at' column to datetime
                    existing_df['coding_dh_date'] = pd.to_datetime(existing_df['coding_dh_date'])

                    # get the row with the latest date
                    latest_date = existing_df['coding_dh_date'].max()
                    latest_row = existing_df[existing_df['coding_dh_date'] == latest_date]

                    # concatenate the latest row with response_df
                    final_df = pd.concat([latest_row, response_df], axis=1)
                    
                    # drop the latest row from existing_df
                    existing_df = existing_df[existing_df['coding_dh_date'] != latest_date]

                    # concatenate existing_df and final_df
                    result_df = pd.concat([existing_df, final_df])

                    # write result_df to a CSV file
                    result_df.to_csv(existing_output_path, index=False)
                
                else:
                    additional_data = {'repo_full_name': row.full_name}
                    log_error_to_file(error_file_path, additional_data, status_code, query)
                    profile_bar.update(1)
                    continue

            except:
                profile_bar.total = profile_bar.total - 1
                additional_data = {'repo_full_name': row.full_name}
                log_error_to_file(error_file_path, additional_data, status_code, query)
                profile_bar.update(1)
                continue
        else:
            console.print(f"Repo {row.full_name} does not exist in the coding DH repo directory", style="bold red")

def get_repo_metadata(repo_df,  error_file_path, existing_repo_dir, check_column, url_column):
    """Function to get community profiles and health percentages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param error_file_path: path to file to write errors
    :param existing_repo_dir: path to directory to write existing repos
    """
    drop_fields = ["full_name", "error_url"]
    clean_write_error_file(error_file_path, drop_fields)
    if check_column in repo_df.columns:
        repos_without_metadata = repo_df[repo_df[check_column].isna()]
    else: 
        repos_without_metadata = repo_df

    if len(repos_without_metadata) > 0:
        get_metadata(repos_without_metadata, existing_repo_dir, error_file_path, check_column, url_column)

def clean_owner(row: pd.DataFrame) -> pd.DataFrame:
    """Function to clean owner column
    :param row: row from repo_df
    :return: row with cleaned owner column"""
    row['cleaned_owner'] = str(dict( ('owner.'+k, v )for k, v in row.owner.items()))
    return row

def get_repo_owners(repo_df: pd.DataFrame) -> pd.DataFrame:
    """Function to get repo owners
    :param repo_df: dataframe of repos
    :return: dataframe of repos with owners
    """
    tqdm.pandas(desc="Cleaning Repo Owners")
    repo_df.owner = repo_df.owner.apply(literal_eval)
    repo_df = repo_df.progress_apply(clean_owner, axis=1)
    repo_df = repo_df.drop(columns=['owner'])
    repo_df.cleaned_owner = repo_df.cleaned_owner.apply(literal_eval)
    repo_df = repo_df.drop('cleaned_owner', axis=1).join(pd.DataFrame(repo_df.cleaned_owner.values.tolist()))
    return repo_df


if __name__ == "__main__":
    
