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

def get_languages(row):
    """Function to get languages for a repo
    :param row: row from repo_df
    :return: dictionary of languages with number of bytes"""
    temp_repo_dir = "../data/temp/repo_languages/"
    temp_name = row.full_name.replace('/', '_') + '_language.json'
    temp_path = temp_repo_dir + temp_name
    if os.path.exists(temp_path):
        with open(temp_path, 'r') as f:
            response_data = ast.literal_eval(f.read())
    else:
        try:
            response = requests.get(row.languages_url, headers=auth_headers)
            response_data = get_response_data(response, row.languages_url)
            
        except:
            print("Error getting languages for repo: " + row.full_name)
            response_data = None
        if response_data is not None:
            os.makedirs(temp_repo_dir, exist_ok=True)
            with open(temp_path, 'w') as f:
                f.write(str(response_data))
    return response_data

def get_repo_languages(repo_df, output_path):
    """Function to get languages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with languages"""
    print(len(repo_df))
    if 'languages' in repo_df.columns:
        repos_without_languages = repo_df[repo_df.languages.isna()]
        repos_with_languages = repo_df[repo_df.languages.notna()]
    else: 
        repos_without_languages = repo_df
        repos_with_languages = pd.DataFrame()

    if len(repos_without_languages) > 0:
        tqdm.pandas(desc="Getting Languages")
        repos_without_languages['languages'] = repos_without_languages.progress_apply(get_languages, axis=1)
        repo_df = pd.concat([repos_with_languages, repos_without_languages])
        print(len(repo_df))
        repo_df = repo_df.drop_duplicates(subset=['full_name'])
        print(len(repo_df))
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_labels(row):
    """Function to get labels for a repo
    :param row: row from repo_df
    :return: list of labels
    Could save this output in separate file since labels also returns url, color, description, and whether the label is default or not"""
    response = requests.get(row.labels_url.split('{')[0], headers=auth_headers)
    response_data = get_response_data(response, row.labels_url.split('{')[0])
    if response_data is not None:
        labels_df = pd.DataFrame(response_data)
        labels = labels_df['name'].tolist()
    else:
        labels = []
    return labels

def get_repo_labels(repo_df, output_path):
    """Function to get labels for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with labels"""
    if 'labels' in repo_df.columns:
        repos_without_labels = repo_df[repo_df.labels.isna()]
        repos_with_labels = repo_df[repo_df.labels.notna()]
    else: 
        repos_without_labels = repo_df
        repos_with_labels = pd.DataFrame()
    
    if len(repos_without_labels) > 0:
        tqdm.pandas(desc="Getting Labels")
        repos_without_labels['labels'] = repos_without_labels.progress_apply(get_labels, axis=1)
        repo_df = pd.concat([repos_with_labels, repos_without_labels])
        repo_df = repo_df.drop_duplicates(subset=['id'])
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_tags(repo_df, existing_repo_dir, error_file_path):
    """Function to get tags for a repo
    :param row: row from repo_df
    :return: list of tags"""
    response = requests.get(row.tags_url, headers=auth_headers)
    response_data = get_response_data(response, row.tags_url)
    if response_data is not None:
        tags_df = pd.DataFrame(response_data)
        tags = tags_df['name'].tolist()
    else:
        tags = []
    return tags

def get_repo_tags(repo_df, existing_repo_dir, error_file_path):
    """Function to get tags for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with tags"""
    drop_fields = ["full_name", "error_url"]
    clean_write_error_file(error_file_path, drop_fields)
    if 'tags' in repo_df.columns:
        repos_without_tags = repo_df[repo_df.tags.isna()]
    else: 
        repos_without_tags = repo_df

    if len(repos_without_tags) > 0:
        tqdm.pandas(desc="Getting Tags")
        repos_without_tags['tags'] = repos_without_tags.progress_apply(get_tags, axis=1, existing_repo_dir=existing_repo_dir, error_file_path=error_file_path)

def get_profiles(repo_df, existing_repo_dir, error_file_path):
    """Function to get community profiles and health percentages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param existing_repo_dir: path to directory to write existing repos
    :param error_file_path: path to file to write errors
    """
    profile_bar = tqdm(total=len(repo_df), desc="Getting Community Profiles")
    for _, row in repo_df.iterrows():
        existing_entities_path = f"{row.full_name.replace('/', '_').replace(' ', '_')}_coding_dh_repo.csv"
        existing_output_path = existing_repo_dir + existing_entities_path
        if os.path.exists(existing_output_path):
            existing_df = pd.read_csv(existing_output_path)
            try:
                query = row.url + '/community/profile'
                response, status_code = make_request_with_rate_limiting(query, auth_headers)
                if response is not None:
                    response_df = pd.json_normalize(response)
                    if 'message' in response_df.columns:
                        console.print(response_df.message.values[0], style="bold red")
                        additional_data = {'repo_full_name': row.full_name}
                        log_error_to_file(error_file_path, additional_data, status_code, query)
                        profile_bar.update(1)
                        continue
                    response_df = response_df.rename(columns={'updated_at': 'community_profile_updated_at'})
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

def get_repo_profile(repo_df,  error_file_path, existing_repo_dir):
    """Function to get community profiles and health percentages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param error_file_path: path to file to write errors
    :param existing_repo_dir: path to directory to write existing repos
    """
    drop_fields = ["full_name", "error_url"]
    clean_write_error_file(error_file_path, drop_fields)
    if 'health_percentage' in repo_df.columns:
        repos_without_community_profile = repo_df[repo_df.health_percentage.isna()]
    else: 
        repos_without_community_profile = repo_df

    if len(repos_without_community_profile) > 0:
        get_profiles(repos_without_community_profile, existing_repo_dir, error_file_path)

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
    # repo_df = pd.read_csv("../data/temp/repo_metadata.csv")
    # repo_df = get_repo_owners(repo_df)
    # repo_df = get_repo_languages(repo_df, "../data/temp/repo_metadata.csv")
    # repo_df = get_repo_labels(repo_df, "../data/temp/repo_metadata.csv")
    # get_repo_profile(repo_df, "../data/temp/repo_metadata.csv", "../data/coding_dh_repo/")
    # get_repo_tags(repo_df, "../data/coding_dh_repo/", "../data/temp/repo_metadata_errors.csv")
    # repo_df = get_repo_tags(repo_df, "../data/temp/repo_metadata.csv")
    # repo_df.to_csv("../data/temp/repo_metadata.csv", index=False)
