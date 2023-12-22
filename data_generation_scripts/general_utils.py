import re
import time
import pandas as pd
import requests
import apikey
import os
import shutil
from tqdm import tqdm
from datetime import datetime
import altair as alt
import warnings
warnings.filterwarnings('ignore')
import vl_convert as vlc
from typing import Optional, List, Any
import numpy as np
from rich import print
from rich.console import Console
from typing import List, Union, Dict, Any
from requests.models import Response

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()

data_directory_path = "../../datasets"

def check_rate_limit() -> pd.DataFrame:
    """Function to check rate limit status
    :return: data from rate limit api call"""
    # Checks for rate limit so that you don't hit issues with Github API. Mostly for search API that has a 30 requests per minute https://docs.github.com/en/rest/rate-limit
    url = 'https://api.github.com/rate_limit'
    response = requests.get(url, headers=auth_headers, timeout=10)
    if response.status_code != 200:
        console.print(f'Failed to retrieve rate limit with status code: {response.status_code}', style='bold red')
        return pd.DataFrame()
    rates_df = pd.json_normalize(response.json())
    return rates_df

def make_request_with_rate_limiting(url: str, auth_headers: dict) -> requests.Response:
    """Make a GET request to the URL, handling rate limiting."""
    for index in range(3):
        response = requests.get(url, headers=auth_headers, timeout=10)
        if response.status_code == 200:
            return response
        if index == 0:
            console.print('hit rate limiting. trying to sleep...', style='bold red')
            time.sleep(120)
        if index == 1:
            rates_df = check_rate_limit()
            if rates_df['resources.core.remaining'].values[0] == 0:
                console.print('rate limit reached. sleeping for 1 hour', style='bold red')
                time.sleep(3600)
    console.print(f'query failed after 3 attempts with code {response.status_code}. Failing URL: {url}', style='bold red')
    return None

def check_total_pages(url: str, auth_headers: dict) -> int:
    """Function to check total number of pages to get from search. Useful for not going over rate limit."""
    response = make_request_with_rate_limiting(f'{url}?per_page=1', auth_headers)
    if response is None or len(response.links) == 0:
        return 1
    match = re.search(r'\d+$', response.links['last']['url'])
    return int(match.group()) if match is not None else 1

def check_total_results(url: str, auth_headers: dict) -> Optional[int]:
    """Function to check total number of results from API. Useful for not going over rate limit. Differs from check_total_pages because this returns all results, not just total number of pagination."""
    response = make_request_with_rate_limiting(url, auth_headers)
    if response is None:
        data = {'total_count': None}
    else:
        data = response.json()
    return data.get('total_count')

def get_response_data(response: Response, query: str, auth_headers: Dict[str, str]) -> Union[List, Dict[str, Any]]:
    """Function to get and process response data from GitHub API call."""
    # First, check if response is valid
    if response.status_code == 401:
        console.print("response code 401 - unauthorized access. check api key", style='bold red')
        return []
    elif response.status_code == 204:
        console.print(f'No data for {query}', style='bold red')
        return []
    elif response.status_code == 200:
        return response.json()
    else:
        console.print(f'response code: {response.status_code}. hit rate limiting. trying to sleep...', style='bold red')
        response = make_request_with_rate_limiting(query, auth_headers)
        if response is None or response.status_code != 200:
            console.print(f'query failed with code {response.status_code if response else "unknown"}. Failing URL: {query}', style='bold red')
            return []
        else:
            return response.json()

def read_csv_file(directory: str, file_name: str) -> Optional[pd.DataFrame]:
    """Reads a CSV file into a pandas DataFrame."""
    try:
        return pd.read_csv(os.path.join(directory, file_name), low_memory=False, encoding='utf-8')
    except pd.errors.EmptyDataError:
        print(f'Empty dataframe for {file_name}')
        return None

def create_file_dict(directory: str, file_name: str) -> dict:
    """Create a file dictionary with name, size, and directory."""
    file_dict = {}
    loaded_file = os.path.join(directory, file_name)
    file_size = os.path.getsize(loaded_file)
    file_dict['file_name'] = file_name
    file_dict['file_size'] = file_size
    file_dict['directory'] = directory
    return file_dict

def read_combine_files(dir_path: str, check_all_dirs: bool=False, file_path_contains: Optional[str]=None, large_files: bool=False) -> pd.DataFrame:
    """Combines all files in a directory."""
    excluded_dirs = ['temp', 'derived_files', 'metadata_files', 'repo_data', 'user_data', 'derived_files', 'archived_data', 'error_logs', 'archived_files']
    rows = []
    relevant_files = []

    for directory, _, files in os.walk(dir_path):
        if check_all_dirs and directory == '../data':
            continue
        if check_all_dirs and any(excluded_dir in directory for excluded_dir in excluded_dirs):
            continue

        for file_name in files:
            if file_path_contains is None or file_path_contains in file_name:
                if large_files:
                    relevant_files.append(create_file_dict(directory, file_name))
                else:
                    row = read_csv_file(directory, file_name)
                    if row is not None:
                        rows.append(row)

    if large_files:
        files_df = pd.DataFrame(relevant_files)
        if len(files_df) == 0:
            return pd.DataFrame()
        files_df['date'] = "202" + files_df['file_name'].str.split('202').str[1].str.split('.').str[0]
        files_df.date = files_df.date.str.replace("_", "-")
        files_df.date = pd.to_datetime(files_df.date)
        top_files = files_df.sort_values(by=['file_size', 'date'], ascending=[False, False]).head(2)
        rows = [read_csv_file(row.directory, row.file_name) for _, row in top_files.iterrows() if row is not None]

    combined_df = pd.concat(rows) if len(rows) > 0 else pd.DataFrame()
    return combined_df


def check_return_error_file(error_file_path):
    """Function to check if error file exists and return it if it does
    :param error_file_path: path to error file
    :return: error dataframe"""
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        return error_df
    else:
        return pd.DataFrame()

def clean_write_error_file(error_file_path, drop_field):
    """Function to clean error file and write it
    :param error_file_path: path to error file
    :param drop_field: field to drop from error file"""
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        if 'error_time' in error_df.columns:
            error_df = error_df.sort_values(by=['error_time']).drop_duplicates(subset=[drop_field], keep='last')
        else: 
            error_df = error_df.drop_duplicates(subset=[drop_field], keep='last')
        error_df.to_csv(error_file_path, index=False)
    else:
        print('no error file to clean')

def check_if_older_file_exists(file_path):
    """Function to check if older file exists and move it to older_files folder
    :param file_path: path to file"""
    if os.path.exists(file_path):
        src = file_path 
        new_file_path = file_path.replace('/data/','/data/older_files/')
        time_stamp = datetime.now().strftime("%Y_%m_%d")
        dst = new_file_path.replace('.csv', f'_{time_stamp}.csv')
        
        new_dir = os.path.dirname(new_file_path)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)

        if not os.path.exists(dst):
            shutil.copy2(src, dst)  


def check_for_entity_in_older_queries(entity_path, entity_df, is_large=True):
    """Function to check if entity exists in older queries and add it to our most recent version of the file
    :param entity_path: path to entity file
    :param entity_df: entity dataframe"""
    entity_type = entity_path.split('/')[-1].split('_dataset')[0]
    print(entity_type)
    older_entity_file_path = entity_path.replace('data/', 'data/older_files/')
    older_entity_file_dir = os.path.dirname(older_entity_file_path) + '/'


    older_entity_df = read_combine_files(dir_path=older_entity_file_dir, check_all_dirs=True, file_path_contains=entity_type, large_files=is_large)
    print(f'older entity df shape: {older_entity_df.shape}')
    if len(older_entity_df) > 0:
        entity_field = 'full_name' if entity_type == 'repos' else 'login'
        missing_entities = older_entity_df[~older_entity_df[entity_field].isin(entity_df[entity_field])]

        if entity_type == 'users':
            user_headers = pd.read_csv('../data/metadata_files/users_dataset_cols.csv')
            if set(missing_entities.columns) != set(user_headers.columns):
                error_file_path = "../data/error_logs/potential_users_errors.csv"
                error_df = check_return_error_file(error_file_path)
                now = pd.Timestamp('now')
                error_df['error_time'] = pd.to_datetime(error_df['error_time'], errors='coerce')
                error_df = error_df.dropna(subset=['error_time'])  # Drop any rows where 'error_time' is NaT
                error_df['time_since_error'] = (now - error_df['error_time']).dt.days
                error_df = error_df[error_df.time_since_error > 7]
                missing_entities = missing_entities[~missing_entities.login.isin(error_df.login)]
                missing_entities = missing_entities[user_headers.columns]
        if entity_type == 'repos':
            repo_headers = pd.read_csv('../data/metadata_files/repo_headers.csv')
            if set(missing_entities.columns) != set(repo_headers.columns):
                error_file_path = "../data/error_logs/potential_repos_errors.csv"
                error_df = check_return_error_file(error_file_path)
                now = pd.Timestamp('now')
                error_df['error_time'] = pd.to_datetime(error_df['error_time'], errors='coerce')
                error_df = error_df.dropna(subset=['error_time'])  # Drop any rows where 'error_time' is NaT
                error_df['time_since_error'] = (now - error_df['error_time']).dt.days
                error_df = error_df[error_df.time_since_error > 7]
                missing_entities = missing_entities[~missing_entities.full_name.isin(error_df.full_name)]
                missing_entities = missing_entities[repo_headers.columns]
        if len(missing_entities) > 0:
            missing_entities = missing_entities[missing_entities.id.notna()]
            entity_df = pd.concat([entity_df, missing_entities])
            cleaned_field = 'cleaned_repo_query_time' if entity_type == 'repos' else 'cleaned_user_query_time'
            time_field = 'repo_query_time' if entity_type == 'repos' else 'user_query_time'
            entity_df[cleaned_field] = pd.to_datetime(entity_df[time_field], errors='coerce')
            entity_field = 'full_name' if 'repo' in entity_type else 'login'
            entity_df = entity_df.sort_values(by=[cleaned_field], ascending=False).drop_duplicates(subset=[entity_field], keep='first').drop(columns=[cleaned_field])
    check_if_older_file_exists(entity_path)
    entity_df.to_csv(entity_path, index=False)
    return entity_df

def check_file_size_and_move(file_dir):
    """Function to check if file size is too large and move it
    :param file_dir: path to file directory"""
    for dir, _, files in os.walk(file_dir):
        for file in files:
            file_path = os.path.join(dir, file)
            size = os.path.getsize(file_path)
            size = round(size/(1024*1024), 2)
            if size > 50:
            
                new_file_path = file_path.replace('data/', 'data/large_files/')
            
                if not os.path.exists(new_file_path):
                    shutil.copy2(file_path, new_file_path)
                    os.remove(file_path)

def check_file_created(file_path, existing_df):
    df = pd.read_csv(file_path, low_memory=False)
    if len(df) == len(existing_df):
        return True
    else:
        print(f'File {file_path} not created correctly')
        return False

def get_core_users_repos(combine_files=True):
    """Function to get core users and repos
    :return: core users and repos
    """
    initial_core_users = pd.read_csv("../data/derived_files/initial_core_users.csv")
    initial_core_users['origin'] = 'initial_core'
    initial_core_repos = pd.read_csv("../data/derived_files/initial_core_repos.csv")
    initial_core_repos['origin'] = 'initial_core'
    initial_core_orgs = pd.read_csv("../data/derived_files/initial_core_orgs.csv")
    initial_core_orgs['origin'] = 'initial_core'

    firstpass_core_users = pd.read_csv("../data/derived_files/firstpass_core_users.csv")
    firstpass_core_users['origin'] = 'firstpass_core'
    firstpass_core_repos = pd.read_csv("../data/derived_files/firstpass_core_repos.csv")
    firstpass_core_repos['origin'] = 'firstpass_core'
    firstpass_core_orgs = pd.read_csv("../data/derived_files/firstpass_core_orgs.csv")
    firstpass_core_orgs['origin'] = 'firstpass_core'

    finalpass_core_users = pd.read_csv("../data/derived_files/finalpass_core_users.csv")
    finalpass_core_users['origin'] = 'finalpass_core'
    finalpass_core_repos = pd.read_csv("../data/large_files/derived_files/finalpass_core_repos.csv", low_memory=False, on_bad_lines='skip')
    finalpass_core_repos['origin'] = 'finalpass_core'
    finalpass_core_orgs = pd.read_csv("../data/derived_files/finalpass_core_orgs.csv")
    finalpass_core_orgs['origin'] = 'finalpass_core'

    if combine_files:
        core_users = pd.concat([initial_core_users, firstpass_core_users, finalpass_core_users])
        core_repos = pd.concat([initial_core_repos, firstpass_core_repos, finalpass_core_repos])
        core_orgs = pd.concat([initial_core_orgs, firstpass_core_orgs, finalpass_core_orgs])
        return core_users, core_repos, core_orgs
    else:
        return initial_core_users, initial_core_repos, initial_core_orgs, firstpass_core_users, firstpass_core_repos, firstpass_core_orgs, finalpass_core_users, finalpass_core_repos, finalpass_core_orgs


def save_chart(chart, filename, scale_factor=2.0):
    '''
    Save an Altair chart using vl-convert
    
    Parameters
    ----------
    chart : altair.Chart
        Altair chart to save
    filename : str
        The path to save the chart to
    scale_factor: int or float
        The factor to scale the image resolution by.
        E.g. A value of `2` means two times the default resolution.
    '''
    with alt.data_transformers.enable("default"), alt.data_transformers.disable_max_rows():
        if filename.split('.')[-1] == 'svg':
            with open(filename, "w") as f:
                f.write(vlc.vegalite_to_svg(chart.to_dict()))
        elif filename.split('.')[-1] == 'png':
            with open(filename, "wb") as f:
                f.write(vlc.vegalite_to_png(chart.to_dict(), scale=scale_factor))
        else:
            raise ValueError("Only svg and png formats are supported")