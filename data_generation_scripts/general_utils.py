# Standard library imports
import os
import re
import shutil
import time
import warnings
from datetime import datetime
from typing import List, Optional, Union

# Related third-party imports
import altair as alt
import apikey
import numpy as np
import pandas as pd
import requests
from rich import print
from rich.console import Console
from tqdm import tqdm

# Local application/library specific imports
import vl_convert as vlc

# Filter warnings
warnings.filterwarnings('ignore')

# Load auth token
auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()

data_directory_path = "../../datasets"

def check_rate_limit() -> pd.DataFrame:
    """
    Checks rate limit status on GitHub API

    :return: data from rate limit api call
    """
    # Checks for rate limit so that you don't hit issues with Github API. Mostly for search API that has a 30 requests per minute https://docs.github.com/en/rest/rate-limit
    url = 'https://api.github.com/rate_limit'
    # Make request
    response = requests.get(url, headers=auth_headers, timeout=10)
    if response.status_code != 200:
        console.print(f'Failed to retrieve rate limit with status code: {response.status_code}', style='bold red')
        return pd.DataFrame()
    # Convert to dataframe
    rates_df = pd.json_normalize(response.json())
    return rates_df

def make_request_with_rate_limiting(url: str, auth_headers: dict, number_of_attempts: int = 3, timeout: int = 10) -> requests.Response:
    """
    Makes a GET request to the specified URL with handling for rate limiting. If the request encounters rate limiting, 
    it will attempt to retry the request a specified number of times before giving up. The function also adheres to 
    a timeout for server response.

    :param url: String representing the URL to which the request is made.
    :param auth_headers: Dictionary containing authentication headers for the request.
    :param number_of_attempts: Integer specifying the maximum number of attempts for the request. Defaults to 3.
    :param timeout: Integer specifying the timeout in seconds to wait for a response from the server. Defaults to 10.
    :return: The response object from the requests library representing the outcome of the GET request.
    """
    # Set range for number of attempts
    for index in range(number_of_attempts):
        response = requests.get(url, headers=auth_headers, timeout=timeout)
        # Check if response is valid and return it if it is
        if response.status_code == 200:
            return response
        elif response.status_code == 401:
            console.print("response code 401 - unauthorized access. check api key", style='bold red')
            return None
        elif response.status_code == 204:
            console.print(f'No data for {url}', style='bold red')
            return None
        # If not, check if it's a rate limit issue
        if index == 0:
            console.print('hit rate limiting. trying to sleep...', style='bold red')
            time.sleep(120)
        # If it is, wait for an hour and try again
        if index == 1:
            rates_df = check_rate_limit()
            if rates_df['resources.core.remaining'].values[0] == 0:
                console.print('rate limit reached. sleeping for 1 hour', style='bold red')
                time.sleep(3600)
    # If it's not a rate limit issue, return None
    console.print(f'query failed after {number_of_attempts} attempts with code {response.status_code}. Failing URL: {url}', style='bold red')
    return None

def check_total_pages(url: str, auth_headers: dict) -> int:
    """
    Checks total number of pages for a given url on the GitHub API.

    :param url: URL to check
    :param auth_headers: Authentication headers
    :return: Total number of pages. If there are no links or response is None, returns 1.
    """
    # Get total number of pages
    response = make_request_with_rate_limiting(f'{url}?per_page=1', auth_headers)
    # If response is None or there are no links, return 1
    if response is None or len(response.links) == 0:
        return 1
    # Otherwise, get the last page number
    match = re.search(r'\d+$', response.links['last']['url'])
    return int(match.group()) if match is not None else 1

def check_total_results(url: str, auth_headers: dict) -> Optional[int]:
    """
    Checks total number of results for a given url on the GitHub API.
    
    :param url: URL to check
    :param auth_headers: Authentication headers
    :return: Total number of results. If response is None, returns None.
    """
    # Get total number of results
    response = make_request_with_rate_limiting(url, auth_headers)
    # If response is None, return None
    if response is None:
        data = {'total_count': None}
    else:
        data = response.json()
    # Return total count
    return data.get('total_count')
        
def read_csv_file(file_name: str, directory: Optional[str] = None, encoding: Optional[str] = 'utf-8', error_bad_lines: Optional[bool] = False) -> Optional[pd.DataFrame]:
    """
    Reads a CSV file into a pandas DataFrame. This function allows specification of the directory, encoding, 
    and handling of bad lines in the CSV file. If the file cannot be read, the function returns None.

    :param file_name: String representing the name of the CSV file to read.
    :param directory: Optional string specifying the directory where the file is located. If None, it is assumed the file is in the current working directory.
    :param encoding: Optional string specifying the encoding used in the CSV file. Defaults to 'utf-8'.
    :param error_bad_lines: Optional boolean indicating whether to skip bad lines in the CSV. If False, an error is raised for bad lines. Defaults to False.
    :return: A pandas DataFrame containing the data from the CSV file, or None if the file cannot be read.
    """
    # Read in the file
    file_path = file_name if directory is None else os.path.join(directory, file_name)
    try:
        # Return the dataframe
        return pd.read_csv(file_path, low_memory=False, encoding=encoding, error_bad_lines=error_bad_lines)
    # If there's a Pandas error, print it and return None
    except pd.errors.EmptyDataError:
        console.print(f'Empty dataframe for {file_name}', style='bold red')
        return None
    # If there's an error, print it and return None
    except Exception as e:
        console.print(f'Failed to read {file_name} with {encoding} encoding. Error: {e}', style='bold red')
        return None

def check_return_error_file(error_file_path:str) -> pd.DataFrame():
    """
    Checks if error file exists and returns it if it does.
    
    :param error_file_path: Path to error file
    :return: Error dataframe
    """
    # Check if error file exists and return it if it does
    if os.path.exists(error_file_path):
        error_df = read_csv_file(error_file_path)
        return error_df
    else:
        return pd.DataFrame()

def clean_write_error_file(error_file_path: str, drop_field: str) -> None:
    """
    Cleans error file and writes it. Drops duplicates if error_time column exists. Also drops duplicates based on drop_field column.

    :param error_file_path: Path to error file
    :param drop_field: Field to drop duplicates on
    """
    # Clean error file and write it
    if os.path.exists(error_file_path):
        error_df = read_csv_file(error_file_path)
        # Drop duplicates if error_time exists
        if 'error_time' in error_df.columns:
            error_df = error_df.sort_values(by=['error_time']).drop_duplicates(subset=[drop_field], keep='last')
        else: 
            error_df = error_df.drop_duplicates(subset=[drop_field], keep='last')
        error_df.to_csv(error_file_path, index=False)
    else:
        console.print('No error file to clean', style='bold blue')

def check_file_size_and_move(file_dir: str) -> None:
    """
    Checks file size and moves it if it is too large.

    :param file_dir: Directory of file
    """
    # Check if file size is too large and move it
    for dir, _, files in os.walk(file_dir):
        for file in files:
            file_path = os.path.join(dir, file)
            size = os.path.getsize(file_path)
            size = round(size/(1024*1024), 2)
            if size > 50:
                console.print(f'File {file_path} is {size} MB', style='bold red')
                new_file_path = file_path.replace(f'{data_directory_path}/', f'{data_directory_path}/large_files/')
                if not os.path.exists(new_file_path):
                    shutil.copy2(file_path, new_file_path)
                    os.remove(file_path)

def check_file_created(file_path :str, existing_df: pd.DataFrame) -> bool:
    """
    Checks if csv file was created correctly and that it has the same length as its current DataFrame.

    :param file_path: Path to file
    :param existing_df: Existing dataframe
    :return: Boolean indicating whether file was created correctly
    """
    # Check if file was created correctly
    df = read_csv_file(file_path)
    if len(df) == len(existing_df):
        return True
    else:
        console.print(f'File {file_path} not created correctly', style='bold red')
        return False
    
def check_if_older_file_exists(file_path: str) -> None:
    """
    Checks if older file exists and moves it if it does.

    :param file_path: Path to file
    """
    # Check if older file exists and move it
    if os.path.exists(file_path):
        src = file_path 
        # Create new file path
        new_file_path = file_path.replace(f'{data_directory_path}/',f'{data_directory_path}/older_files/')
        time_stamp = datetime.now().strftime("%Y_%m_%d")
        dst = new_file_path.replace('.csv', f'_{time_stamp}.csv')
        # Create new directory if it doesn't exist
        new_dir = os.path.dirname(new_file_path)
        if not os.path.exists(new_dir):
            os.makedirs(new_dir)
        # Move file
        if not os.path.exists(dst):
            shutil.copy2(src, dst)  

def create_file_dict(directory: str, file_name: str) -> dict:
    """
    Creates a file dictionary with name, size, and directory.

    :param directory: Directory of file
    :param file_name: Name of file
    :return: File dictionary
    """
    # Create a file dictionary with name, size, and directory
    file_dict = {}
    loaded_file = os.path.join(directory, file_name)
    file_size = os.path.getsize(loaded_file)
    file_dict['file_name'] = file_name
    file_dict['file_size'] = file_size
    file_dict['directory'] = directory
    return file_dict


def read_combine_files(dir_path: str, check_all_dirs: bool = False, file_path_contains: Optional[str] = None, large_files: bool = False) -> pd.DataFrame:
    """
    Combines all relevant files within a specified directory into a single pandas DataFrame. This function can optionally exclude certain directories, filter files by name, and handle large files differently.

    :param dir_path: String specifying the directory path to search for files.
    :param check_all_dirs: Boolean indicating whether to check all subdirectories within the specified directory. Defaults to False.
    :param file_path_contains: Optional string for filtering files by name. Only files containing this string are processed.
    :param large_files: Boolean indicating whether to handle large files differently. If True, only metadata is collected initially. Defaults to False.
    :return: A pandas DataFrame combining data from all relevant files.
    """
    # List of directories to exclude
    excluded_dirs = ['temp', 'derived_files', 'metadata_files', 'repo_data', 'user_data', 'derived_files', 'archived_data', 'error_logs', 'archived_files']
    rows = []
    relevant_files = []
    # Walk through the directory
    for directory, _, files in os.walk(dir_path):
        # If check_all_dirs is False or directory is data directory or excluded directory, skip it
        if check_all_dirs and (directory == data_directory_path or any(excluded_dir in directory for excluded_dir in excluded_dirs)):
            continue
        # Check all files in directory
        for file_name in files:
            # If file_path_contains is None or file_name contains file_path_contains, process it
            if file_path_contains is None or file_path_contains in file_name:
                file_path = os.path.join(directory, file_name)
                if large_files:
                    relevant_files.append(create_file_dict(directory, file_name))  # 
                else:
                    try:
                        row = read_csv_file(file_path)  # Assuming read_csv_file is capable of reading a file given its path
                        if row is not None:
                            rows.append(row)
                    except Exception as e:
                        print(f"Error reading file {file_path}: {e}")
    # If large_files is True, then turn relevant_files into a dataframe and sort by file_size and date
    if large_files:
        files_df = pd.DataFrame(relevant_files)
        if len(files_df) == 0:
            return pd.DataFrame()
        files_df['date'] = "202" + files_df['file_name'].str.split('202').str[1].str.split('.').str[0]
        files_df.date = files_df.date.str.replace("_", "-")
        files_df.date = pd.to_datetime(files_df.date)
        top_files = files_df.sort_values(by=['file_size', 'date'], ascending=[False, False]).head(2)
        rows = [read_csv_file(row.directory, row.file_name) for _, row in top_files.iterrows() if row is not None]
    # Combine all rows into a single dataframe
    combined_df = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
    return combined_df

def handle_entity_type(entity_type: str, missing_entities: pd.DataFrame, error_file_path: str, headers_file_path: str) -> pd.DataFrame:
    """
    Handles entity type by checking for errors and removing entities that have errors.

    :param entity_type: Type of entity
    :param missing_entities: Missing entities dataframe
    :param error_file_path: Path to error file
    :param headers_file_path: Path to headers file
    :return: Missing entities dataframe
    """
    # Read in error file and headers file
    headers = read_csv_file(headers_file_path)
    if set(missing_entities.columns) != set(headers.columns):
        error_df = check_return_error_file(error_file_path)
        now = pd.Timestamp('now')
        error_df['error_time'] = pd.to_datetime(error_df['error_time'], errors='coerce')
        error_df = error_df.dropna(subset=['error_time'])  # Drop any rows where 'error_time' is NaT
        error_df['time_since_error'] = (now - error_df['error_time']).dt.days
        error_df = error_df[error_df.time_since_error > 7]
        entity_field = 'full_name' if entity_type == 'repos' else 'login'
        # Drop entities that have errors
        missing_entities = missing_entities[~missing_entities[entity_field].isin(error_df[entity_field])]
        # Drop columns that are not in headers
        missing_entities = missing_entities[headers.columns]
    return missing_entities

def check_for_entity_in_older_queries(entity_path: str, entity_df: pd.DataFrame, is_large: bool =True) -> pd.DataFrame:
    """
    Checks for entity in older queries and adds it to the entity dataframe if it is not there.

    :param entity_path: Path to entity file
    :param entity_df: Entity dataframe
    :param is_large: Boolean indicating whether to handle large files differently. If True, only metadata is collected initially. Defaults to True.
    :return: Entity dataframe
    """
    # Extract entity type
    entity_type = entity_path.split("/")[-1].split("_dataset")[0]
    console.print(f"Checking for {entity_type} in older queries", style="bold blue")
    # Check for entity in older queries
    older_entity_file_path = entity_path.replace(f"{data_directory_path}/", f"{data_directory_path}/older_files/")
    older_entity_file_dir = os.path.dirname(older_entity_file_path) + "/"
    older_entity_df = read_combine_files(dir_path=older_entity_file_dir, check_all_dirs=True, file_path_contains=entity_type, large_files=is_large)
    console.print(f"older entity df shape: {older_entity_df.shape}", style="bold blue")
    if len(older_entity_df) > 0:
        entity_field = "full_name" if entity_type == "repos" else "login"
        missing_entities = older_entity_df[~older_entity_df[entity_field].isin(entity_df[entity_field])]

        if entity_type == "users":
            missing_entities = handle_entity_type("users", missing_entities, f"{data_directory_path}/error_logs/potential_users_errors.csv", f"{data_directory_path}/metadata_files/users_dataset_cols.csv")
        if entity_type == "repos":
            missing_entities = handle_entity_type("repos", missing_entities, f"{data_directory_path}/error_logs/potential_repos_errors.csv", f"{data_directory_path}/metadata_files/repo_headers.csv")

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

def get_headers(entity_type: str) -> pd.DataFrame:
    """
    Gets headers for entity type.

    :param entity_type: Type of entity
    :return: Headers dataframe
    """
    # Get headers for entity type
    if entity_type == 'users':
        headers = pd.read_csv(f'{data_directory_path}/metadata_files/users_dataset_cols.csv')
    elif entity_type == 'repos':
        headers = pd.read_csv(f'{data_directory_path}/metadata_files/repo_headers.csv')
    elif entity_type == 'orgs':
        headers = pd.read_csv(f'{data_directory_path}/metadata_files/org_headers.csv')
    else:
        console.print(f'No headers for {entity_type}', style='bold red')
        return None
    return headers

def get_new_entities(entity_type:str, potential_new_entities_df: pd.DataFrame, temp_entity_dir: str, entity_progress_bar: tqdm, error_file_path: str, overwrite_existing_temp_files:bool = True) -> pd.DataFrame:
    """
    Gets new entities from GitHub API. 

    :param entity_type: Type of entity
    :param potential_new_entities_df: Potential new entities dataframe
    :param temp_entity_dir: Temporary entity directory
    :param entity_progress_bar: Entity progress bar
    :param error_file_path: Path to error file
    :param overwrite_existing_temp_files: Boolean indicating whether to overwrite existing temporary files. Defaults to True.
    :return: Combined entity dataframe
    """

    # Delete existing temporary directory and create it again if overwrite_existing_temp_files is True
    if (os.path.exists(temp_entity_dir) )and (overwrite_existing_temp_files):
        shutil.rmtree(temp_entity_dir)
    # Create temporary directory if it doesn't exist
    if not os.path.exists(temp_entity_dir):
        os.makedirs(temp_entity_dir)
    
    # Subset headers for orgs and users
    user_cols = ['bio', 'followers_url', 'following_url', 'gists_url', 'gravatar_id', 'hireable', 'organizations_url','received_events_url', 'site_admin', 'starred_url',
    'subscriptions_url','user_query_time', 'login',]

    # Get headers
    headers = get_headers(entity_type)

    # Update progress bar
    entity_progress_bar.total = len(potential_new_entities_df)
    entity_progress_bar.refresh()
    # org_progress_bar = tqdm(total=len(org_df), desc="Cleaning Orgs", position=0)

    # Get entity column based on entity type
    entity_column = 'full_name' if entity_type == 'repos' else 'login'

    # Loop through potential new entities
    for _, row in potential_new_entities_df.iterrows():
        try:
            # Create temporary file path
            temp_entities_path = f"{row[entity_column].replace('/', '')}_potential_{entity_type}.csv"
            # Check if file exists
            if os.path.exists(f"{temp_entity_dir}/{temp_entities_path}"):
                entity_progress_bar.update(1)
                continue
            # Get query
            query = row.url
            if entity_type == "orgs":
                query = row.url if '/users/' in row.url else row.url.replace('/orgs/', '/users/')
            # Make request
            response = make_request_with_rate_limiting(query, auth_headers)
            # If response is None, update progress bar and continue
            if response is None and entity_type != "orgs":
                entity_progress_bar.update(1)
                continue
            # If response is None and entity type is orgs, create empty dataframe
            elif response is None and entity_type == "orgs":
                response_df = pd.DataFrame(columns=headers.columns, data=None, index=None)
            else:
                response_data = response.json()
                response_df = pd.json_normalize(response_data)
                if 'message' in response_df.columns:
                    console.print(f"Error for {row[entity_column]}: {response_df.message.values[0]}", style='bold red')
                    entity_progress_bar.update(1)
                    continue
            
            if entity_type != "orgs":
                response_df = response_df[headers.columns]
                response_df.to_csv(f"{temp_entity_dir}/{temp_entities_path}", index=False)
            else:
                response_df = response_df[user_cols]
                query = row.url.replace('/users/', '/orgs/') if '/users/' in row.url else row.url
                response = make_request_with_rate_limiting(query, auth_headers)
                if response is None:
                    expanded_df = pd.DataFrame(columns=headers.columns, data=None, index=None)
                else:
                    response_data = response.json()
                    expanded_df = pd.json_normalize(response_data)
                expanded_df = expanded_df[headers.columns]
                common_columns = list(set(response_df.columns).intersection(set(expanded_df.columns)))
                final_df = pd.merge(response_df, expanded_df, on=common_columns, how='left')
                final_df.to_csv(f"{temp_entity_dir}/{temp_entities_path}", index=False)

                entity_progress_bar.update(1)
        except Exception as e:
            console.print(f"Error for {row[entity_column]}: {e}", style='bold red')
            error_df = pd.DataFrame([{entity_column: row[entity_column], 'error_time': time.time(), 'error_url': row.url}])
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            entity_progress_bar.update(1)
            continue

    # Read in all temporary files
    combined_entity_df = read_combine_files(temp_entity_dir)
    if overwrite_existing_temp_files:
        shutil.rmtree(temp_entity_dir)
    entity_progress_bar.close()
    return combined_entity_df

def get_entity_df(entity_path: str) -> pd.DataFrame:
    """
    Gets entity dataframe.

    :param entity_path: Path to entity file
    :return: Entity dataframe
    """
    # Get entity dataframe
    entity_df = read_csv_file(entity_path)
    return entity_df

def check_add_new_entities(potential_new_entities_df: pd.DataFrame, output_path: str, entity_type: str, return_df: bool, overwrite_existing_temp_files: bool) -> Optional[pd.DataFrame]:
    """
    Function to check if entities (users, organizations, or repositories) are already in the respective file and add them if not.

    :param potential_new_entities_df: DataFrame of new identified entities (users, organizations, or repositories).
    :param output_path: Path to the output file for entities.
    :param entity_type: Type of entity ('users', 'orgs', or 'repos').
    :param return_df: Boolean to return the DataFrame or not.
    :param overwrite_existing_temp_files: Boolean to overwrite existing temp files or not.
    :return: DataFrame of entities if return_df is True, otherwise None.
    """

    temp_dir = f"{data_directory_path}/temp/temp_{entity_type}/"
    excluded_file_path = f'{data_directory_path}/metadata_files/excluded_{entity_type}.csv'
    error_file_path = f"{data_directory_path}/error_logs/potential_{entity_type}_errors.csv"
    headers_file_path = f'{data_directory_path}/metadata_files/{entity_type}_headers.csv'
    identifier = 'full_name' if entity_type == 'repos' else 'login'
    # Load headers and excluded entities
    headers_df = read_csv_file(headers_file_path)
    if os.path.exists(excluded_file_path):
        excluded_entities = read_csv_file(excluded_file_path)
        # Exclude entities and check for errors
        potential_new_entities_df = potential_new_entities_df[~potential_new_entities_df[identifier].isin(excluded_entities[identifier])]
    error_df = check_return_error_file(error_file_path)

    if os.path.exists(output_path):
        existing_df = read_csv_file(output_path)
        new_entities_df = potential_new_entities_df[~potential_new_entities_df[identifier].isin(existing_df[identifier])]
        if len(error_df) > 0:
            new_entities_df = new_entities_df[~new_entities_df[identifier].isin(error_df[identifier])]
        if len(new_entities_df) > 0:
            progress_bar = tqdm(total=len(new_entities_df), desc=f'Getting {entity_type.capitalize()}', position=1)

            expanded_new_entities = get_new_entities(entity_type, new_entities_df, temp_dir, progress_bar, error_file_path, overwrite_existing_temp_files)
        else:
            expanded_new_entities = new_entities_df
        combined_df = pd.concat([existing_df, expanded_new_entities])
        combined_df = combined_df.drop_duplicates(subset=[identifier])
    else:
        new_entities_df = potential_new_entities_df.copy()
        progress_bar = tqdm(total=len(new_entities_df), desc=f'{entity_type.capitalize()}', position=1)
        combined_df = get_new_entities(entity_type, new_entities_df, temp_dir, progress_bar, error_file_path, overwrite_existing_temp_files)
    
    clean_write_error_file(error_file_path, identifier)
    check_if_older_file_exists(output_path)
    combined_df[f'{entity_type}_query_time'] = datetime.now().strftime("%Y-%m-%d")
    combined_df.to_csv(output_path, index=False)

    if return_df:
        combined_df = get_entity_df(output_path)
        return combined_df

def combined_updated_entities(entity_output_path: str, updated_entity_output_path: str, entity_column: str, query_time_column: str, overwrite_existing_temp_files: bool, return_df: bool, is_large: bool) -> Optional[pd.DataFrame]:
    """
    Combines updated entities and existing entities. Removes duplicates and checks for entities in older queries.

    :param entity_output_path: Path to entity file
    :param updated_entity_output_path: Path to updated entity file
    :param entity_column: Entity column
    :param query_time_column: Query time column
    :param overwrite_existing_temp_files: Boolean indicating whether to overwrite existing temporary files. Defaults to True.
    :param return_df: Boolean to return the DataFrame or not.
    :param is_large: Boolean indicating whether to handle large files differently. If True, only metadata is collected initially. Defaults to True.
    :return: Combined entity dataframe
    """
    # Combine updated entities and existing entities
    if (os.path.exists(entity_output_path)) and (os.path.exists(updated_entity_output_path)):
        # Read in files
        entity_df = read_csv_file(entity_output_path)
        # Check if updated entity file exists
        check_if_older_file_exists(entity_output_path)
        updated_entity_df = read_csv_file(updated_entity_output_path)
        existing_entities_df = entity_df[~entity_df[entity_column].isin(updated_entity_df[entity_column])]
        combined_entities_df = pd.concat([updated_entity_df, existing_entities_df])
        # Sort by query time and drop duplicates
        combined_entities_df = combined_entities_df[updated_entity_df.columns.tolist()]
        combined_entities_df = combined_entities_df.fillna(np.nan).replace([np.nan], [None])
        combined_entities_df = combined_entities_df.sort_values(by=[query_time_column]).drop_duplicates(subset=[entity_column], keep='first')
        cleaned_entities_df = check_for_entity_in_older_queries(entity_output_path, combined_entities_df, is_large)
        if overwrite_existing_temp_files:
            double_check = check_file_created(entity_output_path, cleaned_entities_df)
            if double_check:
                os.remove(updated_entity_output_path)
        if return_df:
            return combined_entities_df
        
def get_missing_entries(df: pd.DataFrame, older_df: pd.DataFrame, subset_fields: List) -> pd.DataFrame:
    """
    Checks for missing entries in a dataframe

    :param df: Current dataframe
    :param older_df: Older existing dataframe
    :param subset_fields: List of columns to subset
    :return: Missing values dataframe
    """
    # Check for missing entries in a dataframe
    merged_df = pd.merge(df[subset_fields], older_df[subset_fields], on=subset_fields, how='outer', indicator=True)
    # Subset missing values
    missing_values = merged_df[merged_df._merge == 'right_only']
    double_check = missing_values[subset_fields]
    combined_condition = np.ones(len(older_df), dtype=bool)
    for field in subset_fields:
        combined_condition = combined_condition & older_df[field].isin(double_check[field])
    older_df['double_check'] = np.where(combined_condition, 1, 0)
    final_missing_values = older_df[(older_df.double_check == 1) & (older_df[subset_fields[0]].isin(double_check[subset_fields[0]]))]
    if len(final_missing_values) > 0:
        final_missing_values = final_missing_values.drop(columns=['double_check'])
        return final_missing_values
    else:
        return pd.DataFrame()

def check_for_joins_in_older_queries(join_file_path: str, join_files_df: pd.DataFrame, join_unique_field: str, filter_fields: List, subset_terms: Optional[List]=[], is_large: bool=False) -> pd.DataFrame:
    """
    Checks if joins from GitHub API exist in older queries and add them to our most recent version of the file

    :param join_file_path: Path to join file
    :param join_files_df: Current join dataframe
    :param join_unique_field: Unique field to join on
    :param filter_fields: Fields to filter on
    :param subset_terms: Optional subset terms to filter on
    :param is_large: Boolean to check if file is large or not
    :return: Final joined dataframe
    """
    # Needs to check if older repos exist and then find their values in older join_files_df
    join_type = join_file_path.split("/")[-1].split("_dataset")[0]

    older_join_file_path = join_file_path.replace(f"{data_directory_path}/", f"{data_directory_path}/older_files/")
    older_join_file_dir = os.path.dirname(older_join_file_path) + "/"

    older_join_df = read_combine_files(dir_path=older_join_file_dir, check_all_dirs=True, file_path_contains=join_type, large_files=is_large) 
    
    # entity_type = "" if "search" in join_file_path else "repo" if "repo" in join_file_path else "user"
    entity_type = join_file_path.split("/")[-1].split("_")[0]

    if "comments" in join_file_path:
        entity_type = "repo"

    if "search" in join_file_path:
        entity_type = ""

    if "search" in join_file_path:
        join_files_df = join_files_df[join_files_df.search_term_source.isin(subset_terms)]
        older_join_df = older_join_df[older_join_df.search_term_source.isin(subset_terms)]

    if len(older_join_df) > 0:
        if join_unique_field in older_join_df.columns:
            older_join_df = older_join_df[older_join_df[join_unique_field].notna()]
            
            combined_join_df = pd.concat([join_files_df, older_join_df])
            time_field = 'search_query_time' if 'search_query' in join_unique_field else f'{entity_type}_query_time'
            cleaned_field = 'cleaned_search_query_time' if 'search_query' in join_unique_field else f'cleaned_{entity_type}_query_time'
            combined_join_df[cleaned_field] = None
            combined_join_df.loc[combined_join_df[time_field].isna(), cleaned_field] = '2022-10-10'
            combined_join_df[cleaned_field] = pd.to_datetime(combined_join_df[time_field], errors='coerce')
            combined_join_df = combined_join_df.sort_values(by=[cleaned_field]).drop_duplicates(subset=filter_fields, keep='first').drop(columns=[cleaned_field])

            missing_values = get_missing_entries(join_files_df, older_join_df, filter_fields)

            if len(missing_values) > 0:
                join_files_df = pd.concat([join_files_df, missing_values])
                # join_files_df.to_csv(join_file_path, index=False)

    return join_files_df

def get_core_users_repos(combine_files: bool =True) -> Union[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Gets core users, repos, and orgs.

    :param combine_files: Boolean indicating whether to combine files. Defaults to True.
    :return: Core users, repos, and orgs
    """
    initial_core_users = read_csv_file(f"{data_directory_path}/derived_files/initial_core_users.csv")
    initial_core_users['origin'] = 'initial_core'
    initial_core_repos = read_csv_file(f"{data_directory_path}/derived_files/initial_core_repos.csv")
    initial_core_repos['origin'] = 'initial_core'
    initial_core_orgs = read_csv_file(f"{data_directory_path}/derived_files/initial_core_orgs.csv")
    initial_core_orgs['origin'] = 'initial_core'

    firstpass_core_users = read_csv_file(f"{data_directory_path}/derived_files/firstpass_core_users.csv")
    firstpass_core_users['origin'] = 'firstpass_core'
    firstpass_core_repos = read_csv_file(f"{data_directory_path}/derived_files/firstpass_core_repos.csv")
    firstpass_core_repos['origin'] = 'firstpass_core'
    firstpass_core_orgs = read_csv_file(f"{data_directory_path}/derived_files/firstpass_core_orgs.csv")
    firstpass_core_orgs['origin'] = 'firstpass_core'

    finalpass_core_users = read_csv_file(f"{data_directory_path}/derived_files/finalpass_core_users.csv")
    finalpass_core_users['origin'] = 'finalpass_core'
    finalpass_core_repos = read_csv_file(f"{data_directory_path}/large_files/derived_files/finalpass_core_repos.csv", low_memory=False, on_bad_lines='skip')
    finalpass_core_repos['origin'] = 'finalpass_core'
    finalpass_core_orgs = read_csv_file(f"{data_directory_path}/derived_files/finalpass_core_orgs.csv")
    finalpass_core_orgs['origin'] = 'finalpass_core'

    if combine_files:
        core_users = pd.concat([initial_core_users, firstpass_core_users, finalpass_core_users])
        core_repos = pd.concat([initial_core_repos, firstpass_core_repos, finalpass_core_repos])
        core_orgs = pd.concat([initial_core_orgs, firstpass_core_orgs, finalpass_core_orgs])
        return core_users, core_repos, core_orgs
    else:
        return initial_core_users, initial_core_repos, initial_core_orgs, firstpass_core_users, firstpass_core_repos, firstpass_core_orgs, finalpass_core_users, finalpass_core_repos, finalpass_core_orgs


def save_chart(chart: alt.Chart, filename: str, scale_factor=2.0) -> None:
    '''
    Save an Altair chart using vl-convert
    
    :param chart: Altair chart to save
    :param filename : The path to save the chart to
    :param scale_factor: int or float
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