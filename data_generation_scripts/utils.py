# Standard library imports
import os
import re
import time
import ast
import warnings
from datetime import datetime, timedelta
from typing import List, Optional, Union

# Related third-party imports
import altair as alt
import apikey
import numpy as np
import pandas as pd
import requests
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

def set_data_directory_path(path: str) -> None:
    """
    Sets data directory path.

    :param path: Path to data directory
    """
    apikey.save("CODING_DH_DATA_DIRECTORY_PATH", path)
    console.print(f'Coding DH data directory path set to {path}', style='bold blue')

def get_data_directory_path() -> str:
    """
    Gets data directory path.

    :return: Data directory path
    """
    return apikey.load("CODING_DH_DATA_DIRECTORY_PATH")

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
        console.print(f'Failed to retrieve rate limit with status code: {response.status_code}. Error from check_rate_limit function', style='bold red')
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
        console.print("Status code", response.status_code)
        # Check if response is valid and return it if it is
        if response.status_code == 200:
            return response, response.status_code
        elif response.status_code == 401:
            console.print("Response status code 401: unauthorized access. Recommend checking api key. Error from make_request_with_rate_limiting function", style='bold red')
            return None, response.status_code
        elif response.status_code == 204:
            console.print(f'Response status code 204: No data for {url}.  Error from make_request_with_rate_limiting function', style='bold red')
            return None, response.status_code
        # If not, check if it's a rate limit issue
        if index == 0:
            console.print('Hitting rate limiting set by number of attempts. Sleeping for 120 seconds to ensure that queries are not blocked by API. Message from make_request_with_rate_limiting function', style='bold red')
            time.sleep(120)
        # If it is, wait for an hour and try again
        if index == 1:
            rates_df = check_rate_limit()
            if rates_df['resources.core.remaining'].values[0] == 0:
                # Get the current time
                now = datetime.now()
                # Calculate when the function should run again
                run_again_at = now + timedelta(hours=1)
                console.print(f'GitHub 5000 query rate limit reached at {now.strftime("%Y-%m-%d %H:%M:%S")}. Sleeping for 1 hour and then restarting at {run_again_at.strftime("%Y-%m-%d %H:%M:%S")}. Message from make_request_with_rate_limiting function', style='bold red')
                time.sleep(3600)
    # If it's not a rate limit issue, return None
    console.print(f'Query failed after {number_of_attempts} attempts with code {response.status_code}. Failing URL: {url}. Error from make_request_with_rate_limiting function', style='bold red')
    return None, response.status_code

def check_total_pages(url: str, auth_headers: dict) -> int:
    """
    Checks total number of pages for a given url on the GitHub API.

    :param url: URL to check
    :param auth_headers: Authentication headers
    :return: Total number of pages. If there are no links or response is None, returns 1.
    """
    
    finalized_url = f'{url}&per_page=1' if "?state=all" in url else f'{url}?per_page=1'

    # Get total number of pages
    response, _ = make_request_with_rate_limiting(finalized_url, auth_headers)
    # If response is None or there are no links, return 1
    if response is None or len(response.links) == 0:
        return 0
    # Otherwise, get the last page number
    match = re.search(r'\d+$', response.links['last']['url'])
    return int(match.group()) if match is not None else 0

def check_total_results(url: str, auth_headers: dict) -> Optional[int]:
    """
    Checks total number of results for a given url on the GitHub API.
    
    :param url: URL to check
    :param auth_headers: Authentication headers
    :return: Total number of results. If response is None, returns None.
    """
    # Get total number of results
    response, _ = make_request_with_rate_limiting(url, auth_headers)
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
    
def read_combine_files(dir_path: str, files: Optional[List] = None, file_path: Optional[str] = None, grouped_columns: Optional[List] = [] , return_all: bool = False) -> pd.DataFrame:
    """
    Reads all CSV files in a directory, combines them into a single DataFrame, and writes the result to a file. Items are organized by most recent coding_dh_date.
    If return_all is False, only the most recent entry for each group (defined by grouped_columns) is kept.
    
    Parameters:
    dir_path (str): The path to the directory containing the CSV files.
    file_path (str): The path to the file where the combined DataFrame will be written.
    grouped_columns (list, optional): The columns to group by when return_all is False. Defaults to None.
    return_all (bool, optional): Whether to return all rows or only the most recent for each group. Defaults to False.
    
    Returns:
    pd.DataFrame: The combined DataFrame.
    """
    
    # Remove the output file if it already exists
    if files is None:
        files = os.listdir(dir_path)
    
    dfs = []
    for file in tqdm(files, desc=f"Reading files in {dir_path}"):
        # Skip .DS_Store files
        if '.DS_Store' in file:
            os.remove(os.path.join(dir_path, file))
            continue
        df = read_csv_file(os.path.join(dir_path, file))
        
        if df is not None:
            try:
                if not return_all:
                    df['coding_dh_date'] = pd.to_datetime(df['coding_dh_date'], errors='coerce')
                    df = df.sort_values(by="coding_dh_date", ascending=False)
                    # get the latest date
                    if ('entity' not in dir_path) and (len(grouped_columns) > 0):
                        df = df.groupby(grouped_columns).first().reset_index()
                    else:
                        df = df[0:1]
                    
                if file_path is not None and os.path.exists(file_path):
                    # Write DataFrame to file
                    mode = "a" if os.path.exists(file_path) else "w"
                    df.to_csv(file_path, mode=mode, header=(mode=="w"), index=False)
                else:
                    dfs.append(df)
            except:
                console.print(f"Error with file {file}", style="bold red")
                continue
    
    if file_path is not None and os.path.exists(file_path):
        combined_df = read_csv_file(file_path)
    else:
        combined_df = pd.concat(dfs)
    return combined_df

def get_headers(entity_type: str) -> pd.DataFrame:
    """
    Gets headers for entity type.

    :param entity_type: Type of entity
    :return: Headers dataframe
    """
    data_directory_path = get_data_directory_path()
    # Get headers for entity type
    headers_file_path = os.path.join(data_directory_path, 'metadata_files', f'{entity_type[:-1]}_headers.csv')
    if entity_type == 'users':
        headers = read_csv_file(headers_file_path)
    elif entity_type == 'repos':
        headers = read_csv_file(headers_file_path)
    elif entity_type == 'orgs':
        headers = read_csv_file(headers_file_path)
    else:
        console.print(f'No headers for {entity_type}', style='bold red')
        return None
    return headers

def sort_groups_add_coding_dh_id(group: pd.DataFrame, subset_columns: List[str]) -> pd.DataFrame:
    """
    Sorts a DataFrame group based on 'coding_dh_date' and adds a new column 'coding_dh_id' with unique identifiers.
    If the group has more than one unique row (excluding subset_columns), each row gets a unique identifier.
    If the group has only one unique row (excluding subset_columns), it gets the identifier 0.

    Parameters:
    group (pd.DataFrame): DataFrame group to sort and add identifiers to.
    subset_columns (List[str]): List of column names to exclude when checking for unique rows.

    Returns:
    pd.DataFrame: The sorted DataFrame group with the new 'coding_dh_id' column.
    """
    # Convert strings that represent lists into actual lists
    for col in group.columns:
        group[col] = group[col].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.startswith('[') and x.endswith(']') else x)

    # Handle list columns and convert them to sorted comma-separated strings
    list_cols = [col for col in group.columns if group[col].apply(lambda x: isinstance(x, list)).any()]
    for col in list_cols:
        group[col] = group[col].apply(lambda x: ', '.join(sorted(map(str, x))) if isinstance(x, list) else x)

    subset_columns = list(set(subset_columns + list_cols))

    # Sort the DataFrame by 'coding_dh_date' in ascending order
    sorted_group = group.sort_values(by='coding_dh_date')

    # Drop duplicates across all columns, excluding subset_columns
    final_group = sorted_group.drop_duplicates(subset=sorted_group.columns.difference(subset_columns), keep='first')

    # Assign unique identifiers
    final_group['coding_dh_id'] = np.arange(len(final_group))
    final_group = final_group.drop(columns=list_cols)

    return final_group

def check_headers_exist(df: pd.DataFrame, headers: pd.DataFrame) -> pd.DataFrame:
    """
    Checks if headers exist in dataframe and adds them if they don't.

    :param df: Dataframe
    :param headers: Headers dataframe
    :return: Dataframe with headers
    """
    # Check if each column in headers exists in the response data
    for column in headers.columns:
        if column not in df.columns:
            df[column] = None  # If not, assign None
    return df

def drop_columns_from_df(df: pd.DataFrame, columns: List) -> pd.DataFrame:
    """
    Drops columns from dataframe.

    :param df: Dataframe
    :param columns: Columns to drop
    :return: Dataframe with dropped columns
    """
    for col in columns:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df

def clean_write_error_file(error_file_path: str, drop_fields: List) -> None:
    """
    Cleans error file and writes it. Drops duplicates if error_time column exists. Also drops duplicates based on drop_field column.

    :param error_file_path: Path to error file
    :param drop_fields: Fields to drop
    """
    # Clean error file and write it
    if os.path.exists(error_file_path):
        error_df = read_csv_file(error_file_path)
        # Drop duplicates if error_date exists
        if 'error_date' in error_df.columns:
            error_df = error_df.sort_values(by=['error_date']).drop_duplicates(subset=drop_fields, keep='last')
        else: 
            error_df = error_df.drop_duplicates(subset=drop_fields, keep='last')
        error_df.to_csv(error_file_path, index=False)
    else:
        console.print('No error file to clean', style='bold blue')

def log_error_to_file(error_file_path: str, additional_data: dict, status_code: int, error_url: str) -> None:
    error_df = pd.DataFrame([{"error_date": datetime.now().strftime("%Y-%m-%d"), "error_url": error_url, "status_code": status_code}])
    error_df = pd.concat([error_df, pd.DataFrame([additional_data])], axis=1)
    if os.path.exists(error_file_path):
        error_df.to_csv(error_file_path, mode='a', header=False, index=False)
    else:
        error_df.to_csv(error_file_path, index=False)

def get_new_entities(entity_type:str, potential_new_entities_df: pd.DataFrame, temp_entity_dir: str, entity_progress_bar: tqdm, error_file_path: str, write_only_new: bool, retry_errors: bool = False):
    """
    Gets new entities from GitHub API. 

    :param entity_type: Type of entity
    :param potential_new_entities_df: Potential new entities dataframe
    :param temp_entity_dir: Temporary entity directory
    :param entity_progress_bar: Entity progress bar
    :param error_file_path: Path to error file
    :param write_only_new: Boolean indicating whether to write only new entities
    :param retry_errors: Boolean indicating whether to retry errors
    """
    data_directory_path = get_data_directory_path()
    # Create temporary directory if it doesn't exist
    if not os.path.exists(temp_entity_dir):
        os.makedirs(temp_entity_dir, exist_ok=True)
    
    # Subset headers for orgs and users
    user_cols = ["bio", "followers_url", "following_url", "gists_url", "gravatar_id", "hireable", "organizations_url","received_events_url", "site_admin", "starred_url",
    "subscriptions_url","login",]
    repo_exclude_headers = ['squash_merge_commit_message', 'security_and_analysis.dependabot_security_updates.status', 'allow_squash_merge','merge_commit_title', 'allow_rebase_merge', 'allow_auto_merge', 'merge_commit_message', 'delete_branch_on_merge','use_squash_pr_title_as_default', 'allow_merge_commit','squash_merge_commit_title', 'security_and_analysis.secret_scanning_validity_checks.status', 'security_and_analysis.secret_scanning_push_protection.status', 'security_and_analysis.secret_scanning.status', 'allow_update_branch']
    org_exclude_headers = ['two_factor_requirement_enabled',
    'advanced_security_enabled_for_new_repositories',
    'members_can_create_pages',
    'members_can_create_public_pages',
    'secret_scanning_push_protection_custom_link',
    'total_private_repos',
    'secret_scanning_validity_checks_enabled',
    'billing_email',
    'members_can_create_repositories',
    'dependency_graph_enabled_for_new_repositories',
    'plan.private_repos',
    'dependabot_alerts_enabled_for_new_repositories',
    'owned_private_repos',
    'members_can_create_private_pages',
    'private_gists',
    'collaborators',
    'plan.space',
    'members_can_create_private_repositories',
    'web_commit_signoff_required',
    'secret_scanning_push_protection_enabled_for_new_repositories',
    'default_repository_permission',
    'members_can_create_internal_repositories',
    'secret_scanning_enabled_for_new_repositories',
    'members_allowed_repository_creation_type',
    'plan.name',
    'members_can_create_public_repositories',
    'disk_usage',
    'secret_scanning_push_protection_custom_link_enabled',
    'dependabot_security_updates_enabled_for_new_repositories',
    'plan.filled_seats',
    'plan.seats',
    'members_can_fork_private_repositories', 'org_query_time']
    user_exclude_headers = ['disk_usage',
    'private_gists',
    'total_private_repos',
    'collaborators',
    'plan.space',
    'plan.collaborators',
    'plan.private_repos',
    'owned_private_repos',
    'plan.name',
    'two_factor_authentication']

    excluded_file_path = os.path.join(data_directory_path, 'metadata_files', f'excluded_{entity_type}.csv')
    
    # Get entity column based on entity type
    entity_column = "full_name" if entity_type == "repos" else "login"
    entity_type_singular = entity_type[:-1]

    if os.path.exists(error_file_path):
        drop_fields = [entity_column, 'error_url']
        clean_write_error_file(error_file_path, drop_fields)
        if retry_errors == False:
            error_df = read_csv_file(error_file_path)
            potential_new_entities_df = potential_new_entities_df[~potential_new_entities_df[entity_column].isin(error_df[entity_column])]
            if potential_new_entities_df.empty:
                console.print(f"No new entities to process for {entity_type}", style="bold blue")
                return

    if os.path.exists(excluded_file_path):
        excluded_entities = read_csv_file(excluded_file_path)
        # Exclude entities and check for errors
        potential_new_entities_df = potential_new_entities_df[~potential_new_entities_df[entity_column].isin(excluded_entities[entity_column])]

    # Get headers
    headers = get_headers(entity_type)

    # Update progress bar
    entity_progress_bar.total = len(potential_new_entities_df)
    entity_progress_bar.refresh()
    columns_to_drop = ['org_query_time', 'user_query_time', 'repo_query_time', 'search_query_time', 'coding_dh_id']

    # Loop through potential new entities
    for _, row in potential_new_entities_df.iterrows():
        try:
            # Create temporary file path
            temp_entities_file_name = f"{row[entity_column].replace('/', '_').replace(' ', '_')}_coding_dh_{entity_type_singular}.csv"
            console.print(temp_entities_file_name)
            # Check if file exists
            temp_file_path = os.path.join(temp_entity_dir, temp_entities_file_name)
            if os.path.exists(temp_file_path):
                existing_temp_entities_df = read_csv_file(temp_file_path)
                existing_temp_entities_df = drop_columns_from_df(existing_temp_entities_df, columns_to_drop)
                if write_only_new:
                    entity_progress_bar.update(1)
                    continue
            else:
                existing_temp_entities_df = pd.DataFrame()
            # Get query
            query = row.url
            if entity_type == "orgs":
                query = row.url if "/users/" in row.url else row.url.replace("/orgs/", "/users/")
            # Make request
            response, status_code = make_request_with_rate_limiting(query, auth_headers)
            # If response is None, update progress bar and continue
            if response is None and entity_type != "orgs":
                entity_progress_bar.update(1)
                additional_data = {entity_column: row[entity_column]}
                log_error_to_file(error_file_path, additional_data, status_code, query)
                continue
            # If response is None and entity type is orgs, create empty dataframe
            elif response is None and entity_type == "orgs":
                response_df = pd.DataFrame(columns=headers.columns, data=None, index=None)
            else:
                response_data = response.json()
                response_df = pd.json_normalize(response_data)
                if "message" in response_df.columns:
                    console.print(f"Error for {row[entity_column]}: {response_df.message.values[0]}", style="bold red")
                    additional_data = {entity_column: row[entity_column]}
                    log_error_to_file(error_file_path, additional_data, status_code, query)
                    entity_progress_bar.update(1)
                    continue
            
            if entity_type != "orgs":
                final_df = check_headers_exist(response_df, headers)
                final_df = final_df[headers.columns]
            else:
                response_df = response_df[user_cols]
                query = row.url.replace("/users/", "/orgs/") if "/users/" in row.url else row.url
                response, _ = make_request_with_rate_limiting(query, auth_headers)
                if response is None:
                    expanded_df = pd.DataFrame(columns=headers.columns, data=None, index=None)
                else:
                    response_data = response.json()
                    expanded_df = pd.json_normalize(response_data)
                    expanded_df = check_headers_exist(expanded_df, headers)            
                    expanded_df = expanded_df[headers.columns]
             
                common_columns = list(set(response_df.columns).intersection(set(expanded_df.columns)))
                final_df = pd.merge(response_df, expanded_df, on=common_columns, how='left')
                
            final_df["coding_dh_date"] = datetime.now().strftime("%Y-%m-%d")
            combined_df = pd.concat([existing_temp_entities_df, final_df])
            grouped_dfs = combined_df.groupby(entity_column)
            processed_files = []
            for _, group in tqdm(grouped_dfs, desc=f"Grouping files"):
                subset_columns = ["coding_dh_date"]
                group = sort_groups_add_coding_dh_id(group, subset_columns)
                processed_files.append(group)
            final_processed_df = pd.concat(processed_files).reset_index(drop=True)
            console.print("Length final_df", len(final_processed_df))
            if entity_type == "repos":
                final_processed_df = drop_columns_from_df(final_processed_df, repo_exclude_headers)
            elif entity_type == "orgs":
                final_processed_df = drop_columns_from_df(final_processed_df, org_exclude_headers)
            else:
                final_processed_df = drop_columns_from_df(final_processed_df, user_exclude_headers)

            final_processed_df.to_csv(temp_file_path, index=False)
            entity_progress_bar.update(1)
        except Exception as e:
            console.print(f"Error for {row[entity_column]}: {e}", style="bold red")
            additional_data = {entity_column: row[entity_column]}
            log_error_to_file(error_file_path, additional_data, status_code, query)
            entity_progress_bar.update(1)
            continue

    # Read in all temporary files
    # combined_entity_df = read_combine_files(temp_entity_dir)
    entity_progress_bar.close()
    # return combined_entity_df

def create_queries_directories(entity_type: str, cleaned_terms: pd.DataFrame) -> pd.DataFrame:
    """
    Function to create directories for the queries

    :param entity_type: Type of entity
    :param cleaned_terms: Cleaned terms dataframe
    :return: Search Queries dataframe
    """
    data_directory_path = get_data_directory_path()
    queries = []
    for _, subdir, _ in tqdm(os.walk(data_directory_path + f"/searched_{entity_type}_data"), desc="Walking through directories"):
        for directory in subdir:
            for file in os.listdir(data_directory_path + f"/searched_{entity_type}_data/" + directory):
                if file.endswith(".csv"):
                    search_term_source = directory.replace("_", " ").title()
                    if 'searched' in file:
                        search_term = file.replace(".csv", "").split(f'{entity_type}s_searched_')[1].replace("+", " ").replace("&#39;", "'")
                        if '20' in search_term:
                            search_term = search_term.split("_20")[0]
                    else:
                        search_term = search_term_source
                    
                    subset_cleaned_terms = cleaned_terms[(cleaned_terms.search_term_source == search_term_source) & (cleaned_terms.search_term == search_term)]
                    if not subset_cleaned_terms.empty:
                        subset_cleaned_terms['file_path'] = f"{data_directory_path}/searched_{entity_type}_data/{directory}/{file}"
                        subset_cleaned_terms["file_name"] = file
                        queries.append(subset_cleaned_terms)
    queries_df = pd.concat(queries)
    queries_df = queries_df.reset_index(drop=True)
    search_queries_dfs = []
    for _, row in tqdm(queries_df.iterrows(), total=queries_df.shape[0], desc="Processing queries"):
        df = read_csv_file(row.file_path, encoding='utf-8-sig')
        df["search_file_name"] = row.file_name
        search_queries_dfs.append(df)
    search_queries_df = pd.concat(search_queries_dfs)
    return search_queries_df

def get_entity_files_from_search_queries(search_user_queries_df, search_org_queries_df, search_repo_queries_df, data_directory_path: str):
    user_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_users/")
    org_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_orgs/")
    repo_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_repos/")
    cleaned_user_files = [f.split("_coding_dh_")[0] for f in user_files if f.endswith(".csv")]
    cleaned_org_files = [f.split("_coding_dh_")[0] for f in org_files if f.endswith(".csv")]
    cleaned_repo_files = [f.split("_coding_dh_")[0].replace("_", "/", 1) for f in repo_files if f.endswith(".csv")]
    existing_search_user_queries_df = search_user_queries_df[search_user_queries_df.login.isin(cleaned_user_files)]
    existing_search_org_queries_df = search_org_queries_df[search_org_queries_df.login.isin(cleaned_org_files)]
    existing_search_repo_queries_df = search_repo_queries_df[search_repo_queries_df.full_name.isin(cleaned_repo_files)]
    finalized_user_logins = existing_search_user_queries_df.login.unique().tolist()
    finalized_org_logins = existing_search_org_queries_df.login.unique().tolist()
    finalized_repo_full_names = existing_search_repo_queries_df.full_name.unique().tolist()

    finalized_user_files = [f"{login}_coding_dh_user.csv" for login in finalized_user_logins]
    finalized_org_files = [f"{login}_coding_dh_org.csv" for login in finalized_org_logins]
    finalized_repo_files = [f"{full_name.replace('/', '_')}_coding_dh_repo.csv" for full_name in finalized_repo_full_names]
    initial_core_users = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_users/", finalized_user_files)
    initial_core_orgs = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_orgs/", finalized_org_files)
    initial_core_repos = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_repos/", finalized_repo_files)
    return initial_core_users, initial_core_orgs, initial_core_repos

def get_data_from_search_terms(target_terms: List, data_directory_path: str, return_search_queries: bool) -> Union[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Load in the translated terms
    cleaned_terms = pd.read_csv(f'{data_directory_path}/derived_files/grouped_cleaned_translated_terms.csv', encoding='utf-8-sig')

    if 'keep_term' in cleaned_terms.columns:
        cleaned_terms = cleaned_terms[cleaned_terms.keep_term == True]
    # check if columns need renaming
    columns_to_rename = ['code', 'term', 'term_source']
    if all(elem in cleaned_terms.columns for elem in columns_to_rename):
        cleaned_terms = cleaned_terms.rename(columns={'code': 'natural_language', 'term': 'search_term', 'term_source': 'search_term_source'})
    cleaned_terms = cleaned_terms[cleaned_terms.search_term_source.isin(target_terms)]
    cleaned_terms = cleaned_terms.reset_index(drop=True)

    cleaned_terms.loc[cleaned_terms.search_term.str.contains("&#39;"), "search_term"] = cleaned_terms.search_term.str.replace("&#39;", "'")
    cleaned_terms['lower_search_term'] = cleaned_terms.search_term.str.lower()

    search_user_queries_df = create_queries_directories("user", cleaned_terms)
    search_org_queries_df = search_user_queries_df[search_user_queries_df['type'] == 'Organization']
    search_org_queries_df = search_org_queries_df[search_org_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]
    search_user_queries_df = search_user_queries_df[search_user_queries_df['type'] == 'User']
    search_user_queries_df = search_user_queries_df[search_user_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]
    search_repo_queries_df = create_queries_directories("repo", cleaned_terms)
    search_repo_queries_df = search_repo_queries_df[search_repo_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]

    if return_search_queries:
        return search_user_queries_df, search_org_queries_df, search_repo_queries_df
    else:
        initial_core_users, initial_core_orgs, initial_core_repos = get_entity_files_from_search_queries(search_user_queries_df, search_org_queries_df, search_repo_queries_df, data_directory_path)
        return initial_core_users, initial_core_orgs, initial_core_repos



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
        
if __name__ == "__main__":
    data_directory_path = "../../new_datasets"
    set_data_directory_path(data_directory_path)