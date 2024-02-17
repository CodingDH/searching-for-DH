# Standard library imports
import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional
import warnings
warnings.filterwarnings("ignore")

# Related third-party imports
import pandas as pd
import requests
import arabic_reshaper
from bidi.algorithm import get_display
from rich.console import Console
from tqdm import tqdm

# Local application/library specific imports
import apikey
sys.path.append("..")
from data_generation_scripts.general_utils import  read_csv_file, check_total_pages, check_total_results, check_rate_limit, make_request_with_rate_limiting, sort_groups_add_coding_dh_id, get_data_directory_path, clean_write_error_file, log_error_to_file

# Load in the API key
auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

# Set the headers
auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

# Initiate the console
console = Console()

def fetch_data(query: str, data_directory_path: str, search_term: str, search_term_source: str) -> Tuple[pd.DataFrame, requests.Response]:
    """
    Fetches data from the search API using the provided query. This function returns both the 
    processed data as a DataFrame and the raw response object from the API request.

    :param query: String specifying the query to be passed to the search API. It should be formatted according to the API's requirements.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    :return: A tuple containing two elements:
        1. DataFrame: The processed data retrieved from the API, structured for analysis.
        2. Response: The raw response object from the requests library, providing access to response headers, status code, and other metadata.
    """
    # Initiate the request
    response, status_code = make_request_with_rate_limiting(query, auth_headers)
    # Check if response is None
    if response is None:
        console.print(f"Failed to fetch data for query: {query}. Error from fetch_data function.", style="bold red")
        additional_data = {"search_term": search_term, "search_term_source": search_term_source}
        log_error_to_file(f"{data_directory_path}/error_logs/search_errors.csv", additional_data, status_code, query)
        return pd.DataFrame(), None
    
    response_data = response.json()

    response_df = pd.json_normalize(response_data["items"])
    # If there is data returned, add the search query to the dataframe
    if len(response_df) > 0:
        response_df["search_query"] = query
    else:
        # If there is no data returned, load in the headers from the metadata files
        if "repo" in query:
            response_df = read_csv_file(f"{data_directory_path}/metadata_files/search_repo_headers.csv")
            response_df["search_query"] = query
        else:
            response_df = read_csv_file(f"{data_directory_path}/metadata_files/search_user_headers.csv")
            response_df["search_query"] = query
    return response_df, response

def get_search_api_data(query: str, total_pages: int, data_directory_path: str, search_term: str, search_term_source: str) -> pd.DataFrame:
    """
    Retrieves data from the search API based on the specified query across a defined number of pages. This function consolidates the data from all pages into a single DataFrame.

    :param query: String representing the query to be passed to the search API. This should conform to the API's query format and include any necessary parameters.
    :param total_pages: Integer specifying the total number of pages of data to be queried from the API. It determines how many API requests will be made.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    :return: DataFrame containing aggregated data from all queried pages of the API. 
    """
    # Initiate an empty list to store the dataframes
    dfs = []
    pbar = tqdm(total=total_pages, desc="Getting Search API Data")
    try:
        # Get the data from the API
        time.sleep(0.01)
        df, response = fetch_data(query, data_directory_path, search_term, search_term_source)
        dfs.append(df)
        pbar.update(1)
        # Loop through the pages. A suggestion we gathered from https://stackoverflow.com/questions/33878019/how-to-get-data-from-all-pages-in-github-api-with-python
        while "next" in response.links.keys():
            time.sleep(120)
            query = response.links["next"]["url"]
            df, response = fetch_data(query, data_directory_path)
            dfs.append(df)
            pbar.update(1)
    except:  # pylint: disable=W0702
        console.print(f"Error with URL: {query}. Error from get_search_api_data function.", style="bold red")

    pbar.close()
    # Concatenate the dataframes
    search_df = pd.concat(dfs)
    return search_df

def encode_decode(x: str) -> str:
    """
    Encodes and decodes a string to handle encoding issues.

    :param x: String to be encoded and decoded.
    :return: The encoded and decoded string.
    """
    try:
        return x.encode("latin1").decode("utf-8")
    except:
        return x

def process_search_data(rates_df: pd.DataFrame, query: str, output_path: str, row_data: Dict[str, Any], data_directory_path: str) -> pd.DataFrame:
    """
    Processes data obtained from the search API. It uses the specified query to fetch data, adhering to the given rate limits, and then processes this data according to the row data from the search terms CSV.

    :param rates_df: DataFrame containing the current rate limit information.
    :param query: Query string to be passed to the search API.
    :param output_path: Path to the file where processed data will be saved.
    :param row_data: Dictionary representing a row of data from the search terms CSV.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    :return: DataFrame containing the processed data from the API.
    """
    console.print(f"Processing data for this finalized query: ", style="purple")
    console.print(query, style=f"link {query}")
    total_pages = int(check_total_pages(query, auth_headers=auth_headers))
    total_pages = 1 if total_pages == 0 else total_pages
    console.print(f"Total pages: {total_pages}", style="green")
    calls_remaining = rates_df["resources.search.remaining"].values[0]
    while total_pages > calls_remaining:
        time.sleep(3700)
        updated_rates_df = check_rate_limit()
        calls_remaining = updated_rates_df["resources.search.remaining"].values[0]
    searched_df = get_search_api_data(query, total_pages, data_directory_path, row_data["search_term"], row_data["search_term_source"])
    searched_df = searched_df.reset_index(drop=True)
    searched_df["search_term"] = row_data["search_term"]
    searched_df["search_term_source"] = row_data["search_term_source"]
    searched_df["natural_language"] = row_data["natural_language"]
    searched_df["search_type"] = "tagged" if "topic" in query else "searched"
    searched_df["cleaned_search_query"] = searched_df.search_query.str.replace("%22", '"').str.replace('"', "").str.replace("%3A", ":").str.split("&page").str[0]
    searched_df["coding_dh_date"] = datetime.now().strftime("%Y-%m-%d")
    if os.path.exists(output_path):
        # If it does load it in
        # previous encoding="ISO-8859-1"
        existing_searched_df = read_csv_file(output_path, error_bad_lines=False)
        encode_columns = ["cleaned_search_query", "search_term", "description"] if "repositories" in query else ["cleaned_search_query", "search_term"]
        existing_searched_df_columns = existing_searched_df.columns
        encode_columns = [col for col in encode_columns if col in existing_searched_df_columns]
        existing_searched_df[encode_columns] = existing_searched_df[encode_columns].applymap(encode_decode)
    else:
        # If it doesn't exist, create an empty dataframe
        existing_searched_df = pd.DataFrame()
 
    combined_dfs = pd.concat([existing_searched_df, searched_df])
    if "coding_dh_id" in combined_dfs.columns:
        combined_dfs = combined_dfs.drop(columns="coding_dh_id")
    
    combined_dfs["split_natural_language"] = combined_dfs["natural_language"].str.split(",")
    # Strip spaces and reorder alphabetically the natural language
    combined_dfs["split_natural_language"] = combined_dfs["split_natural_language"].apply(lambda x: sorted(i.strip() for i in x))
    combined_dfs["natural_language"] = combined_dfs["split_natural_language"].apply(lambda x: ", ".join(x))
    combined_dfs = combined_dfs.drop(columns="split_natural_language")
    grouped_column = "full_name" if "repositories" in query else "login"
    if combined_dfs[grouped_column].isnull().all():
        console.print(f"All values in {grouped_column} are None. Skipping concatenation.")
    else:
        grouped_dfs = combined_dfs.groupby(grouped_column)
        processed_files = []
        for _, group in tqdm(grouped_dfs, desc=f"Grouping files"):
            subset_columns = ["coding_dh_date", "search_query"]
            group = sort_groups_add_coding_dh_id(group, subset_columns)
            processed_files.append(group)

        final_searched_df = pd.concat(processed_files).reset_index(drop=True)
        dir_name = os.path.dirname(output_path)

        if not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)

        final_searched_df.to_csv(output_path, index=False)


def process_large_search_data(rates_df: pd.DataFrame, search_url: str, dh_term: str, params: str, initial_output_path: str, row_data: Dict[str, Any], data_directory_path: str) -> Optional[pd.DataFrame]:
    """
    Processes large datasets from the search API, specifically designed for queries expected to return over 1000 results. It constructs the query using the provided parameters and processes the resulting data. An example query looks like: https://api.github.com/search/repositories?q=%22Digital+Humanities%22+created%3A2017-01-01..2017-12-31+sort:updated

    :param rates_df: DataFrame containing the current rate limit information.
    :param search_url: String representing the base URL for the search API.
    :param dh_term: String indicating the term to be searched within the API.
    :param params: String detailing additional parameters to be passed to the search API. These parameters should be formatted as a query string.
    :param initial_output_path: String specifying the file path where the output data will be stored.
    :param row_data: Dictionary representing a single row from the search terms CSV, used for further processing.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    :return: Optionally returns a DataFrame containing the processed data from the API. Returns None if there are no results or in case of an error.
    """
    # Set the first year to be searched
    first_year = 2008
    current_year = datetime.now().year
    current_day = datetime.now().day
    current_month = datetime.now().month
    # Get the years to be searched
    years = list(range(first_year, current_year+1))
    for year in years:
        # Set the output path for the year
        yearly_output_path = initial_output_path + f"_{year}.csv"
        # Handle the case where the year is the current year
        if year == current_year:
            query = search_url + \
                f'"{dh_term}"+created%3A{year}-01-01..{year}-{current_month}-{current_day}+sort:created{params}'
        else:
            query = search_url + \
                f'"{dh_term}"+created%3A{year}-01-01..{year}-12-31+sort:created{params}'
        # Get the data from the API
        process_search_data(rates_df, query, yearly_output_path, row_data, data_directory_path)   

def prepare_terms_and_directories(translated_terms_output_path: str, threshold_file_path: str, target_terms: List) -> Tuple[pd.DataFrame, int]:
    """
    Prepares the terms and directories necessary for use with the search API. This function processes 
    translated terms, storing them in a specified output path, and reads threshold values from a given 
    file if the code has previously errorer out, setting up the environment for subsequent API searches.

    :param translated_terms_output_path: String specifying the path to the file where translated terms are stored. 
    :param threshold_file_path: String specifying the path to the file containing threshold values.
    :param target_terms: List of terms to be searched in the API. Used in the `generate_translations.py` file. 
    :return: A tuple containing:
        1. DataFrame: Contains the processed and translated terms ready for API search.
        2. Integer: A threshold value read from the threshold file, used for filtering or other purposes in the API search.
    """
    # Load in the translated terms
    cleaned_terms = read_csv_file(translated_terms_output_path, encoding='utf-8-sig')

    if 'keep_term' in cleaned_terms.columns:
        cleaned_terms = cleaned_terms[cleaned_terms.keep_term == True]
    # check if columns need renaming
    columns_to_rename = ['code', 'term', 'term_source']
    if all(elem in cleaned_terms.columns for elem in columns_to_rename):
        cleaned_terms = cleaned_terms.rename(columns={'code': 'natural_language', 'term': 'search_term', 'term_source': 'search_term_source'})
    cleaned_terms = cleaned_terms[cleaned_terms.search_term_source.isin(target_terms)]
    cleaned_terms = cleaned_terms.reset_index(drop=True)
    # Subset the terms to only those that are RTL and update them to be displayed correctly
    rtl = cleaned_terms[cleaned_terms.directionality == 'rtl']
    rtl['search_term'] = rtl['search_term'].apply(lambda x: arabic_reshaper.reshape(x))
    # Concatenate the RTL and LTR terms
    ltr = cleaned_terms[cleaned_terms.directionality == 'ltr']
    final_terms = pd.concat([ltr, rtl])
    final_terms.loc[final_terms.search_term.str.contains("&#39;"), "search_term"] = final_terms.search_term.str.replace("&#39;", "'")
    # Subset to just the terms that are in the threshold file
    if os.path.exists(threshold_file_path):
        threshold = read_csv_file(threshold_file_path)
        final_terms = final_terms[(final_terms.index > threshold.row_index.values[0])]
    else:
        final_terms = final_terms
    # Return the final terms
    return final_terms

def search_for_topics(row: pd.Series, rates_df: pd.DataFrame, initial_repo_output_path: str, search_query: str, source_type: str, data_directory_path: str):
    """
    Searches for topics in the search API based on given parameters.

    :param row: The row of data from the search terms CSV.
    :param rates_df: The dataframe containing the rate limit data.
    :param initial_repo_output_path: Path to the initial repository output file.
    :param search_query: The query string to be passed to the search API.
    :param source_type: The type of the source for the search term.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    """

    # search_query = search_query if row.search_term_source == "Digital Humanities" else '"' + search_query + '"' 
    search_topics_query = f'https://api.github.com/search/topics?q="{search_query}"'
    time.sleep(5)
    console.print(f"Searching for topics with this query: ", style="purple")
    console.print(search_topics_query, style=f"link {search_topics_query}")
    # Initiate the request
    response = make_request_with_rate_limiting(search_topics_query, auth_headers, timeout=5)
    
    # Check if response is None
    if response is None:
        console.print(f'Failed to fetch data for query: {search_topics_query}. Error from search_for_topics function.', style='bold red')
        return None
    data = response.json()
    # If term exists as a topic proceed
    if data['total_count'] > 0:
        # Term may result in multiple topics so loop through them
        for item in data['items']:
            if row.search_term == 'Public History':
                if item['name'] == 'solana':
                    continue
            # Topics are joined by hyphens rather than plus signs in queries
            tagged_query = item['name'].replace(' ', '-')
            repos_tagged_query = f'https://api.github.com/search/repositories?q=topic:"{tagged_query}"&per_page=100&page=1'
            # Check how many results
            total_tagged_results = check_total_results(repos_tagged_query, auth_headers=auth_headers)
            #If results exist then proceed
            if total_tagged_results > 0:

                output_term = item['name'].replace(' ','_')
                # If more than 1000 results, need to reformulate the queries by year since Github only returns max 1000 results
                if total_tagged_results > 1000:
                    search_url = "https://api.github.com/search/repositories?q=topic:"
                    params = "&per_page=100&page=1"
                    initial_tagged_output_path = initial_repo_output_path + \
                        f'{source_type}/' + f'repos_tagged_{output_term}'
                    process_large_search_data(rates_df, search_url, tagged_query, params, initial_tagged_output_path, row, data_directory_path)
                else:
                    # If fewer than a 1000 proceed to normal search calls
                    final_tagged_output_path = initial_repo_output_path + f'{source_type}/' + f'repos_tagged_{output_term}.csv'
                    process_search_data(rates_df, repos_tagged_query, final_tagged_output_path, row, data_directory_path)

def search_for_repos(row: pd.Series, search_query: str, rates_df: pd.DataFrame, initial_repo_output_path: str, source_type: str, data_directory_path: str):
    """
    Searches for repositories in the search API based on given parameters.

    :param row: The row of data from the search terms CSV.
    :param search_query: The query string to be passed to the search API.
    :param rates_df: The dataframe containing the rate limit data.
    :param initial_repo_output_path: Path to the initial repository output file.
    :param source_type: The type of the source for the search term.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    """
    # Now search for repos that contain query string
    search_repos_query = f'https://api.github.com/search/repositories?q="{search_query}"&per_page=100&page=1'
    console.print(f"Searching for repos with this query: ", style="purple")
    console.print(search_repos_query, style=f"link {search_repos_query}")
    # Check how many results
    total_search_results = check_total_results(search_repos_query, auth_headers=auth_headers)

    if total_search_results > 0:
        output_term = row.search_term.replace(' ','+')
        if total_search_results > 1000:
            search_url = "https://api.github.com/search/repositories?q="
            dh_term = search_query
            params = "&per_page=100&page=1"
            initial_searched_output_path = initial_repo_output_path + f'{source_type}/' + f'repos_searched_{output_term}'
            process_large_search_data(rates_df, search_url, dh_term, params, initial_searched_output_path, row, data_directory_path)
        else:
            final_searched_output_path = initial_repo_output_path + f'{source_type}/' + f'repos_searched_{output_term}.csv'
            process_search_data(rates_df, search_repos_query, final_searched_output_path, row, data_directory_path)

def search_for_users(row: pd.Series, search_query: str, rates_df: pd.DataFrame, initial_user_output_path: str, source_type: str, data_directory_path: str):
    """
    Searches for users in the search API based on given parameters.

    :param row: The row of data from the search terms CSV.
    :param search_query: The query string to be passed to the search API.
    :param rates_df: The dataframe containing the rate limit data.
    :param initial_user_output_path: Path to the initial user output file.
    :param source_type: The type of the source for the search term.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    """
    # Now search for repos that contain query string
    search_users_query = f'https://api.github.com/search/users?q="{search_query}"&per_page=100&page=1'
    console.print(f"Searching for users with this query: ", style="purple")
    console.print(search_users_query, style=f"link {search_users_query}")
    # Check how many results
    total_search_results = check_total_results(search_users_query, auth_headers=auth_headers)
    if total_search_results > 0:
        output_term = row.search_term.replace(' ','+')
        if total_search_results > 1000:
            search_url = "https://api.github.com/search/users?q="
            dh_term = search_query
            params = "&per_page=100&page=1"
            initial_searched_output_path = initial_user_output_path + f'{source_type}/' + f'users_searched_{output_term}'
            process_large_search_data(rates_df, search_url, dh_term, params, initial_searched_output_path, row, data_directory_path)
        else:
            final_searched_output_path = initial_user_output_path + f'{source_type}/' + f'users_searched_{output_term}.csv'
            process_search_data(rates_df, search_users_query, final_searched_output_path, row, data_directory_path)

def generate_initial_search_datasets(rates_df: pd.DataFrame, initial_repo_output_path: str,  initial_user_output_path: str,  target_terms: List, data_directory_path: str):
    """
    Generates the initial search datasets using the search API. This function retrieves data from the search API based on the specified search terms and processes the data accordingly.

    :param rates_df: DataFrame containing the current rate limit information.
    :param initial_repo_output_path: Path to the initial repository output file.
    :param initial_user_output_path: Path to the initial user output file.
    :param target_terms: List of terms to be searched in the API.
    :param data_directory_path: String specifying the path to the directory where the data will be stored.
    """

    if os.path.exists(initial_repo_output_path) == False:
        os.makedirs(initial_repo_output_path)

    if os.path.exists(initial_user_output_path) == False:
        os.makedirs(initial_user_output_path)

    error_file = f"{data_directory_path}/error_logs/search_errors.csv"
    if os.path.exists(error_file):
        drop_fields = ['search_term', 'search_term_source', 'status_code', 'error_url']
        clean_write_error_file(error_file, drop_fields)
    
    final_terms = prepare_terms_and_directories(f'{data_directory_path}/derived_files/grouped_cleaned_translated_terms.csv', f'{data_directory_path}/derived_files/threshold_search_errors.csv', target_terms)
    for index, row in final_terms.iterrows():
        try:
            # Update the search term to be displayed correctly
            display_term = get_display(row.search_term) if row.directionality == 'rtl' else row.search_term
            console.print(f"Getting repos with this term {display_term} in this language {row.natural_language}. Number {index} out of {len(final_terms)}", style="bold blue")
            
            search_query = row.search_term.replace(' ', '+')
            # search_query = '"' + search_query + '"'
            source_type = row.search_term_source.lower().replace(' ', '_')
            """First check if search term exists as a topic"""
            search_for_topics(row, rates_df, initial_repo_output_path, search_query, source_type, data_directory_path)
            """Now search for repos that contain query string"""
            search_for_repos(row, search_query, rates_df, initial_repo_output_path, source_type, data_directory_path)
            """Now search for users that contain query string"""
            search_for_users(row, search_query, rates_df, initial_user_output_path, source_type, data_directory_path)
        except Exception as e:
            console.print(f"Error with {row.search_term}: {e}", style="bold red")
            # log_error_to_csv(index, row.search_term, f'{data_directory_path}/derived_files/search_errors.csv')
            continue



if __name__ == '__main__':
    rates_df = check_rate_limit()
    data_directory_path = get_data_directory_path()
    initial_repo_output_path = f"{data_directory_path}/searched_repo_data/"
    initial_user_output_path = f"{data_directory_path}/searched_user_data/"
    target_terms: list = ["Digital Humanities", "Digital History", "Computational Social Science", "Public History"]
    generate_initial_search_datasets(rates_df, initial_repo_output_path, initial_user_output_path, target_terms, data_directory_path)
