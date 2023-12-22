# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=wildcard-import
# pylint: disable=W0614
import time
import os
from datetime import datetime
import sys
import pandas as pd
import requests
sys.path.append("..")
from data_generation_scripts.utils import *
from tqdm import tqdm
import apikey
from rich import print
from rich.console import Console
import arabic_reshaper
from bidi.algorithm import get_display
import warnings
warnings.filterwarnings("ignore")
from typing import List, Dict, Any, Union, Tuple

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()

def fetch_data(query: str, auth_headers: Dict[str, str]) -> Tuple[pd.DataFrame, requests.Response]:
    """Fetch data from the API and return a DataFrame and the response object.
    
    :param query: the query to be passed to the search API
    :param auth_headers: the headers to be passed to the API
    :return: a dataframe of the data returned from the API and the response object"""
    # Initiate the request
    response = requests.get(query, headers=auth_headers)
    response_data = get_response_data(response, query)

    response_df = pd.json_normalize(response_data['items'])
    # If there is data returned, add the search query to the dataframe
    if len(response_df) > 0:
        response_df['search_query'] = query
    else:
        # If there is no data returned, load in the headers from the metadata files
        if 'repo' in query:
            response_df = pd.read_csv('../data/metadata_files/search_repo_headers.csv')
        else:
            response_df = pd.read_csv('../data/metadata_files/search_user_headers.csv')
    return response_df, response

def get_search_api_data(query: str, total_pages: int, auth_headers: Dict[str, str]) -> pd.DataFrame:
    """Function to get all data from the search API.
    
    :param query: the query to be passed to the search API
    :param total_pages: the total number of pages to be queried
    :param auth_headers: the headers to be passed to the API
    :return: a dataframe of the data returned from the API
    """
    # Initiate an empty list to store the dataframes
    dfs = []
    pbar = tqdm(total=total_pages, desc="Getting Search API Data")
    try:
        # Get the data from the API
        time.sleep(0.01)
        df, response = fetch_data(query, auth_headers)
        dfs.append(df)
        pbar.update(1)
        # Loop through the pages. A suggestion we gathered from https://stackoverflow.com/questions/33878019/how-to-get-data-from-all-pages-in-github-api-with-python
        while "next" in response.links.keys():
            time.sleep(120)
            query = response.links['next']['url']
            df, response = fetch_data(query, auth_headers)
            dfs.append(df)
            pbar.update(1)
    except:  # pylint: disable=W0702
        print(f"Error with URL: {query}")

    pbar.close()
    # Concatenate the dataframes
    search_df = pd.concat(dfs)
    return search_df

def process_search_data(rates_df: pd.DataFrame, query: str, output_path: str, total_results: int, return_dataframe: bool, row_data: Dict[str, Any]) -> pd.DataFrame:
    """Function to process the data from the search API
    
    :param rates_df: the dataframe of the current rate limit
    :param query: the query to be passed to the search API
    :param output_path: the path to the output file
    :param total_results: the total number of results to be returned from the API
    :param return_dataframe: whether to return the dataframe or not
    :param row_data: the row of data from the search terms csv
    :return: a dataframe of the data returned from the API"""
    print(query)
    total_pages = int(check_total_pages(query))
    print(f"Total pages: {total_pages}")
    calls_remaining = rates_df['resources.search.remaining'].values[0]
    while total_pages > calls_remaining:
        time.sleep(3700)
        updated_rates_df = check_rate_limit()
        calls_remaining = updated_rates_df['resources.search.remaining'].values[0]
    # Check if the file already exists
    if os.path.exists(output_path):
        # If it does load it in
        searched_df = pd.read_csv(output_path, encoding="ISO-8859-1", error_bad_lines=False)
        # Check if the number of rows is less than the total number of results
        if searched_df.shape[0] != int(total_results):
            # If it is not, move the older file to a backup location and then remove existing file
            check_if_older_file_exists(output_path)
            # Could refactor this to combine new and old data rather than removing it
            os.remove(output_path)
        else:
            # If it is, return the dataframe and don't get queries from the API
            if return_dataframe:
                return searched_df
    # If the file doesn't exist or if the numbers don't match, get the data from the API
    searched_df = get_search_api_data(query, total_pages)
    searched_df = searched_df.reset_index(drop=True)
    searched_df['search_term'] = row_data['search_term']
    searched_df['search_term_source'] = row_data['search_term_source']
    searched_df['natural_language'] = row_data['natural_language']
    searched_df['search_type'] = 'tagged' if 'topic' in query else 'searched'
    check_if_older_file_exists(output_path)
    searched_df.to_csv(output_path, index=False)
    if return_dataframe:
        return searched_df

def process_large_search_data(rates_df: pd.DataFrame, search_url: str, dh_term: str, params: str, initial_output_path: str, total_results: int, return_dataframe: bool, row_data: Dict[str, Any]) -> Optional[pd.DataFrame]:
    """Function to process the data from the search API that has over 1000 results
    An example query looks like: https://api.github.com/search/repositories?q=%22Digital+Humanities%22+created%3A2017-01-01..2017-12-31+sort:updated

    :param rates_df: the dataframe of the current rate limit
    :param search_url: the base url for the search API
    :param term: the term to be searched for
    :param params: the parameters to be passed to the search API
    :param initial_output_path: the path to the output file
    :param total_results: the total number of results to be returned from the API
    :param row_data: the row of data from the search terms csv
    :return: a dataframe of the data returned from the API"""
    # Set the first year to be searched
    first_year = 2008
    current_year = datetime.now().year
    current_day = datetime.now().day
    current_month = datetime.now().month
    # Get the years to be searched
    years = list(range(first_year, current_year+1))
    search_dfs = []
    for year in years:
        # Set the output path for the year
        yearly_output_path = initial_output_path + f"_{year}.csv"
        # Handle the case where the year is the current year
        if year == current_year:
            query = search_url + \
                f"{dh_term}+created%3A{year}-01-01..{year}-{current_month}-{current_day}+sort:created{params}"
        else:
            query = search_url + \
                f"{dh_term}+created%3A{year}-01-01..{year}-12-31+sort:created{params}"
        # Get the data from the API
        if return_dataframe:
            search_df = process_search_data(rates_df, query, yearly_output_path, total_results, return_dataframe, row_data)
            search_dfs.append(search_df)
        else:
            process_search_data(rates_df, query, yearly_output_path, total_results, return_dataframe, row_data)
    if return_dataframe:
        return pd.concat(search_dfs)

def combine_search_df(initial_repo_output_path: str, repo_output_path: str, repo_join_output_path: str, initial_user_output_path: str, user_output_path: str, user_join_output_path: str, org_output_path: str, overwrite_existing_temp_files: bool) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Function to combine the dataframes of the search API data
    
    :param initial_repo_output_path: the path to the initial repo output file
    :param repo_output_path: the path to the repo output file
    :param repo_join_output_path: the path to the repo join output file
    :param initial_user_output_path: the path to the initial user output file
    :param user_output_path: the path to the output file
    :param user_join_output_path: the path to the output file
    :param org_output_path: the path to the output file
    :param overwrite_existing_temp_files: whether to overwrite existing temp files
    :return: a dataframe of the combined data"""
    # Flag to indicate whether to return a DataFrame
    return_df = True

    # Combine repo files
    print("Combining repo files")
    repo_searched_files = read_combine_files(
        dir_path=initial_repo_output_path, check_all_dirs=False, file_path_contains='searched', large_files=False)
    repo_tagged_files = read_combine_files(dir_path=initial_repo_output_path, check_all_dirs=False, file_path_contains='tagged', large_files=False)

    # Concatenate searched and tagged files, add search query time, check if older file exists, and write to CSV
    repo_join_df = pd.concat([repo_searched_files, repo_tagged_files])
    repo_join_df['search_query_time'] = datetime.now().strftime("%Y-%m-%d")
    print("Checking if older file exists")
    check_if_older_file_exists(repo_join_output_path)
    repo_join_df.to_csv(repo_join_output_path, index=False)

    # Drop duplicates, reset index, drop search query column, and add repos
    repo_df = repo_join_df.drop_duplicates(subset='id')
    repo_df = repo_df.reset_index(drop=True)
    repo_df = repo_df.drop(columns=['search_query'])
    print("Adding repos")
    repo_df = check_add_repos(repo_df, repo_output_path, return_df=True)

    # Combine user files
    print("Combining user files")
    user_join_df = read_combine_files(dir_path=initial_user_output_path, check_all_dirs=False, file_path_contains='searched', large_files=False)
    user_join_df['search_query_time'] = datetime.now().strftime("%Y-%m-%d")
    print("Checking if older file exists")
    check_if_older_file_exists(user_join_output_path)
    user_join_df.to_csv(user_join_output_path, index=False)

    # Drop duplicates, reset index, drop search query column, and add users and orgs
    user_df = user_join_df.drop_duplicates(subset='id')
    user_df = user_df.reset_index(drop=True)
    user_df = user_df.drop(columns=['search_query'])
    org_df = user_df[user_df.type == "Organization"]
    print("Adding users")
    user_df = check_add_users(
        user_df, user_output_path, return_df, overwrite_existing_temp_files)
    print("Adding orgs")
    org_df = check_add_orgs(org_df, org_output_path,
                            return_df, overwrite_existing_temp_files)

    # Return the processed DataFrames
    return repo_df, repo_join_df, user_df, user_join_df, org_df

def generate_initial_search_datasets(rates_df, initial_repo_output_path,  repo_output_path, repo_join_output_path, initial_user_output_path,  user_output_path, user_join_output_path, org_output_path, overwrite_existing_temp_files):
    """Function to generate the queries for the search API
    :param rates_df: the dataframe of the rate limit data
    :param initial_repo_output_path: the path to the initial repo output file
    :param repo_output_path: the path to the repo output file
    :param repo_join_output_path: the path to the repo join output file
    :param initial_user_output_path: the path to the initial user output file
    :param user_output_path: the path to the output file
    :param user_join_output_path: the path to the output file
    :param org_output_path: the path to the output file
    :param overwrite_existing_temp_files: whether to overwrite existing temp files
    :return: a dataframe of the combined data"""

    cleaned_dh_terms = pd.read_csv(
        '../data/derived_files/grouped_cleaned_translated_dh_terms.csv', encoding='utf-8-sig')
    cleaned_dh_terms = cleaned_dh_terms.rename(columns={'language_code': 'natural_language', 'term': 'search_term', 'term_source': 'search_term_source'})

    if os.path.exists(initial_repo_output_path) == False:
        os.makedirs(initial_repo_output_path)

    if os.path.exists(initial_user_output_path) == False:
        os.makedirs(initial_user_output_path)
    
    # final_terms = cleaned_dh_terms[cleaned_dh_terms.natural_language.str.contains('zh')]
    rtl = cleaned_dh_terms[cleaned_dh_terms.directionality == 'rtl']
    rtl['search_term'] = rtl['search_term'].apply(lambda x: arabic_reshaper.reshape(x))
    ltr = cleaned_dh_terms[cleaned_dh_terms.directionality == 'ltr']
    final_terms = pd.concat([ltr, rtl])
    threshold = final_terms[(final_terms.natural_language == 'ko') & (final_terms.search_term == "인문학")].index[0]
    # print(threshold)
    # 
    # threshold = 839
    for _, row in final_terms[final_terms.index > threshold ].iterrows():
        display_term = get_display(row.search_term) if row.directionality == 'rtl' else row.search_term
        print(f"Getting repos with this term {display_term} in this language {row.natural_language}")
        #Check if term exists as a topic
        search_query = row.search_term.replace(' ', '+')
        search_query = '"' + search_query + '"'
        # search_query = search_query if row.search_term_source == "Digital Humanities" else '"' + search_query + '"' 
        search_topics_query = "https://api.github.com/search/topics?q=" + search_query
        time.sleep(5)
        response = requests.get(search_topics_query, headers=auth_headers, timeout=5)
        data = get_response_data(response, search_topics_query)

        source_type = row.search_term_source.lower().replace(' ', '_')
        # If term exists as a topic proceed
        if data['total_count'] > 0:
            # Term may result in multiple topics so loop through them
            for item in data['items']:
                if row.search_term == 'Public History':
                    if item['name'] == 'solana':
                        continue
                dh_term = item['name']
                # Topics are joined by hyphens rather than plus signs in queries
                tagged_query = item['name'].replace(' ', '-')
                repos_tagged_query = "https://api.github.com/search/repositories?q=topic:" + tagged_query + "&per_page=100&page=1"
                # Check how many results
                total_tagged_results = check_total_results(repos_tagged_query)
                #If results exist then proceed
                if total_tagged_results > 0:

                    output_term = item['name'].replace(' ','_')
                    # If more than 1000 results, need to reformulate the queries by year since Github only returns max 1000 results
                    if total_tagged_results > 1000:
                        search_url = "https://api.github.com/search/repositories?q=topic:"
                        params = "&per_page=100&page=1"
                        initial_tagged_output_path = initial_repo_output_path + \
                            f'{source_type}/' + f'repos_tagged_{output_term}'
                        process_large_search_data(rates_df, search_url, tagged_query, params, initial_tagged_output_path, total_tagged_results, row)
                    else:
                        # If fewer than a 1000 proceed to normal search calls
                        final_tagged_output_path = initial_repo_output_path + f'{source_type}/' + f'repos_tagged_{output_term}.csv'
                        process_search_data(rates_df, repos_tagged_query, final_tagged_output_path, total_tagged_results, row)

        # Now search for repos that contain query string
        search_repos_query = "https://api.github.com/search/repositories?q=" + search_query + "&per_page=100&page=1"
        # Check how many results
        total_search_results = check_total_results(search_repos_query)

        if total_search_results > 0:
            output_term = row.search_term.replace(' ','+')
            if total_search_results > 1000:
                search_url = "https://api.github.com/search/repositories?q="
                dh_term = search_query
                params = "&per_page=100&page=1"
                initial_searched_output_path = initial_repo_output_path + f'{source_type}/' + f'repos_searched_{output_term}'
                process_large_search_data(rates_df, search_url, dh_term, params, initial_searched_output_path, total_search_results, row)
            else:
                final_searched_output_path = initial_repo_output_path + f'{source_type}/' + f'repos_searched_{output_term}.csv'
                process_search_data(rates_df, search_repos_query, final_searched_output_path, total_search_results, row)

        # Now search for repos that contain query string
        search_users_query = "https://api.github.com/search/users?q=" + search_query + "&per_page=100&page=1"
        # Check how many results
        total_user_search_results = check_total_results(search_users_query)
        if total_user_search_results > 0:
            output_term = row.search_term.replace(' ','+')
            if total_user_search_results > 1000:
                search_url = "https://api.github.com/search/users?q="
                dh_term = search_query
                params = "&per_page=100&page=1"
                initial_searched_output_path = initial_user_output_path + f'{source_type}/' + f'users_searched_{output_term}'
                process_large_search_data(rates_df, search_url, dh_term, params, initial_searched_output_path, total_search_results, row)
            else:
                final_searched_output_path = initial_user_output_path + f'{source_type}/' + f'users_searched_{output_term}.csv'
                process_search_data(rates_df, search_users_query, final_searched_output_path, total_search_results, row)


    repo_df, repo_join_df, user_df, user_join_df, org_df = combine_search_df(initial_repo_output_path, repo_output_path, repo_join_output_path, initial_user_output_path, user_output_path, user_join_output_path, org_output_path, overwrite_existing_temp_files)
    join_unique_field = 'search_query'
    repo_filter_fields = ["full_name", "cleaned_search_query"]
    user_filter_fields = ["login", "cleaned_search_query"]
    repo_join_df["cleaned_search_query"] = repo_join_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
    user_join_df["cleaned_search_query"] = user_join_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
    repo_join_df = check_for_joins_in_older_queries(repo_join_output_path, repo_join_df, join_unique_field, repo_filter_fields)
    user_join_df = check_for_joins_in_older_queries(user_join_output_path, user_join_df, join_unique_field, user_filter_fields)
    return repo_df, repo_join_df, user_df, user_join_df, org_df

def get_initial_search_datasets(rates_df, initial_repo_output_path,  repo_output_path, repo_join_output_path, initial_user_output_path,  user_output_path, user_join_output_path, org_output_path, overwrite_existing_temp_files, load_existing_data):
    """Gets the search repo data from Github API and stores it in a dataframe
    :param final_output_path: path to store final dataframe
    :param initial_output_path: path to store initial dataframes
    :param rates_df: dataframe of rate limits
    :param load_existing_data: boolean to indicate whether to load existing data
    :param load_existing_temp_files: boolean to indicate whether to load existing temp files
    :return: dataframe of search repo data
    """
    if load_existing_data:
        if os.path.exists(repo_output_path):
            repo_df = pd.read_csv(repo_output_path)
            join_df = pd.read_csv(repo_join_output_path)
            user_df = pd.read_csv(user_output_path)
            user_join_df = pd.read_csv(user_join_output_path)
            org_df = pd.read_csv(org_output_path)
        else:
            repo_df, join_df, user_df, user_join_df, org_df = generate_initial_search_datasets(rates_df, initial_repo_output_path,  repo_output_path, repo_join_output_path, initial_user_output_path,  user_output_path, user_join_output_path, org_output_path, overwrite_existing_temp_files)
    else:
        repo_df, join_df, user_df, user_join_df, org_df  = generate_initial_search_datasets(rates_df, initial_repo_output_path,  repo_output_path, repo_join_output_path, initial_user_output_path,  user_output_path, user_join_output_path, org_output_path, overwrite_existing_temp_files)
    return repo_df, join_df, user_df, user_join_df, org_df 



if __name__ == '__main__':
    rates_df = check_rate_limit()
    initial_repo_output_path = "../data/repo_data/"
    repo_output_path = "../data/large_files/entity_files/repos_dataset.csv"
    repo_join_output_path = "../data/large_files/join_files/search_queries_repo_join_dataset.csv"

    initial_user_output_path = "../data/user_data/"
    user_output_path = "../data/entity_files/users_dataset.csv"
    user_join_output_path = "../data/join_files/search_queries_user_join_dataset.csv"
    load_existing_data = False
    overwrite_existing_temp_files = False
    org_output_path = "../data/entity_files/orgs_dataset.csv"

    get_initial_search_datasets(rates_df, initial_repo_output_path,  repo_output_path, repo_join_output_path, initial_user_output_path, user_output_path, user_join_output_path, org_output_path, overwrite_existing_temp_files, load_existing_data)
    # repo_df, repo_join_df, user_df, user_join_df, org_df = combine_search_df(
    #     initial_repo_output_path, repo_output_path, repo_join_output_path, initial_user_output_path, user_output_path, user_join_output_path, org_output_path, overwrite_existing_temp_files)
    # join_unique_field = 'search_query'
    # print("Checking for older repo joins")
    # repo_join_df = check_for_joins_in_older_queries(
    #     repo_df, repo_join_output_path, repo_join_df, join_unique_field)
    # print("Checking for older user joins")
    # user_join_df = check_for_joins_in_older_queries(
    #     user_df, user_join_output_path, user_join_df, join_unique_field)

    # console = Console()
    # cleaned_dh_terms = pd.read_csv(
    #     '../data/derived_files/grouped_cleaned_translated_dh_terms.csv', encoding='utf-8-sig')
    # cleaned_dh_terms = cleaned_dh_terms.rename(
    # columns={'language_code': 'natural_language', 'term': 'search_term', 'term_source': 'search_term_source'})
    # subset_dh_terms = cleaned_dh_terms[cleaned_dh_terms.directionality == 'rtl']
    # subset_dh_terms['search_term'] = subset_dh_terms['search_term'].apply(lambda x: arabic_reshaper.reshape(x))
    # # subset_dh_terms['search_term'] = subset_dh_terms['search_term'].apply(lambda x: get_display(x))
    # val = subset_dh_terms[subset_dh_terms.natural_language == 'ar'][0:1].search_term.values[0]
    # search_repos_query = "https://api.github.com/search/repositories?q=" + val + "&per_page=100&page=1"
    # print(search_repos_query)
    # print(get_display(val))
    # response = requests.get(search_repos_query, headers=auth_headers)
    # print(response.json())
