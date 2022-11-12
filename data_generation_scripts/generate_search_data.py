# This script is currently organized based on previous ideas of having files for each search term. We are moving away from this but for now I'm leaving the current structure and just reorganizing the files post-ad hoc generation.
import time
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import json
import codecs
from datetime import datetime
import sys
sys.path.append("..")
from data_generation_scripts.utils import *


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}


def get_search_api_data(query, total_pages):
    """Function to get all data from the search API
    :param query: the query to be passed to the search API
    :param total_pages: the total number of pages to be queried
    :return: a dataframe of the data returned from the API"""
    # Thanks https://stackoverflow.com/questions/33878019/how-to-get-data-from-all-pages-in-github-api-with-python 
    dfs = []
    pbar = tqdm(total=total_pages, desc="Getting Search API Data")
    try:
        response = requests.get(query, headers=auth_headers)
        response_data = get_response_data(response, query)

        response_df = pd.json_normalize(response_data['items'])
        if len(response_df) > 0:
            response_df['search_query'] = query
        else:
            response_df = pd.read_csv('../data/metadata_files/search_repo_headers.csv')
        dfs.append(response_df)
        pbar.update(1)
        while "next" in response.links.keys():
            time.sleep(120)
            query = response.links['next']['url']
            response = requests.get(query, headers=auth_headers)
            response_data = get_response_data(response, query)

            response_df = pd.json_normalize(response_data['items'])
            if len(response_df) > 0:
                response_df['search_query'] = query
            else:
                if 'repo' in query:
                    response_df = pd.read_csv('../data/metadata_files/search_repo_headers.csv')
                else: 
                    response_df = pd.read_csv('../data/metadata_files/search_user_headers.csv')
            dfs.append(response_df)
            pbar.update(1)
    
    except:
        print(f"Error with URL: {query}")

    pbar.close()
    search_df = pd.concat(dfs)
    return search_df


def process_search_data(rates_df, query, output_path, total_results):
    """Function to process the data from the search API
    :param rates_df: the dataframe of the current rate limit
    :param query: the query to be passed to the search API
    :param output_path: the path to the output file
    :param total_results: the total number of results to be returned from the API
    :return: a dataframe of the data returned from the API"""
    print(query)
    total_pages = int(check_total_pages(query))
    print(f"Total pages: {total_pages}")
    calls_remaining = rates_df['resources.search.remaining'].values[0]
    while total_pages > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.search.remaining'].values[0]
    else:
        # Check if the file already exists
        if os.path.exists(output_path):
            # If it does load it in
            searched_df = pd.read_csv(output_path)
            # Check if the number of rows is less than the total number of results
            if searched_df.shape[0] != int(total_results):
                # If it is not, move the older file to a backup location and then remove existing file
                check_if_older_file_exists(output_path)
                #Could refactor this to combine new and old data rather than removing it
                os.remove(output_path)
            else:
                # If it is, return the dataframe and don't get queries from the API
                return searched_df 
        # If the file doesn't exist or if the numbers don't match, get the data from the API
        searched_df = get_search_api_data(query, total_pages)
        searched_df = searched_df.reset_index(drop=True)
        check_if_older_file_exists(output_path)
        searched_df.to_csv(output_path, index=False)


def process_large_search_data(rates_df, search_url, dh_term, params, initial_output_path, total_results):
    """https://api.github.com/search/repositories?q=%22Digital+Humanities%22+created%3A2017-01-01..2017-12-31+sort:updated
    Function to process the data from the search API that has over 1000 results
    :param rates_df: the dataframe of the current rate limit
    :param search_url: the base url for the search API
    :param term: the term to be searched for
    :param params: the parameters to be passed to the search API
    :param initial_output_path: the path to the output file
    :param total_results: the total number of results to be returned from the API"""
    first_year = 2008
    current_year = datetime.now().year
    current_day = datetime.now().day
    current_month = datetime.now().month
    years = list(range(first_year, current_year+1))
    search_dfs = []
    for year in years:
        yearly_output_path = initial_output_path + f"_{year}.csv"
        cleaned_dh_term = dh_term.replace(' ', '+')
        if year == current_year:
            query = search_url + f"%22{cleaned_dh_term}%22+created%3A{year}-01-01..{year}-{current_month}-{current_day}+sort:created{params}"
        else:
            query = search_url + f"%22{cleaned_dh_term}%22+created%3A{year}-01-01..{year}-12-31+sort:created{params}"
        print(query)
        total_pages = int(check_total_pages(query))
        print(f"Total pages: {total_pages}")
        calls_remaining = rates_df['resources.search.remaining'].values[0]
        total_results = check_total_results(query)
        while total_pages > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.search.remaining'].values[0]
        else:
            if os.path.exists(yearly_output_path):
                search_df = pd.read_csv(yearly_output_path)
                if search_df.shape[0] != int(total_results):
                    #Could refactor this to combine new and old data rather than removing it
                    check_if_older_file_exists(yearly_output_path)
                    os.remove(yearly_output_path)
                else:
                    search_dfs.append(search_df)
                    return search_df
            search_df = get_search_api_data(query, total_pages)
            search_df = search_df.reset_index(drop=True)
            check_if_older_file_exists(yearly_output_path)
            search_df.to_csv(yearly_output_path, index=False)
        
   

def combine_search_df(initial_repo_output_path, repo_output_path, repo_join_output_path, initial_user_output_path, user_output_path, user_join_output_path, overwrite_existing_temp_files):
    """Function to combine the dataframes of the search API data
    :param repo_output_path: the path to the output file
    :param join_output_path: the path to the output file
    :return: a dataframe of the combined data"""
    return_df = True
    repo_searched_files = read_combine_files(initial_repo_output_path, 'searched')
    repo_tagged_files = read_combine_files(repo_output_path, 'tagged')

    repo_join_df = pd.concat([repo_searched_files, repo_tagged_files])
    repo_join_df['search_query_time'] = datetime.now().strftime("%Y-%m-%d")
    check_if_older_file_exists(repo_join_output_path)
    repo_join_df.to_csv(repo_join_output_path, index=False)
    repo_df = repo_join_df.drop_duplicates(subset='id')
    repo_df = repo_df.reset_index(drop=True)
    repo_df = repo_df.drop(columns=['search_query'])
    repo_df = check_add_repos(repo_df, repo_output_path, overwrite_existing_temp_files, return_df=True)


    user_join_df = read_combine_files(initial_user_output_path, 'searched')
    user_join_df['search_query_time'] = datetime.now().strftime("%Y-%m-%d")
    check_if_older_file_exists(user_join_output_path)
    user_join_df.to_csv(user_join_output_path, index=False)
    user_df = user_join_df.drop_duplicates(subset='id')
    user_df = user_df.reset_index(drop=True)
    user_df = user_df.drop(columns=['search_query'])


    user_df = check_add_users(user_df, user_output_path, return_df, overwrite_existing_temp_files)

    return repo_df, repo_join_df, user_df, user_join_df

def generate_initial_dh_repos(rates_df, initial_repo_output_path,  repo_output_path, repo_join_output_path, initial_user_output_path,  user_output_path, user_join_output_path, overwrite_existing_temp_files=True):
    """Function to generate the queries for the search API
    :param initial_repo_output_path: the path to the output file
    :param rates_df: the dataframe of the current rate limit
    :param repo_output_path: the path to the output file
    :param join_output_path: the path to the output file
    :param overwrite_existing_temp_files: whether to overwrite existing temp files or not
    :return: a dataframe of the data returned from the API"""

    #Get the list of terms to search for
    dh_df = pd.DataFrame([json.load(codecs.open('../data/metadata_files/en.Digital humanities.json', 'r', 'utf-8-sig'))])
    dh_df = dh_df.melt()
    dh_df.columns = ['language', 'dh_term']
    # Combine German and English terms because of identical spelling (should maybe make this a programatic check)
    dh_df.loc[dh_df.language == 'de', 'language'] = 'de_en'

    for _, row in dh_df.iterrows():
        print(f"Getting repos with this term: {row.dh_term} in this language: {row.language}")
        
        #Check if term exists as a topic
        search_query = row.dh_term.replace(' ', '+')
        search_topics_query = "https://api.github.com/search/topics?q=" + search_query
        response = requests.get(search_topics_query, headers=auth_headers)
        data = get_response_data(response, search_topics_query)

        # If term exists as a topic proceed
        if data['total_count'] > 0:
            # Term may result in multiple topics so loop through them
            for item in data['items']:
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
                        initial_tagged_output_path = initial_repo_output_path + f'repos_tagged_{row.language}_{output_term}'
                        process_large_search_data(rates_df, search_url, dh_term, params, initial_tagged_output_path, total_tagged_results)
                    else:
                        # If fewer than a 1000 proceed to normal search calls
                        final_tagged_output_path = initial_repo_output_path + f'repos_tagged_{row.language}_{output_term}.csv'
                        process_search_data(rates_df, repos_tagged_query, final_tagged_output_path, total_tagged_results)

        # Now search for repos that contain query string
        search_repos_query = "https://api.github.com/search/repositories?q=" + search_query + "&per_page=100&page=1"
        # Check how many results 
        total_search_results = check_total_results(search_repos_query)

        if total_search_results > 0:
            output_term = row.dh_term.replace(' ','+')
            if total_search_results > 1000:
                search_url = "https://api.github.com/search/repositories?q="
                dh_term = row.dh_term
                params = "&per_page=100&page=1"
                initial_searched_output_path = initial_repo_output_path + f'repos_searched_{row.language}_{output_term}'
                process_large_search_data(rates_df, search_url, dh_term, params, initial_searched_output_path, total_search_results)
                
            else:
                final_searched_output_path = initial_repo_output_path + f'repos_searched_{row.language}_{output_term}.csv'
                process_search_data(rates_df, search_repos_query, final_searched_output_path, total_search_results)

        # Now search for repos that contain query string
        search_users_query = "https://api.github.com/search/users?q=" + search_query + "&per_page=100&page=1"
        # Check how many results 
        total_user_search_results = check_total_results(search_users_query)
        if total_user_search_results > 0:
            output_term = row.dh_term.replace(' ','+')
            if total_user_search_results > 1000:
                search_url = "https://api.github.com/search/users?q="
                dh_term = row.dh_term
                params = "&per_page=100&page=1"
                initial_searched_output_path = initial_user_output_path + f'users_searched_{row.language}_{output_term}'
                process_large_search_data(rates_df, search_url, dh_term, params, initial_searched_output_path, total_search_results)
                
            else:
                final_searched_output_path = initial_user_output_path + f'users_searched_{row.language}_{output_term}.csv'
                process_search_data(rates_df, search_users_query, final_searched_output_path, total_search_results)


    repo_df, repo_join_df, user_df, user_join_df = combine_search_df(initial_repo_output_path, repo_output_path, repo_join_output_path, initial_user_output_path, user_output_path, user_join_output_path, overwrite_existing_temp_files)
    join_unique_field = 'search_query'
    repo_join_df = check_for_joins_in_older_queries(repo_df, repo_join_output_path, repo_join_df, join_unique_field)
    user_join_df = check_for_joins_in_older_queries(user_df, user_join_output_path, user_join_df, join_unique_field)
    return repo_df, repo_join_df, user_df, user_join_df
        
def get_initial_repo_df(repo_output_path, join_output_path, initial_output_path, rates_df, load_existing_data, load_existing_temp_files):
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
            join_df = pd.read_csv(join_output_path)
        else:
            repo_df, join_df = generate_initial_dh_repos(initial_output_path, rates_df, repo_output_path, join_output_path, load_existing_temp_files)
    else:
        repo_df, join_df = generate_initial_dh_repos(initial_output_path, rates_df, repo_output_path, join_output_path, load_existing_temp_files)
    return repo_df, join_df



