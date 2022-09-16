import re
import time
from time import sleep
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import json
import codecs
import shutil
from datetime import datetime


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def check_rate_limit():
    # Checks for rate limit so that you don't hit issues with Github API. Mostly for search API that has a 30 requests per minute https://docs.github.com/en/rest/rate-limit
    url = 'https://api.github.com/rate_limit'
    response = requests.get(url, headers=auth_headers)
    rates_df = pd.json_normalize(response.json())
    return rates_df

def check_total_pages(url):
    # Check total number of pages to get from search. Useful for not going over rate limit
    response = requests.get(f'{url}?per_page=1', headers=auth_headers)
    if len(response.links) == 0:
        total_pages = 1
    else:
        total_pages = re.search('\d+$', response.links['last']['url']).group()
    return total_pages

def check_total_results(url):
    # Check total results from api call
    response = requests.get(url, headers=auth_headers)
    if response.status_code != 200:
        time.sleep(120)
        response = requests.get(url, headers=auth_headers)
        data = response.json()
    else:
        data = response.json()
    return data['total_count']

def search_request(url):
    response = requests.get(url, headers=auth_headers)
    # response_data.extend(response.json())
    if response.status_code != 200:
        print(response.status_code)
    response_data = response.json()
    response_df = pd.DataFrame.from_dict(response_data['items'])
    response_df['query'] = query
    return response_df


def get_search_api_data(query, total_pages):
    # Thanks https://stackoverflow.com/questions/33878019/how-to-get-data-from-all-pages-in-github-api-with-python 
    dfs = []
    pbar = tqdm(total=total_pages, desc="Getting Search API Data")
    try:
        response = requests.get(query, headers=auth_headers)
        # response_data.extend(response.json())
        response_data = response.json()
        response_df = pd.DataFrame.from_dict(response_data['items'])
        response_df['query'] = query
        dfs.append(response_df)
        pbar.update(1)
        while "next" in response.links.keys():
            time.sleep(120)
            query = response.links['next']['url']
            response = requests.get(query, headers=auth_headers)
            # response_data.extend(response.json())
            response_data = response.json()
            response_df = pd.DataFrame.from_dict(response_data['items'])
            response_df['query'] = query
            response_df.to_csv(" ")
            dfs.append(response_df)
            pbar.update(1)
    
    except:
        print(f"Error with URL: {query}")

    pbar.close()
    search_df = pd.concat(dfs)
    return search_df


def process_search_data(rates_df, query, output_path, total_results):
    print(query)
    total_pages = int(check_total_pages(query))
    print(f"Total pages: {total_pages}")
    calls_remaining = rates_df['resources.search.remaining'].values[0]
    while total_pages > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.search.remaining'].values[0]
    else:
        if os.path.exists(output_path):
            repo_df = pd.read_csv(output_path)
            print(repo_df.shape[0], len(repo_df), int(total_results))
            if repo_df.shape[0] != int(total_results):
                #Could refactor this to combine new and old data rather than removing it
                os.remove(output_path)
            else:
                return repo_df
        repo_df = get_search_api_data(query, total_pages)
    
        repo_df = repo_df.reset_index(drop=True)
        repo_df.to_csv(output_path, index=False)
        return repo_df


def process_large_search_data(rates_df, search_url, term, params, output_path, total_results):
    """https://api.github.com/search/repositories?q=%22Digital+Humanities%22+created%3A2017-01-01..2017-12-31+sort:updated"""
    first_year = 2008
    current_year = datetime.now().year
    current_day = datetime.now().day
    current_month = datetime.now().month
    years = list(range(first_year, current_year+1))
    repo_dfs = []
    for year in years:
        output_path = output_path + f"_{year}.csv"
        dh_term = term.replace(' ', '+')
        if year == current_year:
            query = search_url + f"%22{dh_term}%22+created%3A{year}-01-01..{year}-{current_month}-{current_day}+sort:created{params}"
        else:
            query = search_url + f"%22{dh_term}%22+created%3A{year}-01-01..{year}-12-31+sort:created{params}"
        print(query)
        total_pages = int(check_total_pages(query))
        print(f"Total pages: {total_pages}")
        calls_remaining = rates_df['resources.search.remaining'].values[0]
        while total_pages > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.search.remaining'].values[0]
        else:
            if os.path.exists(output_path):
                repo_df = pd.read_csv(output_path)
                print(repo_df.shape[0], len(repo_df), int(total_results))
                if repo_df.shape[0] != int(total_results):
                    #Could refactor this to combine new and old data rather than removing it
                    os.remove(output_path)
                else:
                    return repo_df
            repo_df = get_search_api_data(query, total_pages)
            repo_df = repo_df.reset_index(drop=True)
            repo_df.to_csv(output_path, index=False)
            repo_dfs.append(repo_df)
        
    final_df = pd.concat(repo_dfs)
    return final_df

def build_query_directory(dh_term, language, query, total_results, output_path):
    metadata_dict = {}
    metadata_dict['dh_term'] = dh_term
    metadata_dict['language'] = language
    metadata_dict['query'] = query
    metadata_dict['total_results'] = total_results
    metadata_df = pd.DataFrame([metadata_dict])
    if os.path.exists(output_path):
        metadata_df.to_csv(output_path, mode="a", header=False, index=False)
    else:
        metadata_df.to_csv(output_path, index=False)

def generate_dh_queries(initial_output_path, rates_df):
    """Needs to: 
     - first get each language
     - then search for topics
        - then search for repos tagged with topics OR containing search query
     - then deduplicate
     - finally store by language
     """
    dh_df = pd.DataFrame([json.load(codecs.open('../data/en.Digital humanities.json', 'r', 'utf-8-sig'))])
    dh_df = dh_df.melt()
    dh_df.columns = ['language', 'dh_term']
    dh_df.loc[dh_df.language == 'de', 'language'] = 'de_en'
    final_dfs = []
    metadata_output_path = "../data/repo_query_directory.csv"
    for index, row in dh_df.iterrows():
        if index == 0:
            os.remove(metadata_output_path)
        
        search_query = row.dh_term.replace(' ', '+')
        search_topics_query = "https://api.github.com/search/topics?q=" + search_query
        response = requests.get(search_topics_query, headers=auth_headers)
        data = response.json()

        build_query_directory(row.dh_term, row.language, search_topics_query, data['total_count'], metadata_output_path)
        if data['total_count'] > 0:
            for item in data['items']:
                tagged_query = item['name'].replace(' ', '-')
                repos_tagged_query = "https://api.github.com/search/repositories?q=topic:" + tagged_query + "&per_page=100&page=1"

                total_results = check_total_results(repos_tagged_query)
                build_query_directory(row.dh_term, row.language, repos_tagged_query, total_results, metadata_output_path)
                if total_results > 0:

                    output_term = item['name'].replace(' ','_')
                    
                    if total_results > 1000:
                        search_url = "https://api.github.com/search/repositories?q=topic:"
                        term = item['name']
                        params = "&per_page=100&page=1"
                        output_path = initial_output_path + f'repos_tagged_{row.language}_{output_term}'
                        repo_df = process_search_data(rates_df, search_url, term, params, output_path, total_results)
                    else:
                        output_path = initial_output_path + f'repos_tagged_{row.language}_{output_term}.csv'
                        repo_df = process_search_data(rates_df, repos_tagged_query, output_path, total_results)
                    repo_df['dh_term'] = item['name']
                    repo_df['dh_lang'] = row.language
                    final_dfs.append(repo_df)
        search_repos_query = "https://api.github.com/search/repositories?q=" + search_query + "&per_page=100&page=1"
        total_results = check_total_results(search_repos_query)
        build_query_directory(row.dh_term, row.language, search_repos_query, total_results, metadata_output_path)
        if total_results > 0:
            output_term = row.dh_term.replace(' ','_')
            if total_results > 1000:
                search_url = "https://api.github.com/search/repositories?q=topic:"
                term = item['name']
                params = "&per_page=100&page=1"
                output_path = initial_output_path + f'repos_tagged_{row.language}_{output_term}'
                repo_df = process_search_data(rates_df, search_url, term, params, output_path, total_results)
            else:
                output_path = initial_output_path + f'repos_searched_{row.language}_{output_term}.csv'
                repo_df = process_search_data(rates_df, search_repos_query, output_path, total_results)
            repo_df['dh_term'] = row.dh_term
            repo_df['dh_lang'] = row.language
            final_dfs.append(repo_df)
    final_df = pd.concat(final_dfs)
    return final_df
        
        
