from syslog import LOG_NEWS
import time
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import sys
sys.path.append("..")
from data_generation_scripts.utils import *
import shutil
import ast


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def get_languages(row):
    """Function to get languages for a repo
    :param row: row from repo_df
    :return: dictionary of languages with number of bytes"""
    response = requests.get(row.languages_url, headers=auth_headers)
    if response.status_code != 200:
        time.sleep(120)
        response = requests.get(row.languages_url, headers=auth_headers)
    return response.json()

def get_repo_languages(repo_df, output_path, rates_df):
    """Function to get languages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with languages"""
    calls_remaining = rates_df['resources.core.remaining'].values[0]
    if 'languages' in repo_df.columns:
        repos_without_languages = repo_df[repo_df.languages.isna()]
    else: 
        repos_without_languages = repo_df

    while len(repos_without_languages[repos_without_languages.languages_url.notna()]) > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.core.remaining'].values[0]
    else:
        tqdm.pandas(desc="Getting Languages")
        repos_without_languages['languages'] = repos_without_languages.progress_apply(get_languages, axis=1)
        repo_df = pd.concat([repo_df, repos_without_languages])
        repo_df = repo_df.drop_duplicates(subset=['id'])
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_labels(row):
    """Function to get labels for a repo
    :param row: row from repo_df
    :return: list of labels
    Could save this output in separate file since labels also returns url, color, description, and whether the label is default or not"""
    response = requests.get(row.labels_url.str.split('{')[0], headers=auth_headers)
    if response.status_code != 200:
        time.sleep(120)
        response = requests.get(row.labels_url, headers=auth_headers)
    labels_df = pd.DataFrame(response.json())
    if len(labels_df) > 0:
        labels = labels_df['name'].tolist()
    else:
        labels = []
    return labels

def get_repo_labels(repo_df, output_path, rates_df):
    """Function to get labels for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with labels"""
    calls_remaining = rates_df['resources.core.remaining'].values[0]
    if 'labels' in repo_df.columns:
        repos_without_labels = repo_df[repo_df.labels.isna()]
    else: 
        repos_without_labels = repo_df
    while len(repos_without_labels[repos_without_labels.labels_url.notna()]) > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.core.remaining'].values[0]
    else:
        tqdm.pandas(desc="Getting Labels")
        repos_without_labels['labels'] = repos_without_labels.progress_apply(get_labels, axis=1)
        repo_df = pd.concat([repo_df, repos_without_labels])
        repo_df = repo_df.drop_duplicates(subset=['id'])
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_tags(row):
    """Function to get tags for a repo
    :param row: row from repo_df
    :return: list of tags"""
    response = requests.get(row.tags_url, headers=auth_headers)
    if response.status_code != 200:
        time.sleep(120)
        response = requests.get(row.tags_url, headers=auth_headers)
    tags_df = pd.DataFrame(response.json())
    if len(tags_df) > 0:
        tags = tags_df['name'].tolist()
    else:
        tags = []
    return tags

def get_repo_tags(repo_df, output_path, rates_df):
    """Function to get tags for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with tags"""
    calls_remaining = rates_df['resources.core.remaining'].values[0]
    if 'tags' in repo_df.columns:
        repos_without_tags = repo_df[repo_df.tags.isna()]
    else: 
        repos_without_tags = repo_df
    while len(repos_without_tags[repos_without_tags.tags_url.notna()]) > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.core.remaining'].values[0]
    else:
        tqdm.pandas(desc="Getting Tags")
        repos_without_tags['tags'] = repos_without_tags.progress_apply(get_tags, axis=1)
        repo_df = pd.concat([repo_df, repos_without_tags])
        repo_df = repo_df.drop_duplicates(subset=['id'])
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_teams():
    pass

def get_combined_repo_actors(repo_actors_temp_path, repo_actors_output_path):
    repo_actors_rows = []
    for subdir, _, files in os.walk(repo_actors_temp_path):
        for f in files:
            try:
                temp_df = pd.read_csv(subdir + '/' + f)
                repo_actors_rows.append(temp_df)
            except pd.errors.EmptyDataError:
                print(f'Empty dataframe for {f}')
    if len(repo_actors_rows) > 0:
        repo_actors_df = pd.concat(repo_actors_rows)
        repo_actors_df.to_csv(repo_actors_output_path, index=False)
    else:
        repo_actors_df = pd.DataFrame()
    return repo_actors_df

def get_actors(repo_df, repo_actors_output_path, users_output_path, get_url_field, is_stargazers):
    """Function to get all contributors to a list of repositories and also update final list of users.
    :param repo_df: dataframe of repositories
    :param repo_contributors_output_path: path to repo contributors file
    :param users_output_path: path to users file
    returns: dataframe of repo contributors and unique users"""
    error_file_path = f"../data/error_logs/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
    if os.path.exists(error_file_path):
        os.remove(error_file_path)

    temp_repo_actors_dir = f"../data/temp/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}/"
    if os.path.exists(temp_repo_actors_dir):
        shutil.rmtree(temp_repo_actors_dir)
        os.makedirs(temp_repo_actors_dir)
    else:
        os.makedirs(temp_repo_actors_dir)  
    
    temp_users_dir = f"../data/temp_users/"
    urls_df = pd.read_csv("../data/repo_url_cols.csv")
    original_auth_headers = auth_headers.copy()
    repo_progress_bar = tqdm(total=len(repo_df), desc="Getting Repo Actors", position=0)
    users_progress_bar = tqdm(total=0, desc="Getting Users", position=1)
    for _, row in repo_df.iterrows():
        try:
            counts_exist = urls_df[urls_df.url_type == get_url_field].count_type.values[0]

            if counts_exist != 'None':
                if (row[counts_exist] == 0):
                    repo_progress_bar.update(1)
                    continue
            dfs = []
            temp_repo_actors_path =  F"{row.full_name.replace('/','')}_repo_actors_{get_url_field}.csv"
            url = row[get_url_field].split('{')[0] + '?per_page=100&page=1' if '{' in row[get_url_field] else row[get_url_field] + '?per_page=100&page=1'
            if is_stargazers:
                original_auth_headers = original_auth_headers | {'Accept': 'application/vnd.github.star+json'} 
            response = requests.get(url, headers=original_auth_headers)
            response_data = get_response_data(response, url)
            response_df = pd.json_normalize(response_data)
            dfs.append(response_df)
            while "next" in response.links.keys():
                time.sleep(120)
                query = response.links['next']['url']
                response = requests.get(query, headers=auth_headers)
                response_data = get_response_data(response, query)
                response_df = pd.json_normalize(response_data)
                dfs.append(response_df)
            data_df = pd.concat(dfs)
            if len(data_df) == 0:
                repo_progress_bar.update(1)
                continue
            else:
                if is_stargazers:
                    cols = [col.split('user.')[-1] for col in data_df.columns.to_list()]
                    data_df.columns = cols
                cols = ast.literal_eval(urls_df[urls_df.url_type == get_url_field].cols.values[0])
                repo_actors_df = data_df[cols].copy()
                repo_actors_df['repo_id'] = row.id
                repo_actors_df['repo_url'] = row.url
                repo_actors_df['repo_html_url'] = row.html_url
                repo_actors_df['repo_full_name'] = row.full_name
                repo_actors_df[get_url_field] = row[get_url_field]
                repo_actors_df.to_csv(temp_repo_actors_dir + temp_repo_actors_path, index=False)
                if 'login' in data_df.columns:
                    check_add_users(data_df, users_output_path, temp_users_dir, users_progress_bar, return_df=False)
                repo_progress_bar.update(1)
            
        except:
            print(f"Error on getting actors for {row.full_name}")
            error_df = pd.DataFrame([{'repo_full_name': row.full_name, 'error_time': time.time(), f'{get_url_field}': row[get_url_field]}])
            if os.path.exists(error_file_path):
                error_df.to_csv(error_file_path, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file_path, index=False)
            continue
    repo_actors_df = get_combined_repo_actors(temp_repo_actors_dir, repo_actors_output_path)
    shutil.rmtree(temp_repo_actors_dir)
    users_df = get_user_df(users_output_path)
    repo_progress_bar.close()
    users_progress_bar.close()
    return repo_actors_df, users_df


def get_repos_user_actors(repo_df,repo_actors_output_path, users_output_path, rates_df, get_url_field, load_existing, is_stargazers):
    """Function to take a list of repositories and get any user activities that are related to a repo, save that into a join table, and also update final list of users.
    :param repo_df: dataframe of repositories
    :param repo_actors_output_path: path to repo actors file (actors here could be subscribers, stargazers, etc...)
    :param users_output_path: path to users file
    :param rates_df: dataframe of rate limits
    :param get_url_field: field in repo_df that contains the url to get the actors
    :param load_existing: boolean to load existing data
    :param is_stargazers: boolean to indicate if the actors are stargazers because stargazers have a slightly different Auth Headers
    returns: dataframe of repo contributors and unique users"""
    if load_existing:
        # if the file already exists, load it and return it
        repo_actors_df = pd.read_csv(repo_actors_output_path, low_memory=False)
        users_df = pd.read_csv(users_output_path, low_memory=False)
    else:
        # if the file doesn't exist, check that we have enough rate limits to get the data
        calls_remaining = rates_df['resources.core.remaining'].values[0]
        while len(repo_df[repo_df[get_url_field].notna()]) > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.core.remaining'].values[0]
        else:
            # if we have enough rate limits, first check if we already have an existing file with the repo actors so that we don't have to get the entirety each time
            if os.path.exists(repo_actors_output_path):
                repo_actors_df = pd.read_csv(repo_actors_output_path, low_memory=False)
                users_df = pd.read_csv(users_output_path, low_memory=False)
                unprocessed_actors = repo_df[~repo_df[get_url_field].isin(repo_actors_df[get_url_field])]
                error_file_path = f"../data/error_logs/{repo_actors_output_path.split('/')[-1].split('.csv')[0]}_errors.csv"
                if os.path.exists(error_file_path):
                    error_df = pd.read_csv(error_file_path)
                    unprocessed_actors = unprocessed_actors[~unprocessed_actors[get_url_field].isin(error_df[get_url_field])]
                if len(unprocessed_actors) > 0:
                    new_actors_df, users_df = get_actors(unprocessed_actors, repo_actors_output_path, users_output_path, get_url_field, is_stargazers)
                    repo_actors_df = pd.concat([unprocessed_actors, new_actors_df])
                    repo_actors_df.to_csv(repo_actors_output_path, index=False)
            else:
                repo_actors_df, users_df = get_actors(repo_df, repo_actors_output_path, users_output_path, get_url_field, is_stargazers)
        
    return repo_actors_df, users_df

if __name__ == "__main__":
    repo_df = pd.read_csv("../data/repos_dataset.csv", low_memory=False)
    repo_actors_output_path = "../data/repo_stargazers_join_dataset.csv"
    users_output_path = "../data/users_dataset.csv"
    rates_df = check_rate_limit()
    repo_actors_df, users_df = get_repos_user_actors(repo_df, repo_actors_output_path, users_output_path, rates_df, 'stargazers_url', load_existing=False)