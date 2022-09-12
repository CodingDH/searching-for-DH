from time import sleep
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import re
import time

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
    return re.search('\d+$', requests.get(f'{url}?per_page=1', headers=auth_headers).links['last']['url']).group()

def check_if_new_repos(repo_df):
    # Checks if there are any new repos or not
    query = f"https://api.github.com/search/repositories?q=topic:digital-humanities&per_page=100&page=1"
    try:
        response = requests.get(query, headers=auth_headers)
        response_data = response.json()
        updated_count = response_data['total_count']
        if updated_count > repo_df.shape[0]:    
            return True
    except:
        print(f"Error on checking for new repos")
        return False

def get_search_api_data(query, total_pages):
    # Thanks https://stackoverflow.com/questions/33878019/how-to-get-data-from-all-pages-in-github-api-with-python 
    dfs = []
    pbar = tqdm(total=total_pages, desc="Getting Search API Data")
    try:
        response = requests.get(f"{query}", headers=auth_headers)
        response_data = response.json()
        response_df = pd.DataFrame.from_dict(response_data['items'])
        response_df['query'] = query
        dfs.append(response_df)
        pbar.update(1)
        while "next" in response.links.keys():
            url = response.links['next']['url']
            response = requests.get(url, headers=auth_headers)
            # response_data.extend(response.json())
            response_data = response.json()
            response_df = pd.DataFrame.from_dict(response_data['items'])
            response_df['query'] = query
            dfs.append(response_df)
            pbar.update(1)
    
    except:
        print(f"Error with URL: {url}")
    pbar.close()
    repo_df = pd.concat(dfs)
    return repo_df

def get_dh_repos_data(output_path, rates_df):
    #Specifically does a query for DH repos, but checks if rate limit will be hit or not, and whether there are new repos
    query = f"https://api.github.com/search/repositories?q=topic:digital-humanities&per_page=100&page=1"

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
            new_repos = check_if_new_repos(repo_df)
            if new_repos:
                #Could refactor this to combine new and old data rather than removing it
                os.remove(output_path)
            else:
                return repo_df
        
        repo_df = get_search_api_data(query, total_pages)
    
        repo_df = repo_df.reset_index(drop=True)
        repo_df.to_csv(output_path, index=False)
        return repo_df

def get_api_data(query):
    # Thanks https://stackoverflow.com/questions/33878019/how-to-get-data-from-all-pages-in-github-api-with-python

    try:
        response = requests.get(f"{query}", headers=auth_headers)
        response_data = response.json()

        while "next" in response.links.keys():
            url = response.links['next']['url']
            response = requests.get(url, headers=auth_headers)
            response_data.extend(response.json())
            
    except:
        print(f"Error with URL: {url}")

    return response_data

def get_languages(row):
    response = requests.get(row.languages_url, headers=auth_headers)
    return response.json()

def get_repo_languages(repo_df, output_path, rates_df):
    calls_remaining = rates_df['resources.core.remaining']
    if os.path.exists(output_path):
        repo_df = pd.read_csv(output_path)
    else:
        while len(repo_df[repo_df.languages_url.notna()]) > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.core.remaining']
        else:
            tqdm.pandas(desc="Getting Languages")
            repo_df['languages'] = repo_df.progress_apply(get_languages, axis=1)
            repo_df.to_csv(output_path, index=False)
    return repo_df

def get_contributors(repo_df, output_path):
    contributors_rows = []
    for _, row in tqdm(repo_df.iterrows(), total=repo_df.shape[0], desc="Getting Contributors"):
        try: 
            url = row.contributors_url
            response = requests.get(url, headers=auth_headers)
            response_data = response.json()
            df = pd.json_normalize(response_data)
            df['repo_id'] = row.id
            df['html_url'] = row.html_url
            df['full_name'] = row.full_name
            expanded_response = requests.get(df.url.values[0], headers=auth_headers)
            expanded_df = pd.json_normalize(expanded_response.json())
            cols = list(set(expanded_df.columns) & set(df.columns))
            merged_df = df.merge(expanded_df, on=cols, how='left')
            contributors_rows.append(merged_df)
        except:
            print(f"Error on getting contributors for {row.full_name}")
            continue
    contributors_df = pd.concat(contributors_rows)
    contributors_df.to_csv(output_path, index=False)
    return contributors_df

def get_repo_contributors(repo_df, output_path, rates_df):
    calls_remaining = rates_df['resources.core.remaining'].values[0]
    while len(repo_df[repo_df.contributors_url.notna()]) > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.core.remaining'].values[0]
    else:
        if os.path.exists(output_path):
            contributors_df = pd.read_csv(output_path)
            if len(contributors_df[contributors_df.login.isna()]) > 0:
                existing_contributors = contributors_df[contributors_df.login.isna() == False]
                missing_repos = contributors_df[contributors_df.login.isna()].html_url.unique().tolist()
                missing_repos_df = repo_df[repo_df.html_url.isin(missing_repos)]
                missing_repos_df = get_contributors(missing_repos_df, output_path)
                contributors_df = pd.concat([existing_contributors, missing_repos_df])
                contributors_df.to_csv(output_path, index=False)
        else:
            contributors_df = get_contributors(repo_df, output_path)
    return contributors_df

def get_total_commits(url):
    return re.search('\d+$', requests.get(f'{url}?per_page=1', headers=auth_headers).links['last']['url']).group()

def get_commits(repo_df, output_path):
    commits_rows = []
    for _, row in tqdm(repo_df.iterrows(), total=repo_df.shape[0], desc="Getting Commits"):
        try:
            url = row.commits_url.split('{')[0]
            response_data = get_api_data(url)
            df = pd.json_normalize(response_data)
            df['repo_id'] = row.id
            df['html_url'] = row.html_url
            df['full_name'] = row.full_name
            commits_rows.append(df)
        except:
            print(f"Error on getting commits for {row.full_name}")
            continue
    commits_df = pd.concat(commits_rows)
    commits_df.to_csv(output_path, index=False)
    return commits_df

def get_repos_commits(repo_df, output_path, rates_df):
    calls_remaining = rates_df['resources.core.remaining'].values[0]
    while len(repo_df[repo_df.html_url.notna()]) > calls_remaining:
        time.sleep(3700)
        rates_df = check_rate_limit()
        calls_remaining = rates_df['resources.core.remaining'].values[0]
    else:
        if os.path.exists(output_path):
            commits_df = pd.read_csv(output_path, low_memory=False)
            repos = repo_df.html_url.unique().tolist()
            existing_repos = commits_df[commits_df.html_url.isin(repos)].html_url.unique().tolist()
            if len(existing_repos) != len(repos):
                missing_commits_repos = set(repos) - set(existing_repos)
                missing_repos_df = repo_df[repo_df.html_url.isin(missing_commits_repos)]
                missing_repos_df = get_commits(missing_repos_df, output_path)
                final_commits_df = pd.concat([commits_df, missing_repos_df])
                final_commits_df = final_commits_df.reset_index(drop=True)
                final_commits_df['commit.committer.date_time'] = pd.to_datetime(final_commits_df['commit.committer.date'], format='%Y-%m-%dT%H:%M:%SZ')
                final_commits_df['date'] = final_commits_df['commit.committer.date_time'].dt.date
                final_commits_df['datetime'] = pd.to_datetime(final_commits_df['date'])
                final_commits_df.to_csv(output_path, index=False)
            else:
                final_commits_df = commits_df
        else:
            # tqdm.pandas(desc="Getting Commits")
            # repo_df['commits'] = repo_df.progress_apply(get_commits, axis=1, output_path=output_path)
            # repo_df.to_csv(output_path, index=False)
            final_commits_df = get_commits(repo_df, output_path)
    return final_commits_df

if __name__ == "__main__":
    rates_df = check_rate_limit()
    repo_df = get_dh_repos_data('../data/repos_topic_dh.csv', rates_df)
    repo_languages_df = get_repo_languages(repo_df, '../data/repos_topic_dh_languages.csv', rates_df)
    contributors_df = get_repo_contributors(repo_df, '../data/repos_topic_dh_contributors.csv', rates_df)
    commits_df = get_repos_commits(repo_df, '../private_data/repos_topic_dh_commits.csv', rates_df)