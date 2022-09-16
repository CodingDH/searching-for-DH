import re
import time
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import sys
sys.path.append("..")
from data_generation_scripts.utils import *


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def get_total_commits(url):
    response = requests.get(f'{url}?per_page=1', headers=auth_headers)
    if response.status_code != 200:
        print('hit rate limiting. trying to sleep...')
        time.sleep(120)
        response = requests.get(url, headers=auth_headers)
        total_commits = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
    else:
        total_commits = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
    return total_commits

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