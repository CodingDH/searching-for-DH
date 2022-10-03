# get all repo tags  /repos/OWNER/REPO/tags
# get all repo labels /repos/OWNER/REPO/labels


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