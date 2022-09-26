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
    if os.path.exists(output_path):
        repo_df = pd.read_csv(output_path)
    else:
        while len(repo_df[repo_df.languages_url.notna()]) > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.core.remaining'].values[0]
        else:
            tqdm.pandas(desc="Getting Languages")
            repo_df['languages'] = repo_df.progress_apply(get_languages, axis=1)
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
    if os.path.exists(output_path):
        repo_df = pd.read_csv(output_path)
    else:
        while len(repo_df[repo_df.labels_url.notna()]) > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.core.remaining'].values[0]
        else:
            tqdm.pandas(desc="Getting Labels")
            repo_df['labels'] = repo_df.progress_apply(get_labels, axis=1)
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
    if os.path.exists(output_path):
        repo_df = pd.read_csv(output_path)
    else:
        while len(repo_df[repo_df.tags_url.notna()]) > calls_remaining:
            time.sleep(3700)
            rates_df = check_rate_limit()
            calls_remaining = rates_df['resources.core.remaining'].values[0]
        else:
            tqdm.pandas(desc="Getting Tags")
            repo_df['tags'] = repo_df.progress_apply(get_tags, axis=1)
            repo_df.to_csv(output_path, index=False)
    return repo_df