import apikey
from tqdm import tqdm
import re
import time
import requests
import sys
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
sys.path.append("..")
from data_generation_scripts.general_utils import check_rate_limit, check_total_pages, read_csv_file


def write_results_to_csv(count_column: str, row: pd.DataFrame, entity_type: str, dir_path: str):
    """Function to write results to csv
    
    :param count_column: Column that will store the count values
    :param row: Row with the latest date
    :param entity_type: Type of entity (user or organization or repo)
    :param dir_path: Directory path to existing csv files
    """
    entity_column = "full_name" if entity_type == "repos" else "login"
    entity_name = row[entity_column].replace("/", "_")
    entity_type_singular = entity_type[:-1]
    file_path = f"{dir_path}/{entity_name}_{entity_type_singular}.csv"
    if os.path.exists(file_path):
        df = read_csv_file(file_path)
        df['coding_dh_date'] = pd.to_datetime(df['coding_dh_date'])
        # get the row with the latest date
        latest_date = df['coding_dh_date'].max()
        # update the count_column for the latest date
        df.loc[df['coding_dh_date'] == latest_date, count_column] = row[count_column]
        df.to_csv(file_path, index=False)

def get_results(row: pd.DataFrame, count_column: str, url_column: str, auth_headers: dict, entity_type: str, dir_path: str) -> pd.DataFrame:
    """Function to get total results for each user or organization
    
    :param row: Row with the latest date
    :param count_column: Column that will store the count values
    :param url_column: Column that contains the url to get the total results
    :param auth_headers: Authorization headers
    :param entity_type: Type of entity (user or organization or repo)
    :param dir_path: Directory path to existing csv files
    :return: Row with the total results"""
    url = f"{row[url_column].split('{')[0]}?per_page=1"
    total_results = check_total_pages(url, auth_headers)
    row[count_column] = total_results
    write_results_to_csv(count_column, row, entity_type, dir_path)
    return row

def get_counts(df: pd.DataFrame, url_column: str, count_column: str, entity_type: str, dir_path: str, auth_headers: dict=None) -> pd.DataFrame:
    """Function to get total results for each user or organization

    :param df: DataFrame with user or organization data
    :param url_column: Column that contains the url to get the total results
    :param count_column: Column that will store the count values
    :param entity_type: Type of entity (user or organization or repo)
    :param dir_path: Directory path to existing csv files
    :param auth_headers: Authorization headers
    :return: DataFrame with the total results"""
    if count_column in df.columns:
        needs_counts = df[df[count_column].isna()]
        has_counts = df[df[count_column].notna()]
    else:
        needs_counts = df
        has_counts = pd.DataFrame()
        
    if len(has_counts) == len(df):
        df = has_counts
    else:
        tqdm.pandas(desc=f"Getting total results for each {entity_type}'s {count_column}")
        processed_needs_counts = needs_counts.reset_index(drop=True)
        processed_needs_counts = processed_needs_counts.progress_apply(get_results, axis=1, count_column=count_column, url_column=url_column,  auth_headers=auth_headers, entity_type=entity_type, dir_path=dir_path)
        df = pd.concat([processed_needs_counts, has_counts])
    return df

def process_counts(df: pd.DataFrame, cols_df: pd.DataFrame, auth_headers: dict, entity_type: str, dir_path: str) -> pd.DataFrame:
    """Function to process counts for users, organizations, and repositories

    :param df: DataFrame with user or organization or repository data
    :param cols_df: DataFrame with the count columns and url columns
    :param auth_headers: Authorization headers
    :param entity_type: Type of entity (users or orgs or repos)
    :param dir_path: Directory path to existing csv files
    :return: DataFrame with the total results"""
    for _, row in cols_df.iterrows():
        if (row['count_column'] not in df.columns) or (df[df[row.count_column].isna()].shape[0] > 0):
            if 'url' in row.url_column:
                try:
                    print(f'Getting {row.col_name} for {entity_type}')
                    df = get_counts(df, row.url_column, row.count_column, entity_type, dir_path, auth_headers=auth_headers)
                except:
                    print(f'Issues with {row.count_column} for {entity_type}')
            else:
                print(f'Counts already exist {row.count_column} for {entity_type}')
    return df

def get_count_metadata(entity_df: pd.DataFrame, entity_type: str, dir_path: str) -> pd.DataFrame:
    """Function to get count metadata for users, organizations, and repositories

    :param entity_df: DataFrame with user or organization or repository data
    :param entity_type: Type of entity (users or orgs or repos)
    :param dir_path: Directory path to existing csv files
    :return: DataFrame with the total results"""
    auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

    auth_headers = {'Authorization': f'token {auth_token}',
                'User-Agent': 'request'}
    if entity_type == "repos":
        cols_df = read_csv_file("../data/metadata_files/repo_url_cols.csv")
        skip_types = ['review_comments_url', 'commits_url', 'collaborators_url']
        cols_df = cols_df[cols_df.url_type.isin(skip_types)]
        entity_df = process_counts(entity_df, cols_df, auth_headers, entity_type, dir_path)
    else:
        if os.path.exists("../data/metadata_files/user_url_cols.csv"):
            cols_df = read_csv_file("../data/metadata_files/user_url_cols.csv")
        else:
            cols_dict ={'followers': 'followers', 'following': 'following', 'public_repos': 'public_repos', 'public_gists': 'public_gists', 'star_count': 'starred_url', 'subscription_count': 'subscriptions_url', 'organization_count': 'organizations_url'}
            cols_df = pd.DataFrame(cols_dict.items(), columns=['count_column', 'url_column'])
            cols_df.to_csv("../data/metadata_files/user_url_cols.csv", index=False)
        if entity_type  == "orgs":
            add_cols = pd.DataFrame({'count_column': ['members_count'], 'url_column': ['members_url']})
            cols_df = pd.concat([cols_df, add_cols])
            entity_df["members_url"] = entity_df["url"].apply(lambda x: x + "/public_members")
            entity_df.members_url = entity_df.members_url.str.replace('users', 'orgs')
        entity_df = process_counts(entity_df, cols_df, auth_headers, entity_type, dir_path)
    return entity_df
            

# if __name__ == "__main__":




