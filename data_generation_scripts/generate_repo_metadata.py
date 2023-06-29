from syslog import LOG_NEWS
import time
from urllib.parse import parse_qs
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import sys
from ast import literal_eval
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
    temp_repo_dir = "../data/temp/repo_languages/"
    temp_name = row.full_name.replace('/', '_') + '_language.json'
    temp_path = temp_repo_dir + temp_name
    if os.path.exists(temp_path):
        with open(temp_path, 'r') as f:
            response_data = ast.literal_eval(f.read())
    else:
        try:
            response = requests.get(row.languages_url, headers=auth_headers)
            response_data = get_response_data(response, row.languages_url)
            
        except:
            print("Error getting languages for repo: " + row.full_name)
            response_data = None
        if response_data is not None:
            os.makedirs(temp_repo_dir, exist_ok=True)
            with open(temp_path, 'w') as f:
                f.write(str(response_data))
    return response_data

def get_repo_languages(repo_df, output_path):
    """Function to get languages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with languages"""
    if 'languages' in repo_df.columns:
        repos_without_languages = repo_df[repo_df.languages.isna()]
        repos_with_languages = repo_df[repo_df.languages.notna()]
    else: 
        repos_without_languages = repo_df
        repos_with_languages = pd.DataFrame()

    if len(repos_without_languages) > 0:
        tqdm.pandas(desc="Getting Languages")
        repos_without_languages['languages'] = repos_without_languages.progress_apply(get_languages, axis=1)
        repo_df = pd.concat([repos_with_languages, repos_without_languages])
        print(len(repo_df))
        repo_df = repo_df.drop_duplicates(subset=['full_name'])
        print(len(repo_df))
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_labels(row):
    """Function to get labels for a repo
    :param row: row from repo_df
    :return: list of labels
    Could save this output in separate file since labels also returns url, color, description, and whether the label is default or not"""
    response = requests.get(row.labels_url.split('{')[0], headers=auth_headers)
    response_data = get_response_data(response, row.labels_url.split('{')[0])
    if response_data is not None:
        labels_df = pd.DataFrame(response_data)
        labels = labels_df['name'].tolist()
    else:
        labels = []
    return labels

def get_repo_labels(repo_df, output_path):
    """Function to get labels for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with labels"""
    if 'labels' in repo_df.columns:
        repos_without_labels = repo_df[repo_df.labels.isna()]
        repos_with_labels = repo_df[repo_df.labels.notna()]
    else: 
        repos_without_labels = repo_df
        repos_with_labels = pd.DataFrame()
    
    if len(repos_without_labels) > 0:
        tqdm.pandas(desc="Getting Labels")
        repos_without_labels['labels'] = repos_without_labels.progress_apply(get_labels, axis=1)
        repo_df = pd.concat([repos_with_labels, repos_without_labels])
        repo_df = repo_df.drop_duplicates(subset=['id'])
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_tags(row):
    """Function to get tags for a repo
    :param row: row from repo_df
    :return: list of tags"""
    response = requests.get(row.tags_url, headers=auth_headers)
    response_data = get_response_data(response, row.tags_url)
    if response_data is not None:
        tags_df = pd.DataFrame(response_data)
        tags = tags_df['name'].tolist()
    else:
        tags = []
    return tags

def get_repo_tags(repo_df, output_path):
    """Function to get tags for all repos in repo_df
    :param repo_df: dataframe of repos
    :param output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :return: dataframe of repos with tags"""
    if 'tags' in repo_df.columns:
        repos_without_tags = repo_df[repo_df.tags.isna()]
        repos_with_tags = repo_df[repo_df.tags.notna()]
    else: 
        repos_without_tags = repo_df
        repos_with_tags = pd.DataFrame()

    if len(repos_without_tags) > 0:
        tqdm.pandas(desc="Getting Tags")
        repos_without_tags['tags'] = repos_without_tags.progress_apply(get_tags, axis=1)
        repo_df = pd.concat([repos_with_tags, repos_without_tags])
        repo_df = repo_df.drop_duplicates(subset=['id'])
        repo_df.to_csv(output_path, index=False)
    return repo_df

def get_profiles(repo_df, temp_repo_dir, error_file_path):
    """Function to get community profiles and health percentages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param temp_repo_dir: path to temp directory to write repos
    :param error_file_path: path to file to write errors
    :return: dataframe of repos with community profiles and health percentages
    """
    profile_bar = tqdm(total=len(repo_df), desc="Getting Community Profiles")
    for _, row in repo_df.iterrows():
        temp_name = row.full_name.replace('/', '_')
        temp_output_path = temp_repo_dir + f'{temp_name}_community_profile.csv'
        if os.path.exists(temp_output_path):
            profile_bar.update(1)
            continue
        else:
            try:
                response = requests.get(row.url +'/community/profile', headers=auth_headers)
                response_data = get_response_data(response, row.url +'/community/profile')
                if response_data is not None:
                    response_df = pd.json_normalize(response_data)
                    if 'message' in response_df.columns:
                        print(response_df.message.values[0])
                        profile_bar.update(1)
                        continue
                    response_df = response_df.rename(columns={'updated_at': 'community_profile_updated_at'})
                    final_df = pd.DataFrame(row.to_dict() | response_df.to_dict())
                    
                    final_df.to_csv(temp_output_path, index=False)
                
                else:
                    continue
                profile_bar.update(1)
            except:
                profile_bar.total = profile_bar.total - 1
                error_df = pd.DataFrame([{'repo_full_name': row.full_name, 'error_time': time.time(), 'url': str(row.url) +'/community/profile'}])
                if os.path.exists(error_file_path):
                    error_df.to_csv(error_file_path, mode='a', header=False, index=False)
                else:
                    error_df.to_csv(error_file_path, index=False)
                continue
    updated_repo_df = read_combine_files(dir_path=temp_repo_dir)
    updated_repo_df = updated_repo_df[updated_repo_df.full_name.isin(repo_df.full_name)]
    missing_rows = repo_df[~repo_df.full_name.isin(updated_repo_df.full_name)]
    if len(missing_rows) > 0:
        updated_repo_df = pd.concat([updated_repo_df, missing_rows])
    clean_write_error_file(error_file_path, 'repo_full_name')
    profile_bar.close()
    return updated_repo_df

def get_repo_profile(repo_df, repo_output_path, rates_df, error_file_path, temp_repo_dir, overwrite_existing_files=False):
    """Function to get community profiles and health percentages for all repos in repo_df
    :param repo_df: dataframe of repos
    :param repo_output_path: path to save output
    :param rates_df: dataframe of rate limit info
    :param error_file_path: path to file to write errors
    :param temp_repo_dir: path to temp directory to write repos
    :return: dataframe of repos with community profiles and health percentages
    """
    if os.path.exists(temp_repo_dir) and overwrite_existing_files:
        shutil.rmtree(temp_repo_dir)
    
    if not os.path.exists(temp_repo_dir):
        os.makedirs(temp_repo_dir) 

    if 'health_percentage' in repo_df.columns:
        repos_without_community_profile = repo_df[repo_df.health_percentage.isna()]
        repos_with_community_profile = repo_df[repo_df.health_percentage.notna()]
    else: 
        repos_without_community_profile = repo_df
        repos_with_community_profile = pd.DataFrame()

    if len(repos_without_community_profile) > 0:
        updated_repos = get_profiles(repos_without_community_profile, temp_repo_dir, error_file_path)
        repo_df = pd.concat([repos_with_community_profile, updated_repos])
        repo_df.to_csv(repo_output_path, index=False)
    return repo_df

def make_total_commits_calls(row):
    try:
        url = row['commits_url'].replace('{/sha}', '')
        response = requests.get(f'{url}?per_page=1', headers=auth_headers)
        if response.status_code != 200:
            print('hit rate limiting. trying to sleep...')
            time.sleep(120)
            response = requests.get(url, headers=auth_headers)
            total_commits = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
        else:
            total_commits = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
    except:
        total_commits = 0
    return total_commits

def get_total_commits(repo_df, repos_with_commits_output_path):
    """Function to check if commits have been made to a repo
    :param repo_df: dataframe of repos
    :return: dataframe of repos with total commits
    """
    if 'total_commits' in repo_df.columns:
        repos_without_commits = repo_df[repo_df.total_commits.isna()]
    else:
        if os.path.exists(repos_with_commits_output_path):
            repo_df = pd.read_csv(repos_with_commits_output_path)
            repos_without_commits = repo_df[repo_df.total_commits.isna()]
        else:
            repos_without_commits = repo_df
    
    if len(repos_without_commits) > 0:
        tqdm.pandas(desc="Getting Total Commits")
        repos_without_commits['total_commits'] = repos_without_commits.progress_apply(make_total_commits_calls, axis=1)
        repo_df = pd.concat([repo_df, repos_without_commits])
        repo_df = repo_df.drop_duplicates(subset=['id'])
    
    return repo_df


def clean_owner(row):
    row['cleaned_owner'] = str(dict( ('owner.'+k, v )for k, v in row.owner.items()))
    return row

def get_repo_owners(repo_df, repo_output_path):
    """Function to get repo owners
    :param repo_df: dataframe of repos
    :param repo_output_path: path to save output
    :return: dataframe of repos with owners
    """
    tqdm.pandas(desc="Cleaning Repo Owners")
    repo_df.owner = repo_df.owner.apply(literal_eval)
    repo_df = repo_df.progress_apply(clean_owner, axis=1)
    repo_df = repo_df.drop(columns=['owner'])
    repo_df.cleaned_owner = repo_df.cleaned_owner.apply(literal_eval)
    repo_df = repo_df.drop('cleaned_owner', axis=1).join(pd.DataFrame(repo_df.cleaned_owner.values.tolist()))
    return repo_df


def process_response(response):
    if len(response.links) == 0:
        total_results = len(response.json())
    else:
        total_results = re.search("\d+$", response.links['last']['url']).group()
    return total_results


def get_total_results(response, query):
    """Function to get response data from api call
    :param response: response from api call
    :param query: query used to make api call
    :return: response data"""
    # Check if response is valid
    total_results = 0
    if response.status_code != 200:
        if response.status_code == 401:
            print("response code 401 - unauthorized access. check api key")
        elif response.status_code == 204:
            print(f'No data for {query}')
        else:
            print(
                f'response code: {response.status_code}. hit rate limiting. trying to sleep...')
            time.sleep(60)
            response = requests.get(query, headers=auth_headers)

            # Check if response is valid a second time after sleeping
            if response.status_code != 200:
                print(
                    f'query failed twice with code {response.status_code}. Failing URL: {query}')

                # If failed again, check the rate limit and sleep for the amount of time needed to reset rate limit
                rates_df = check_rate_limit()
                if rates_df['resources.core.remaining'].values[0] == 0:
                    print('rate limit reached. sleeping for 1 hour')
                    time.sleep(3600)
                    response = requests.get(
                        query, headers=auth_headers)
                    if response.status_code != 200:
                        print(
                            f'query failed third time with code {response.status_code}. Failing URL: {query}')
                    else:
                        total_results = process_response(response)
            else:
                total_results = process_response(response)
    else:
        total_results = process_response(response)
    return total_results


def get_repo_counts(row, url_field, overwrite_existing):
    """Function to get total counts for each repo user interaction"""
    url = f"{row[url_field].split('{')[0]}?per_page=1"
    response = requests.get(url, headers=auth_headers)
    total_results = get_total_results(response, url)
    field_name = url_field.split('_')[0]
    row[f'{field_name}_count'] = total_results
    if (row.name == 0) and (overwrite_existing == True):
        pd.DataFrame(row).T.to_csv(
            f'../data/temp/{field_name}_counts.csv', header=True, index=False)
    else:
        pd.DataFrame(row).T.to_csv(f'../data/temp/{field_name}_counts.csv',
                                   mode='a', header=False, index=False)
    return row


def check_total_results(repo_df, url_field, overwrite_existing):
    """Function to check total results for each repo user interaction
    :param repo_df: dataframe of repos
    :return: dataframe of repos with total results"""
    tqdm.pandas(desc="Getting total results for each repo user interaction")
    repo_df = repo_df.reset_index(drop=True)
    repo_df = repo_df.progress_apply(get_repo_counts, axis=1, url_field=url_field, overwrite_existing=overwrite_existing)
    return repo_df

def get_counts(repo_df, url_type, count_type, overwrite_existing_temp_files = False):
        if count_type in repo_df.columns:
            needs_counts = repo_df[repo_df[count_type].isna()]
            has_counts = repo_df[repo_df[count_type].notna()]
        else:
            needs_counts = repo_df
            has_counts = pd.DataFrame()
            
        if len(has_counts) == len(repo_df):
            repo_df = has_counts
        else:
            needs_counts = check_total_results(needs_counts, url_type, overwrite_existing_temp_files)
            repo_df = pd.concat([needs_counts, has_counts])
        return repo_df

if __name__ == "__main__":
    # repo_file_path = "../data/derived_files/initial_core_repos.csv"
    # core_repos = pd.read_csv(repo_file_path)
    # counts_fields = pd.read_csv('../data/metadata_files/repo_url_cols.csv')
    # counts_fields.loc[counts_fields.url_type == 'review_comments_url', 'count_type'] = 'review_count'
    
    
    # skip_types = ['review_comments_url', 'commits_url', 'collaborators_url']
    # overwrite_existing_temp_files = True
    # for index, row in counts_fields.iterrows():
    #     if (row.url_type not in skip_types):
    #         count_type = row.url_type.split("_")[0] + "_count"
    #         print(f"Getting {count_type} for {row['url_type']}")
    #         core_repos = get_counts(core_repos, row['url_type'], count_type, overwrite_existing_temp_files)
    #         core_repos.to_csv(repo_file_path, index=False)
    #         row['count_type'] = count_type
    # core_repos.to_csv(repo_file_path, index=False)
    # pulls_df = pd.read_csv('../data/large_files/join_files/repo_pulls_join_dataset.csv')
    # url_type = "review_comments_url"
    # count_type = "review_count"
    # pulls_df = get_counts(pulls_df, url_type, count_type, overwrite_existing_temp_files=False)
    # pulls_df.to_csv('../data/large_files/join_files/repo_pulls_join_dataset.csv', index=False)
    firstpass_core_repos = pd.read_csv("../data/derived_files/firstpass_core_repos.csv")
   
    # repo_output_path = "../data/derived_files/firstpass_core_repos.csv"
    # error_file_path = "../data/error_logs/repo_profile_errors.csv"
    # temp_repo_dir = "../data/temp/repo_profile/"
    # rates_df = check_rate_limit()
    # core_repos = get_repo_profile(firstpass_core_repos, repo_output_path, rates_df, error_file_path, temp_repo_dir)
    firstpass_core_repos = get_repo_languages(firstpass_core_repos, "../data/derived_files/firstpass_core_repos.csv")
