import apikey
from tqdm import tqdm
from rich.console import Console
import sys
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
sys.path.append("..")
from data_generation_scripts.general_utils import read_combine_files, check_total_pages, read_csv_file, get_data_directory_path, create_queries_directories

console = Console()

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
    file_path = f"{dir_path}{entity_name}_coding_dh_{entity_type_singular}.csv"
    console.print(f"Writing to {file_path}", style="bold green")
    if os.path.exists(file_path):
        df = read_csv_file(file_path)
        df['coding_dh_date'] = pd.to_datetime(df['coding_dh_date'])
        # get the row with the latest date
        latest_date = df['coding_dh_date'].max()
        # update the count_column for the latest date
        df.loc[df['coding_dh_date'] == latest_date, count_column] = row[count_column]
        df.to_csv(file_path, index=False)

def get_results(row: pd.DataFrame, count_column: str, url_column: str, auth_headers: dict, entity_type: str, dir_path: str, check_state: bool) -> pd.DataFrame:
    """Function to get total results for each user or organization
    
    :param row: Row with the latest date
    :param count_column: Column that will store the count values
    :param url_column: Column that contains the url to get the total results
    :param auth_headers: Authorization headers
    :param entity_type: Type of entity (user or organization or repo)
    :param dir_path: Directory path to existing csv files
    :param check_state: Boolean to check if the state is all
    :return: Row with the total results"""
    console.print(f"Getting total results for {row[url_column]}", style="bold green")
    url = f"{row[url_column].split('{')[0]}"
    if check_state:
        url = f"{url}?state=all"
    total_results = check_total_pages(url, auth_headers)
    console.print(f"Total results for {url}: {total_results}", style="bold green")
    row[count_column] = total_results
    write_results_to_csv(count_column, row, entity_type, dir_path)
    return row

def get_counts(df: pd.DataFrame, url_column: str, count_column: str, entity_type: str, dir_path: str, check_state: bool, auth_headers: dict=None) -> pd.DataFrame:
    """Function to get total results for each user or organization

    :param df: DataFrame with user or organization data
    :param url_column: Column that contains the url to get the total results
    :param count_column: Column that will store the count values
    :param entity_type: Type of entity (user or organization or repo)
    :param dir_path: Directory path to existing csv files
    :param check_state: Boolean to check if the state is all
    :param auth_headers: Authorization headers
    :return: DataFrame with the total results"""
    if count_column in df.columns:
        needs_counts = df[df[count_column].isna()]
        has_counts = df[df[count_column].notna()]
    else:
        needs_counts = df
        has_counts = pd.DataFrame()
    console.print(f"For {entity_type}, {len(needs_counts)} {count_column} need to be processed versus {len(has_counts)} has already been processed", style="bold blue")
    if len(has_counts) == len(df):
        df = has_counts
    else:
        tqdm.pandas(desc=f"Getting total results for each {entity_type}'s {count_column}")
        processed_needs_counts = needs_counts.reset_index(drop=True)
        processed_needs_counts = processed_needs_counts.progress_apply(get_results, axis=1, count_column=count_column, url_column=url_column,  auth_headers=auth_headers, entity_type=entity_type, dir_path=dir_path, check_state=check_state)
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
    check_state = False
    for _, row in cols_df.iterrows():
        console.print(f'Processing {row.count_column} for {entity_type}', style="bold blue")
        if (row['count_column'] not in df.columns) or (len(df[df[row.count_column].isna()]) > 0):
            if 'url' in row.url_column:
                try:
                    if entity_type == "repos":
                        check_state = row['check_state']
                    df = get_counts(df, row.url_column, row.count_column, entity_type, dir_path, check_state, auth_headers=auth_headers)
                except Exception as e:
                    console.print(f'Issues with {row.count_column} for {entity_type} with error: {e}', style='bold red')
                    continue
            else:
                console.print(f'Counts already exist {row.count_column} for {entity_type}')
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
    data_directory_path = get_data_directory_path()
    if entity_type == "repos":
        cols_df = read_csv_file(f"{data_directory_path}/metadata_files/repo_url_cols.csv")
        skip_types = ['review_comments_url', 'commits_url', 'collaborators_url']
        cols_df = cols_df[~cols_df.url_column.isin(skip_types)]
        entity_df = process_counts(entity_df, cols_df, auth_headers, entity_type, dir_path)
    else:
        if os.path.exists(f"{data_directory_path}/metadata_files/user_url_cols.csv"):
            cols_df = read_csv_file(f"{data_directory_path}/metadata_files/user_url_cols.csv")
        else:
            cols_dict ={'followers': 'followers', 'following': 'following', 'public_repos': 'public_repos', 'public_gists': 'public_gists', 'star_count': 'starred_url', 'subscription_count': 'subscriptions_url', 'organization_count': 'organizations_url'}
            cols_df = pd.DataFrame(cols_dict.items(), columns=['count_column', 'url_column'])
            cols_df.to_csv(f"{data_directory_path}/metadata_files/user_url_cols.csv", index=False)
        if entity_type  == "orgs":
            console.print(f'Processing Orgs', style="bold blue")
            add_cols = pd.DataFrame({'count_column': ['members_count'], 'url_column': ['members_url']})
            cols_df = pd.concat([cols_df, add_cols])
            entity_df["members_url"] = entity_df["url"].apply(lambda x: x + "/public_members")
            entity_df.members_url = entity_df.members_url.str.replace('users', 'orgs')
        entity_df = process_counts(entity_df, cols_df, auth_headers, entity_type, dir_path)
    return entity_df
            

if __name__ == "__main__":

    data_directory_path = get_data_directory_path() 
    target_terms: list = ["Public History", "Digital History", "Digital Cultural Heritage", "Cultural Analytics", "Computational Humanities", "Computational Social Science", "Digital Humanities"]

    # Load in the translated terms
    cleaned_terms = pd.read_csv(f'{data_directory_path}/derived_files/grouped_cleaned_translated_terms.csv', encoding='utf-8-sig')

    if 'keep_term' in cleaned_terms.columns:
        cleaned_terms = cleaned_terms[cleaned_terms.keep_term == True]
    # check if columns need renaming
    columns_to_rename = ['code', 'term', 'term_source']
    if all(elem in cleaned_terms.columns for elem in columns_to_rename):
        cleaned_terms = cleaned_terms.rename(columns={'code': 'natural_language', 'term': 'search_term', 'term_source': 'search_term_source'})
    cleaned_terms = cleaned_terms[cleaned_terms.search_term_source.isin(target_terms)]
    cleaned_terms = cleaned_terms.reset_index(drop=True)

    cleaned_terms.loc[cleaned_terms.search_term.str.contains("&#39;"), "search_term"] = cleaned_terms.search_term.str.replace("&#39;", "'")
    cleaned_terms['lower_search_term'] = cleaned_terms.search_term.str.lower()

    search_user_queries_df = create_queries_directories("user", cleaned_terms)
    search_org_queries_df = search_user_queries_df[search_user_queries_df['type'] == 'Organization']
    search_org_queries_df = search_org_queries_df[search_org_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]
    search_user_queries_df = search_user_queries_df[search_user_queries_df['type'] == 'User']
    search_user_queries_df = search_user_queries_df[search_user_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]
    search_repo_queries_df = create_queries_directories("repo", cleaned_terms)
    search_repo_queries_df = search_repo_queries_df[search_repo_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]


    user_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_users/")
    org_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_orgs/")
    repo_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_repos/")
    cleaned_user_files = [f.split("_coding_dh_")[0] for f in user_files if f.endswith(".csv")]
    cleaned_org_files = [f.split("_coding_dh_")[0] for f in org_files if f.endswith(".csv")]
    cleaned_repo_files = [f.split("_coding_dh_")[0].replace("_", "/", 1) for f in repo_files if f.endswith(".csv")]
    existing_search_user_queries_df = search_user_queries_df[search_user_queries_df.login.isin(cleaned_user_files)]
    existing_search_org_queries_df = search_org_queries_df[search_org_queries_df.login.isin(cleaned_org_files)]
    existing_search_repo_queries_df = search_repo_queries_df[search_repo_queries_df.full_name.isin(cleaned_repo_files)]
    finalized_user_logins = existing_search_user_queries_df.login.unique().tolist()
    finalized_org_logins = existing_search_org_queries_df.login.unique().tolist()
    finalized_repo_full_names = existing_search_repo_queries_df.full_name.unique().tolist()

    finalized_user_files = [f"{login}_coding_dh_user.csv" for login in finalized_user_logins]
    finalized_org_files = [f"{login}_coding_dh_org.csv" for login in finalized_org_logins]
    finalized_repo_files = [f"{full_name.replace('/', '_')}_coding_dh_repo.csv" for full_name in finalized_repo_full_names]
    initial_core_users = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_users/", finalized_user_files)
    initial_core_orgs = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_orgs/", finalized_org_files)
    initial_core_repos = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_repos/", finalized_repo_files)
    # get_count_metadata(entity_df: pd.DataFrame, entity_type: str, dir_path: str)
    error_file_path = f"{data_directory_path}/error_logs/repo_errors.csv"
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        subset_core_repos = initial_core_repos[~initial_core_repos.full_name.isin(error_df.full_name)]
    else:
        subset_core_repos = initial_core_repos
    subset_core_repos = get_count_metadata(subset_core_repos, "repos", f"{data_directory_path}/historic_data/entity_files/all_repos/")