import apikey
from tqdm import tqdm
from rich.console import Console
import sys
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
sys.path.append("..")
from data_generation_scripts.general_utils import *
from ast import literal_eval
import apikey

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()

def turn_names_into_list(prefix: str, df: pd.DataFrame) -> pd.DataFrame:
    """Function to turn names into list
    :param prefix: prefix to add to each column
    :param df: dataframe
    :return: dataframe with names as a list"""
    return pd.DataFrame([{prefix: df.name.tolist()}])    

def get_repo_metadata(repo_df: pd.DataFrame, error_file_path: str, existing_repo_dir: str, check_column: str, url_column: str):
    """Function to get repo metadata

    :param repo_df: dataframe of repos
    :param existing_repo_dir: path to directory to write existing repos
    :param error_file_path: path to file to write errors
    :param check_column: column to check
    :param url_column: column that contains the url to get the total results
    """
    drop_fields = ["full_name", "error_url"]
    clean_write_error_file(error_file_path, drop_fields)
    if check_column in repo_df.columns:
        repos_without_metadata = repo_df[repo_df[check_column].isna()]
    else: 
        repos_without_metadata = repo_df

    if len(repos_without_metadata) > 0:
        profile_bar = tqdm(total=len(repos_without_metadata), desc="Getting Metadata")
        for _, row in repos_without_metadata.iterrows():
            existing_entities_path = f"{row.full_name.replace('/', '_').replace(' ', '_')}_coding_dh_repo.csv"
            existing_output_path = existing_repo_dir + existing_entities_path
            if os.path.exists(existing_output_path):
                existing_df = pd.read_csv(existing_output_path)
                try:
                    query = row[url_column] + '/community/profile' if 'health_percentage' in check_column else row[url_column]
                    response, status_code = make_request_with_rate_limiting(query, auth_headers)
                    if response is not None:
                        response_df = pd.json_normalize(response)
                        if 'message' in response_df.columns:
                            console.print(response_df.message.values[0], style="bold red")
                            additional_data = {'repo_full_name': row.full_name}
                            log_error_to_file(error_file_path, additional_data, status_code, query)
                            profile_bar.update(1)
                            continue
                        if 'health_percentage' in response_df.columns:
                            response_df = response_df.rename(columns={'updated_at': 'community_profile_updated_at'})
                        elif 'languages' in url_column:
                            # prefix languages to each column
                            response_df = response_df.add_prefix('languages.')
                        else:
                            prefixes = ['tags', 'labels']
                            for prefix in prefixes:
                                if prefix in url_column:
                                    response_df = turn_names_into_list(prefix, response_df)
                        # convert the 'updated_at' column to datetime
                        existing_df['coding_dh_date'] = pd.to_datetime(existing_df['coding_dh_date'])

                        # get the row with the latest date
                        latest_date = existing_df['coding_dh_date'].max()
                        latest_row = existing_df[existing_df['coding_dh_date'] == latest_date]

                        # concatenate the latest row with response_df
                        final_df = pd.concat([latest_row, response_df], axis=1)
                        
                        # drop the latest row from existing_df
                        existing_df = existing_df[existing_df['coding_dh_date'] != latest_date]

                        # concatenate existing_df and final_df
                        result_df = pd.concat([existing_df, final_df])

                        # write result_df to a CSV file
                        result_df.to_csv(existing_output_path, index=False)
                    
                    else:
                        additional_data = {'repo_full_name': row.full_name}
                        log_error_to_file(error_file_path, additional_data, status_code, query)
                        profile_bar.update(1)
                        continue

                except:
                    profile_bar.total = profile_bar.total - 1
                    additional_data = {'repo_full_name': row.full_name}
                    log_error_to_file(error_file_path, additional_data, status_code, query)
                    profile_bar.update(1)
                    continue
            else:
                console.print(f"Repo {row.full_name} does not exist in the coding DH repo directory", style="bold red")

def clean_owner(row: pd.DataFrame) -> pd.DataFrame:
    """Function to clean owner column
    :param row: row from repo_df
    :return: row with cleaned owner column"""
    row['cleaned_owner'] = str(dict( ('owner.'+k, v )for k, v in row.owner.items()))
    return row

def get_repo_owners(repo_df: pd.DataFrame) -> pd.DataFrame:
    """Function to get repo owners
    :param repo_df: dataframe of repos
    :return: dataframe of repos with owners
    """
    tqdm.pandas(desc="Cleaning Repo Owners")
    repo_df.owner = repo_df.owner.apply(literal_eval)
    repo_df = repo_df.progress_apply(clean_owner, axis=1)
    repo_df = repo_df.drop(columns=['owner'])
    repo_df.cleaned_owner = repo_df.cleaned_owner.apply(literal_eval)
    repo_df = repo_df.drop('cleaned_owner', axis=1).join(pd.DataFrame(repo_df.cleaned_owner.values.tolist()))
    return repo_df

def write_entity_results_to_csv(count_column: str, row: pd.DataFrame, entity_type: str, dir_path: str):
    """Function to write results to csv
    
    :param count_column: Column that will store the count values
    :param row: Row with the latest date
    :param entity_type: Type of entity (user or organization or repo)
    :param dir_path: Directory path to existing csv files
    """
    entity_column = "full_name" if entity_type == "repos" else "login"
    entity_name = row[entity_column].replace("/", "_")
    entity_type_singular = entity_type[:-1]
    file_path = os.path.join(dir_path, f"{entity_name}_coding_dh_{entity_type_singular}.csv")
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
    write_entity_results_to_csv(count_column, row, entity_type, dir_path)
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
    if url_column == "owner.organizations_url":
        needs_counts = needs_counts[needs_counts['owner.type'] == 'User']
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
    cols_path = os.path.join(data_directory_path, "metadata_files", f"{entity_type[:-1]}_url_cols.csv")
    if entity_type == "repos":
        cols_df = read_csv_file(cols_path)
        skip_types = ['review_comments_url', 'collaborators_url']
        cols_df = cols_df[~cols_df.url_column.isin(skip_types)]
        entity_df = process_counts(entity_df, cols_df, auth_headers, entity_type, dir_path)
    else:
        if os.path.exists(cols_path):
            cols_df = read_csv_file(cols_path)
        else:
            cols_dict ={'followers': 'followers', 'following': 'following', 'public_repos': 'public_repos', 'public_gists': 'public_gists', 'star_count': 'starred_url', 'subscription_count': 'subscriptions_url', 'organization_count': 'organizations_url'}
            cols_df = pd.DataFrame(cols_dict.items(), columns=['count_column', 'url_column'])
            cols_df.to_csv(cols_path, index=False)
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
    cleaned_terms_path = os.path.join(data_directory_path, "derived_files", "grouped_cleaned_translated_terms.csv")
    cleaned_terms = read_csv_file(cleaned_terms_path, encoding='utf-8-sig')

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