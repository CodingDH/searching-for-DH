from rich import print
from rich.console import Console
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
import os
from typing import Optional, List, Union
import sys
sys.path.append('..')
from data_generation_scripts.utils import *
from data_generation_scripts.generate_user_metadata import check_total_results
from data_generation_scripts.generate_translations import check_detect_language

def get_languages(search_df: pd.DataFrame, search_type: str, is_repo: bool) -> pd.DataFrame:
    """
    Gets the languages for the search queries data.

    :param search_df: The search queries data for repos
    :param search_type: The type of search queries data
    :param is_repo: Whether the search queries data is for repos or users
    :return: The search queries data with the languages added
    """
    tqdm.pandas(desc='Detecting language')
    if 'repo' in search_type:
        search_df.description = search_df.description.fillna('')
    else:
        search_df.bio = search_df.bio.fillna('')
    search_df = search_df.progress_apply(check_detect_language, axis=1, is_repo=is_repo)
    return search_df

def update_finalized_language(df: pd.DataFrame, condition: pd.Series, new_value: Union[str, pd.Series]) -> None:
    """
    Update the 'finalized_language' column in the DataFrame based on a condition.

    This function checks if there are any rows in the DataFrame that meet the condition
    and have a NaN 'finalized_language'. If there are, it updates the 'finalized_language'
    for those rows with the new value.

    Parameters:
    df (pd.DataFrame): The DataFrame to update.
    condition (pd.Series): A boolean Series representing the condition to meet.
    new_value (Union[str, pd.Series]): The new value for 'finalized_language'. Can be a string or a Series.

    Returns:
    None
    """

    # Get the rows that meet the condition and have a NaN 'finalized_language'
    needs_language = df[condition & df.finalized_language.isna()]

    # If there are any such rows, update the 'finalized_language' for those rows
    if len(needs_language) > 0:
        df.loc[condition, 'finalized_language'] = new_value

def use_set_search_cleaning(search_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the search queries data using the SET search cleaning method.

    :param search_df: The search queries data for repos
    :return: The search queries data with the languages cleaned
    """
    english_langs = 'en, ny, ha, ig, lb, mg, sm, sn, st, tl, yo'
    english_langs = english_langs.split(', ')

    update_finalized_language(search_df, search_df.detected_language.isin(english_langs), search_df.detected_language)
    update_finalized_language(search_df, search_df.natural_language == search_df.detected_language, search_df.detected_language)
    update_finalized_language(search_df, (search_df.detected_language.str.contains('zh', na=False)) & (search_df.natural_language == 'zh'), search_df.detected_language)
    update_finalized_language(search_df, (search_df.natural_language.str.contains('fr')) & (search_df.detected_language.str.contains('fr')), 'fr')
    update_finalized_language(search_df, search_df.natural_language == 'xh, zu', search_df.detected_language)
    return search_df

def handle_join_field(search_df: pd.DataFrame, join_field: str, field: str) -> pd.DataFrame:
    """
    Handles the join field for the search queries data.

    :param search_df: The search queries data for repos
    :param join_field: The field to join the search queries data to the repo data
    :param field: The field to use for the join
    :return: The search queries data with the join field handled
    """
    search_df.loc[(search_df.finalized_language.isna()) & (search_df[field].str.len() < 30), 'finalized_language'] = None
    search_df.loc[(search_df.detected_language.isna()) & (search_df[field].isna() & (search_df.finalized_language.isna())), 'finalized_language'] = None
    if join_field == 'full_name':
        search_df.loc[(search_df.detected_language.isna()) & (search_df.description.isna()) & (search_df['size'] < 1) & (search_df.finalized_language.isna()), 'keep_resource'] = False
    return search_df

def clean_languages(search_df: pd.DataFrame, join_field: str, grouped_translated_terms_df: pd.DataFrame, use_defined_search_cleaning: bool=True) -> pd.DataFrame:
    """
    Cleans the languages for the search queries data.

    :param search_df: The search queries data for repos
    :param join_field: The field to join the search queries data to the repo data
    :param grouped_translated_terms_df: Grouped translated terms dataframe
    :param use_defined_search_cleaning: Whether to use the SET search cleaning method
    :return: The search queries data with the languages cleaned"""
    if use_defined_search_cleaning:
        search_df = use_set_search_cleaning(search_df)
    else:
        # check if columns need renaming
        columns_to_rename = ['language_code', 'term', 'term_source']
        if set(columns_to_rename).issubset(grouped_translated_terms_df.columns) == False:
            grouped_translated_terms_df = grouped_translated_terms_df.rename(columns={'language_code': 'natural_language', 'term': 'search_term', 'term_source': 'search_term_source'})
        contains_multiple_languages = grouped_translated_terms_df[grouped_translated_terms_df.search_term.str.contains(',', na=False)]
        if len(contains_multiple_languages) > 0:
            for _, row in contains_multiple_languages.iterrows():
                search_df = update_finalized_language(search_df, (search_df['natural_language'] == row['natural_language']) & (search_df.search_term == row.search_term), row.natural_language)

    search_df.loc[(search_df.finalized_language.isna()) & (search_df.detected_language_confidence < 0.5), 'finalized_language'] = None
    if join_field == 'full_name':
        search_df = handle_join_field(search_df, join_field, 'description')
    if join_field == 'login':
        search_df = handle_join_field(search_df, join_field, 'bio')
    return search_df

def clean_search_queries_data(search_df: object, join_field: str, search_type: str, is_repo: bool, grouped_translated_terms_df: pd.DataFrame, use_set_search_cleaning: bool) -> object:
    """
    Cleans the search queries data and tries to determine as much as possible the exact language using automated language detection and natural language processing.

    :param search_df: The search queries data
    :param join_field: The field to join the search queries data to the repo data
    :param search_type: The type of search queries data
    :param is_repo: Whether the search queries data is for repos or users
    :param grouped_translated_terms_df: Grouped translated terms dataframe
    :param use_set_search_cleaning: Whether to use the SET search cleaning method
    :return: The cleaned search queries data
    """
    search_df = search_df.drop_duplicates(
        subset=[join_field, 'cleaned_search_query'])
    
    if 'keep_resource' not in search_df.columns:
        search_df['keep_resource'] = True
    else:
        search_df.loc[search_df.keep_resource == 'None'] = None

    if 'finalized_language' not in search_df.columns:
        search_df['finalized_language'] = None
    else:
        search_df.loc[search_df.finalized_language == 'None'] = None
    
    if 'detected_language' not in search_df.columns:
        search_df = get_languages(search_df, search_type, is_repo)
        search_df = clean_languages(search_df, join_field, grouped_translated_terms_df, use_set_search_cleaning)
    else:
        subset_search_df = search_df[(search_df.detected_language.isna()) & (search_df.finalized_language.isna())]
        existing_search_df = search_df[(search_df.detected_language.notna()) & (search_df.finalized_language.notna())]
        if len(subset_search_df) > 0:
            subset_search_df = get_languages(subset_search_df, search_type, is_repo)

        search_df = pd.concat([existing_search_df, subset_search_df])
        search_df = clean_languages(search_df, join_field, grouped_translated_terms_df, use_set_search_cleaning)
    return search_df

def fill_missing_language_data(rows: pd.DataFrame, is_repo: bool) -> pd.DataFrame:
    """
    Fills in the missing language data for the search queries data. 

    :param rows: The search queries data
    :param is_repo: Whether the search queries data is for repos or users
    :return: The search queries data with the missing language data filled in
    """
    console = Console()

    if len(rows[rows.finalized_language.notna()]) == 0:
        entity_type = 'Repo' if is_repo else 'User'
        field = 'full_name' if is_repo else 'login'
        console.print(f"No finalized language {len(rows)}, {rows.finalized_language.unique()}, {entity_type} {rows[rows[field].notna()][field].unique()[0]}") 

    detected_languages = rows[rows.detected_language.notnull()].detected_language.unique()
    if len(detected_languages) > 0:
        for i, language in enumerate(detected_languages, start=1):
            console.print(f"{i}: {language}")
        selected_number = console.input("If correct language, press enter. Else please enter the number of the detected language: ")
        if selected_number:
            selected_number = int(selected_number)
            if 1 <= selected_number <= len(detected_languages):
                selected_language = detected_languages[selected_number - 1]
                rows.detected_language = selected_language
                rows.detected_language_confidence = rows[rows.detected_language == selected_language].detected_language_confidence.max()
            else:
                rows.detected_language = detected_languages[0]
                rows.detected_language_confidence = rows[rows.detected_language == detected_languages[0]].detected_language_confidence.max()
    else:
        rows.detected_language = None
        rows.detected_language_confidence = None

    finalized_languages = rows[rows.finalized_language.notna()].finalized_language.unique()
    if len(finalized_languages) > 1:
        # Display each language with a number
        for i, language in enumerate(finalized_languages, start=1):
            console.print(f"{i}: {language}")

        # Ask the user to enter the number of the language
        selected_number = console.input("If correct language, press enter. Else please enter the number of the finalized language: ")

        # If the user entered a number, update the finalized language
        if selected_number:
            selected_number = int(selected_number)
            if 1 <= selected_number <= len(finalized_languages):
                selected_language = finalized_languages[selected_number - 1]
                rows.finalized_language = selected_language
            else:
                rows.finalized_language = finalized_language[0] if len(finalized_language) > 0 else None
    else:
        rows.finalized_language = None

    keep_resource = rows[rows.keep_resource.notna()].keep_resource.unique()
    rows.keep_resource = keep_resource[0] if len(keep_resource) > 0 else None
 
    return rows

def fix_queries(df: pd.DataFrame, query: str, search_term_source: str, id_field: str) -> pd.DataFrame:
    """
    Fixes the queries in a DataFrame.

    :param df: The DataFrame to fix
    :param query: The query to look for
    :param search_term_source: The search term source to look for
    :param id_field: The ID field to use ('full_name' for repos, 'login' for users)
    :return: The DataFrame with the queries fixed
    """
    fix_queries = df[(df.cleaned_search_query.str.contains(query)) & (df.search_term_source == search_term_source)]
    if len(fix_queries) > 0:
        replace_queries = df[(df[id_field].isin(fix_queries[id_field])) & (df.search_term_source == search_term_source)][[id_field, 'search_query']]
        df.loc[df[id_field].isin(fix_queries[id_field]), 'cleaned_search_query'] = df.loc[df[id_field].isin(fix_queries[id_field]), id_field].map(replace_queries.set_index(id_field).to_dict()['search_query'])
    return df

def fix_results(search_queries_repo_df: pd.DataFrame, search_queries_user_df: pd.DataFrame, grouped_translated_terms_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixes the results of the search queries to ensure that the results are correct.

    :param search_queries_repo_df: The search queries data for repos
    :param search_queries_user_df: The search queries data for users
    :return: The fixed search queries data
    """
    # Still need to fix this so it isn't hardcoding search_query and search_term_source, but is instead checking for any disconnect between search_term_source, search_term, and search_query
    search_queries_repo_df = fix_queries(search_queries_repo_df, 'q="Humanities"', "Digital Humanities", 'full_name')
    search_queries_user_df = fix_queries(search_queries_user_df, 'q="Humanities"', "Digital Humanities", 'login')
    return search_queries_repo_df, search_queries_user_df


def verify_results_exist(initial_search_queries_repo_file_path: str, exisiting_search_queries_repo_file_path: str, initial_search_queries_user_file_path: str, existing_search_queries_user_file_path: str, subset_terms: List, grouped_translated_terms_output_path: str) -> pd.DataFrame:
    repo_join_output_path = "search_queries_repo_join_dataset.csv"
    user_join_output_path = "search_queries_user_join_dataset.csv"
    join_unique_field = 'search_query'
    repo_filter_fields = ['full_name', 'cleaned_search_query']
    user_filter_fields = ['login', 'cleaned_search_query']
    grouped_translated_terms_df = read_csv_file(grouped_translated_terms_output_path)
    if (os.path.exists(existing_search_queries_user_file_path)) and (os.path.exists(exisiting_search_queries_repo_file_path)):
        search_queries_user_df = pd.read_csv(existing_search_queries_user_file_path)
        search_queries_repo_df = pd.read_csv(exisiting_search_queries_repo_file_path)
        
        search_queries_user_df['cleaned_search_query'] = search_queries_user_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        search_queries_repo_df['cleaned_search_query'] = search_queries_repo_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        
        updated_search_queries_repo_df = check_for_joins_in_older_queries(repo_join_output_path, search_queries_repo_df, join_unique_field, repo_filter_fields, subset_terms)
        updated_search_queries_user_df = check_for_joins_in_older_queries(user_join_output_path, search_queries_user_df, join_unique_field, user_filter_fields, subset_terms)

        initial_search_queries_repo_df = pd.read_csv(initial_search_queries_repo_file_path)
        initial_search_queries_user_df  = pd.read_csv(initial_search_queries_user_file_path)

        initial_search_queries_user_df['cleaned_search_query'] = initial_search_queries_user_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        initial_search_queries_repo_df['cleaned_search_query'] = initial_search_queries_repo_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]

        initial_search_queries_repo_df = initial_search_queries_repo_df[initial_search_queries_repo_df.search_term_source.isin(subset_terms)]
        initial_search_queries_user_df = initial_search_queries_user_df[initial_search_queries_user_df.search_term_source.isin(subset_terms)]

        
        search_queries_repo_df = pd.concat([updated_search_queries_repo_df, initial_search_queries_repo_df])
        search_queries_user_df = pd.concat([updated_search_queries_user_df, initial_search_queries_user_df])

        tqdm.pandas(desc="Fill missing language data")
        cleaned_search_queries_repo_df = search_queries_repo_df.groupby(['full_name']).progress_apply(fill_missing_language_data, is_repo=True)
        cleaned_search_queries_user_df = search_queries_user_df.groupby(['login']).progress_apply(fill_missing_language_data, is_repo=False)


        cleaned_search_queries_repo_df.loc[cleaned_search_queries_repo_df.search_query_time.isna(), 'search_query_time'] = "2022-10-10"
        cleaned_search_queries_repo_df['search_query_time'] = pd.to_datetime(cleaned_search_queries_repo_df['search_query_time'], errors='coerce')
        cleaned_search_queries_repo_df = cleaned_search_queries_repo_df.sort_values(by=['search_query_time'], ascending=False).drop_duplicates(subset=['full_name', 'cleaned_search_query'], keep='first')

        cleaned_search_queries_user_df.loc[cleaned_search_queries_user_df.search_query_time.isna(), 'search_query_time'] = "2022-10-10"
        cleaned_search_queries_user_df['search_query_time'] = pd.to_datetime(cleaned_search_queries_user_df['search_query_time'], errors='coerce')
        cleaned_search_queries_user_df = cleaned_search_queries_user_df.sort_values(by=['search_query_time'], ascending=False).drop_duplicates(subset=['login','cleaned_search_query'], keep='first')

        cleaned_search_queries_repo_df, cleaned_search_queries_user_df = fix_results(cleaned_search_queries_repo_df, cleaned_search_queries_user_df)
        search_queries_repo_df = clean_search_queries_data(cleaned_search_queries_repo_df, 'full_name', 'repo')
        search_queries_user_df = clean_search_queries_data(cleaned_search_queries_user_df, 'login', 'user')
    else:
        initial_search_queries_repo_df = pd.read_csv(initial_search_queries_repo_file_path)
        initial_search_queries_user_df  = pd.read_csv(initial_search_queries_user_df)

        initial_search_queries_user_df['cleaned_search_query'] = initial_search_queries_user_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        initial_search_queries_repo_df['cleaned_search_query'] = initial_search_queries_repo_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        
        search_queries_repo_df = check_for_joins_in_older_queries(repo_join_output_path, initial_search_queries_repo_df, join_unique_field, repo_filter_fields, subset_terms)
        search_queries_user_df = check_for_joins_in_older_queries(user_join_output_path, initial_search_queries_user_df, join_unique_field, user_filter_fields, subset_terms)

        tqdm.pandas(desc="Fill missing language data")
        search_queries_repo_df = search_queries_repo_df.groupby(['full_name']).progress_apply(fill_missing_language_data, is_repo=True)
        search_queries_user_df = search_queries_user_df.groupby(['login']).progress_apply(fill_missing_language_data, is_repo=False)
        
        search_queries_repo_df, search_queries_user_df = fix_results(search_queries_repo_df, search_queries_user_df)
        search_queries_repo_df = clean_search_queries_data(search_queries_repo_df, 'full_name', 'repo')
        search_queries_user_df = clean_search_queries_data(search_queries_user_df, 'login', 'user')
    search_queries_repo_df = search_queries_repo_df.drop_duplicates(subset=['full_name', 'cleaned_search_query'])
    search_queries_user_df = search_queries_user_df.drop_duplicates(subset=['login', 'cleaned_search_query'])
    return search_queries_repo_df, search_queries_user_df


subset_terms = ["Digital Humanities"]
console = Console()
initial_repo_output_path = "../data/repo_data/"
repo_output_path = "../data/large_files/entity_files/repos_dataset.csv"
initial_repo_join_output_path = "../data/large_files/join_files/search_queries_repo_join_dataset.csv"
repo_join_output_path = "../data/derived_files/updated_search_queries_repo_join_subset_dh_dataset.csv"

initial_user_output_path = "../data/user_data/"
user_output_path = "../data/entity_files/users_dataset.csv"
org_output_path = "../data/entity_files/orgs_dataset.csv"
initial_user_join_output_path = "../data/join_files/search_queries_user_join_dataset.csv"
user_join_output_path = "../data/derived_files/updated_search_queries_user_join_subset_dh_dataset.csv"


# search_queries_repo_df, search_queries_user_df = verify_results_exist(initial_repo_join_output_path, repo_join_output_path, initial_user_join_output_path, user_join_output_path, subset_terms)

# search_queries_repo_df.to_csv("../data/derived_files/initial_search_queries_repo_join_subset_dh_dataset.csv", index=False)
# search_queries_user_df.to_csv("../data/derived_files/initial_search_queries_user_join_subset_dh_dataset.csv", index=False)

search_queries_repo_df = pd.read_csv("../data/derived_files/initial_search_queries_repo_join_subset_dh_dataset.csv")
search_queries_user_df = pd.read_csv("../data/derived_files/initial_search_queries_user_join_subset_dh_dataset.csv")

needs_checking = search_queries_repo_df[(search_queries_repo_df.finalized_language.isna()) & ((search_queries_repo_df.keep_resource.isna()) | (search_queries_repo_df.keep_resource == True))]

if os.path.exists(repo_join_output_path):
    existing_search_queries_repo_df = pd.read_csv(repo_join_output_path)
    needs_checking = existing_search_queries_repo_df[(existing_search_queries_repo_df.full_name.isin(needs_checking.full_name)) & (existing_search_queries_repo_df.finalized_language.isna())]
    if len(needs_checking) > 0:
        search_queries_repo_df = pd.concat([existing_search_queries_repo_df, needs_checking])
    else:
        search_queries_repo_df = existing_search_queries_repo_df

needs_checking_repos = search_queries_repo_df[(search_queries_repo_df['finalized_language'].isna())].full_name.unique().tolist()
search_queries_repo_df.loc[search_queries_repo_df.detected_language.isna(), 'detected_language'] = None
search_queries_repo_df.loc[search_queries_repo_df.natural_language.isna(), 'natural_language'] = None
search_queries_repo_df = search_queries_repo_df.reset_index(drop=True)

for index, repo in enumerate(needs_checking_repos):
    all_rows = search_queries_repo_df[(search_queries_repo_df['full_name'] == repo)]
    print(f"On {index} out of {len(needs_checking_repos)}")
    print(f"This repo {all_rows.full_name.unique()} ")
    print(f"Repo URL: {all_rows.html_url.unique()}")
    print(f"Repo Description: {all_rows.description.unique()}")
    print(f"Repo Natural Language: {all_rows.natural_language.unique()}")
    print(f"Repo Detected Language: {all_rows.detected_language.unique()}")
    print(f"Repo Search Query: {all_rows.search_query.unique()}")
    print(f"Repo Search Query Term: {all_rows.search_term.unique()}")
    print(f"Repo Search Query Source Term: {all_rows.search_term_source.unique()}")
    # Input answer
    keep_resource = True
    answer = console.input("stay in the dataset? (y/n)")
    if answer == 'n':
        keep_resource = False

    detected_languages = all_rows[all_rows.detected_language.notna()].detected_language.unique().tolist()
    natural_languages = all_rows[all_rows.natural_language.notna()].natural_language.unique().tolist()

    detected_languages = detected_languages[0] if len(detected_languages) == 1 else str(detected_languages).replace('[', '').replace(']', '')
    natural_languages = natural_languages[0] if len(natural_languages) == 1 else str(natural_languages).replace('[', '').replace(']', '')
    potential_language = detected_languages if len(detected_languages) != 0 else natural_languages
    potential_language = potential_language if len(potential_language) != 0 else 'None'

    if ',' in potential_language:
        if 'fr' in potential_language:
            potential_language = 'fr'
        elif 'en' in potential_language:
            potential_language = 'en'
        elif 'xh' in potential_language:
            potential_language = 'en'

    language_answers = console.input(
        f"Is the finalized language: [bold blue] {potential_language} [/] of this repo correct? ")
    finalized_language = None
    if language_answers != 'n':
        finalized_language = potential_language
    if language_answers == 'n':
        final_language = console.input("What is the correct language? ")
        finalized_language = final_language
    search_queries_repo_df.loc[(search_queries_repo_df.full_name == repo), 'keep_resource'] = keep_resource
    search_queries_repo_df.loc[(search_queries_repo_df.full_name == repo), 'finalized_language'] = finalized_language
    search_queries_repo_df.to_csv(repo_join_output_path, index=False)
    print(u'\u2500' * 10)

subset_search_df = search_queries_repo_df.drop_duplicates(
    subset=['full_name', 'finalized_language'])
double_check = subset_search_df.full_name.value_counts().reset_index().rename(
    columns={'index': 'full_name', 'full_name': 'count'}).sort_values('count', ascending=False)
double_check = double_check[double_check['count'] > 1]
for index, row in tqdm(double_check.iterrows(), total=len(double_check), desc="Double Checking Repos"):
    needs_updating = search_queries_repo_df[search_queries_repo_df.full_name == row.full_name]
    unique_detected_languages = needs_updating.detected_language.unique().tolist()
    if len(unique_detected_languages) > 1:
        print(f"Repo {row.full_name}")
        print(f"Repo URL: {needs_updating.html_url.unique()}")
        print(f"Repo Description: {needs_updating.description.unique()}")
        print(f"Repo Natural Language: {needs_updating.natural_language.tolist()}")
        print(f"Repo Detected Language: {needs_updating.detected_language.tolist()}")
        print(f"Repo Search Query: {needs_updating.search_query.unique()}")
        print(f"Repo Search Query Term: {needs_updating.search_term.unique()}")
        print(f"Repo Search Query Source Term: {needs_updating.search_term_source.unique()}")
        print(f"Repo Finalized Language: {needs_updating.finalized_language.tolist()}")
        final_language = console.input("What is the correct language? ")
        search_queries_repo_df.loc[(search_queries_repo_df.full_name == row.full_name), 'finalized_language'] = final_language
        search_queries_repo_df.to_csv(repo_join_output_path, index=False)
        print(u'\u2500' * 10)
    else:
        search_queries_repo_df.loc[(search_queries_repo_df.full_name == row.full_name), 'finalized_language'] = unique_detected_languages[0]
        search_queries_repo_df.to_csv(repo_join_output_path, index=False)

# CHECK USER

needs_checking = search_queries_user_df[(search_queries_user_df.finalized_language.isna()) & ((search_queries_user_df.keep_resource.isna()) | (search_queries_user_df.keep_resource == True))]

if os.path.exists(user_join_output_path):
    existing_search_queries_user_df = pd.read_csv(user_join_output_path)
    needs_checking = existing_search_queries_user_df[(existing_search_queries_user_df.login.isin(needs_checking.login)) & (existing_search_queries_user_df.finalized_language.isna())]
    if len(needs_checking) > 0:
        search_queries_user_df = pd.concat([existing_search_queries_user_df, needs_checking])
    else:
        search_queries_user_df = existing_search_queries_user_df

needs_checking_users = search_queries_user_df[(search_queries_user_df['finalized_language'].isna())].login.unique().tolist()
search_queries_user_df.loc[search_queries_user_df.detected_language.isna(), 'detected_language'] = None
search_queries_user_df.loc[search_queries_user_df.natural_language.isna(), 'natural_language'] = None
search_queries_user_df = search_queries_user_df.reset_index(drop=True)


for index, user in enumerate(needs_checking_users):
    all_rows = search_queries_user_df[(
        search_queries_user_df['login'] == user)]
    print(f"On {index} out of {len(needs_checking_users)}")
    print(f"This user {all_rows.login.unique()} ")
    print(f"User URL: {all_rows.html_url.unique()}")
    print(f"User Type: {all_rows.type.unique()}")
    print(f"User Bio: {all_rows.bio.unique()}")
    print(f"User Location: {all_rows.location.unique()}")
    print(f"User Natural Language: {all_rows.natural_language.unique()}")
    print(f"User Detected Language: {all_rows.detected_language.unique()}")
    print(f"User Search Query: {all_rows.search_query.unique()}")
    print(f"User Search Query Term: {all_rows.search_term.unique()}")
    print(
        f"User Search Query Source Term: {all_rows.search_term_source.unique()}")
    # Input answer
    answer = console.input("stay in the dataset? (y/n)")
    keep_resource = True
    if answer == 'n':
        keep_resource = False

    detected_languages = all_rows[all_rows.detected_language.notna(
    )].detected_language.unique().tolist()
    natural_languages = all_rows[all_rows.natural_language.notna(
    )].natural_language.unique().tolist()
    detected_languages = detected_languages[0] if len(detected_languages) == 1 else str(detected_languages).replace('[', '').replace(']', '')
    natural_languages = natural_languages[0] if len(natural_languages) == 1 else str(
        natural_languages).replace('[', '').replace(']', '')
    potential_language = detected_languages if len(
        detected_languages) != 0 else natural_languages
    potential_language = potential_language if len(
        potential_language) != 0 else 'None'
    if ',' in potential_language:
        if 'fr' in potential_language:
            potential_language = 'fr'
        elif 'en' in potential_language:
            potential_language = 'en'
        elif 'xh' in potential_language:
            potential_language = 'en'

    language_answers = console.input(
        f"Is the finalized language: [bold blue] {potential_language} [/] of this user correct? ")
    finalized_language = None
    if language_answers != 'n':
        finalized_language = potential_language
    if language_answers == 'n':
        final_language = console.input("What is the correct language? ")
        finalized_language = final_language
    search_queries_user_df.loc[(
        search_queries_user_df.login == user), 'keep_resource'] = keep_resource
    search_queries_user_df.loc[(search_queries_user_df.login == user), 'finalized_language'] = finalized_language
    search_queries_user_df.to_csv(user_join_output_path, index=False)
    print(u'\u2500' * 10)

subset_search_df = search_queries_user_df.drop_duplicates(
    subset=['login', 'finalized_language'])
double_check = subset_search_df.login.value_counts().reset_index().rename(
    columns={'index': 'login', 'login': 'count'}).sort_values('count', ascending=False)
double_check = double_check[double_check['count'] > 1]
for index, row in tqdm(double_check.iterrows(), total=len(double_check), desc="Double Checking Repos"):
    needs_updating = search_queries_user_df[search_queries_user_df.login == row.login]
    unique_detected_languages = needs_updating.detected_language.unique().tolist()
    if len(unique_detected_languages) > 1:
        print(f"User {row.login}")
        print(f"User URL: {needs_updating.html_url.unique()}")
        print(f"User Bio: {needs_updating.bio.unique()}")
        print(f"User Natural Language: {needs_updating.natural_language.tolist()}")
        print(
            f"User Detected Language: {needs_updating.detected_language.tolist()}")
        print(f"User Search Query: {needs_updating.search_query.unique()}")
        print(f"User Search Query Term: {needs_updating.search_term.unique()}")
        print(
            f"User Search Query Source Term: {needs_updating.search_term_source.unique()}")
        print(
            f"User Finalized Language: {needs_updating.finalized_language.tolist()}")
        final_language = console.input("What is the correct language? ")
        search_queries_user_df.loc[(search_queries_user_df.login ==
                                    row.login), 'finalized_language'] = final_language
        search_queries_user_df.to_csv(user_join_output_path, index=False)
        print(u'\u2500' * 10)
    else:
        search_queries_user_df.loc[(search_queries_user_df.login ==
                                    row.login), 'finalized_language'] = unique_detected_languages[0]
        search_queries_user_df.to_csv(user_join_output_path, index=False)

search_queries_repo_df.to_csv(repo_join_output_path, index=False)
search_queries_user_df.to_csv(user_join_output_path, index=False)