import os
import sys
import warnings
from typing import List, Union
import urllib.parse

import pandas as pd
from rich import print
from rich.console import Console
from tqdm import tqdm

sys.path.append('..')
from data_generation_scripts.general_utils import *
from data_generation_scripts.generate_translations import check_detect_language

warnings.filterwarnings('ignore')

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
    Cleans the search queries data using the SET search cleaning method. Older function.

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

def auto_set_language_resource(search_df: pd.DataFrame, join_field: str, field: str) -> pd.DataFrame:
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
        search_df = auto_set_language_resource(search_df, join_field, 'description')
    if join_field == 'login':
        search_df = auto_set_language_resource(search_df, join_field, 'bio')
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

def older_fix_results(search_queries_repo_df: pd.DataFrame, search_queries_user_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixes the results of the search queries to ensure that the results are correct.

    :param search_queries_repo_df: The search queries data for repos
    :param search_queries_user_df: The search queries data for users
    :return: The fixed search queries data
    """

    fix_repo_queries = search_queries_repo_df[(search_queries_repo_df.cleaned_search_query.str.contains('q="Humanities"')) & (search_queries_repo_df.search_term_source == "Digital Humanities")]
    fix_user_queries = search_queries_user_df[(search_queries_user_df.cleaned_search_query.str.contains('q="Humanities"')) & (search_queries_user_df.search_term_source == "Digital Humanities")]
    if len(fix_repo_queries) > 0:
        replace_repo_queries = search_queries_repo_df[(search_queries_repo_df.full_name.isin(fix_repo_queries.full_name)) & (search_queries_repo_df.search_term_source == "Digital Humanities")][['full_name', 'search_query']]
        search_queries_repo_df.loc[search_queries_repo_df.full_name.isin(fix_repo_queries.full_name), 'cleaned_search_query'] = search_queries_repo_df.loc[search_queries_repo_df.full_name.isin(fix_repo_queries.full_name), 'full_name'].map(replace_repo_queries.set_index('full_name').to_dict()['search_query'])
        
    if len(fix_user_queries) > 0:
        replace_user_queries = search_queries_user_df[(search_queries_user_df.full_name.isin(fix_user_queries.login)) & (search_queries_user_df.search_term_source == "Digital Humanities")][['login', 'search_query']]
        search_queries_user_df.loc[search_queries_user_df.login.isin(fix_user_queries.login), 'cleaned_search_query'] = search_queries_user_df.loc[search_queries_user_df.login.isin(fix_user_queries.login), 'login'].map(replace_user_queries.set_index('login').to_dict()['search_query'])
    return search_queries_repo_df, search_queries_user_df

def fix_queries(df: pd.DataFrame, entity_type: str, id_field: str) -> pd.DataFrame:
    """
    Fixes the queries in a DataFrame.

    :param df: The DataFrame to fix
    :param query: The query to look for
    :param search_term_source: The search term source to look for
    :param id_field: The ID field to use ('full_name' for repos, 'login' for users)
    :return: The DataFrame with the queries fixed
    """
    needs_fixing = df[df.search_type == "searched"]
    already_fixed = df[df.search_type == "tagged"]
    to_fix = needs_fixing['extracted_search_term'] = needs_fixing['search_query'].apply(lambda url: urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get('q', [None])[0] if isinstance(url, str) else None)
    to_fix = to_fix['extracted_search_term'] = to_fix['extracted_search_term'].str.replace('+', ' ').str.replace('%22', '"').str.replace('%27', "'").str.replace('"', '').str.split('created').str[0].str.strip()
    fix_queries = to_fix[to_fix.search_term != to_fix.extracted_search_term]
    if len(fix_queries) > 0:
        for index, row in fix_queries:
            console.print(f"{entity_type} {row[id_field]}")
            console.print(f"Current disconnect between search_term and extracted_search_ter: {row.search_term} and {row.extracted_search_term}")
            answer = console.input("Would you like to fix this? (y/n)")
            if answer == 'y':
                corrected_search_term = console.input("What is the correct search term? ")
                needs_fixing.loc[(needs_fixing[id_field] == row[id_field]) & (needs_fixing.index == index), 'search_term'] = corrected_search_term
                corrected_search_query = console.input("What is the correct search query? ")
                needs_fixing.loc[(needs_fixing[id_field] == row[id_field]) & (needs_fixing.index == index), 'search_query'] = corrected_search_query
            else:
                console.print("Skipping...")
    final_fixed_results = pd.concat([already_fixed, needs_fixing], ignore_index=True)
    return final_fixed_results

def fix_results(search_queries_repo_df: pd.DataFrame, search_queries_user_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fixes the results of the search queries to ensure that the results are correct.

    :param search_queries_repo_df: The search queries data for repos
    :param search_queries_user_df: The search queries data for users
    :return: The fixed search queries data
    """
    search_queries_repo_df = fix_queries(search_queries_repo_df, 'full_name')
    search_queries_user_df = fix_queries(search_queries_user_df, 'login')
    return search_queries_repo_df, search_queries_user_df


def verify_results_exist(initial_search_queries_repo_file_path: str, exisiting_search_queries_repo_file_path: str, initial_search_queries_user_file_path: str, existing_search_queries_user_file_path: str, subset_terms: List, grouped_translated_terms_output_path: str, use_set_search_cleaning: bool=True) -> pd.DataFrame:
    """
    Verifies that the results of the search queries exist.

    :param initial_search_queries_repo_file_path: The path to the initial search queries data for repos
    :param exisiting_search_queries_repo_file_path: The path to the existing search queries data for repos
    :param initial_search_queries_user_file_path: The path to the initial search queries data for users
    :param existing_search_queries_user_file_path: The path to the existing search queries data for users
    :param subset_terms: The subset of terms to use
    :param grouped_translated_terms_output_path: The path to the grouped translated terms data
    :param use_set_search_cleaning: Whether to use the SET search cleaning method
    :return: The search queries data for repos and users
    """
    repo_join_output_path = "search_queries_repo_join_dataset.csv"
    user_join_output_path = "search_queries_user_join_dataset.csv"
    join_unique_field = 'search_query'
    repo_filter_fields = ['full_name', 'cleaned_search_query']
    user_filter_fields = ['login', 'cleaned_search_query']
    grouped_translated_terms_df = read_csv_file(grouped_translated_terms_output_path)
    if (os.path.exists(existing_search_queries_user_file_path)) and (os.path.exists(exisiting_search_queries_repo_file_path)):
        search_queries_user_df = read_csv_file(existing_search_queries_user_file_path)
        search_queries_repo_df = read_csv_file(exisiting_search_queries_repo_file_path)
        
        if 'cleaned_search_query' not in search_queries_user_df.columns:
            search_queries_user_df['cleaned_search_query'] = search_queries_user_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        if 'cleaned_search_query' not in search_queries_repo_df.columns:
            search_queries_repo_df['cleaned_search_query'] = search_queries_repo_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        
        is_large = False
        updated_search_queries_repo_df = check_for_joins_in_older_queries(repo_join_output_path, search_queries_repo_df, join_unique_field, repo_filter_fields, subset_terms, is_large)
        updated_search_queries_user_df = check_for_joins_in_older_queries(user_join_output_path, search_queries_user_df, join_unique_field, user_filter_fields, subset_terms, is_large)

        initial_search_queries_repo_df = read_csv_file(initial_search_queries_repo_file_path)
        initial_search_queries_user_df  = read_csv_file(initial_search_queries_user_file_path)

        if 'cleaned_search_query' not in initial_search_queries_user_df.columns:
            initial_search_queries_user_df['cleaned_search_query'] = initial_search_queries_user_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        if 'cleaned_search_query' not in initial_search_queries_repo_df.columns:
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
        if use_set_search_cleaning:
            cleaned_search_queries_repo_df, cleaned_search_queries_user_df = fix_results(cleaned_search_queries_repo_df, cleaned_search_queries_user_df)
        search_queries_repo_df = clean_search_queries_data(cleaned_search_queries_repo_df, 'full_name', 'repo', True, grouped_translated_terms_df, use_set_search_cleaning)
        search_queries_user_df = clean_search_queries_data(cleaned_search_queries_user_df, 'login', 'user', False, grouped_translated_terms_df, use_set_search_cleaning)
    else:
        initial_search_queries_repo_df = read_csv_file(initial_search_queries_repo_file_path)
        initial_search_queries_user_df  = read_csv_file(initial_search_queries_user_df)

        if 'cleaned_search_query' not in initial_search_queries_user_df.columns:
            initial_search_queries_user_df['cleaned_search_query'] = initial_search_queries_user_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]
        
        if 'cleaned_search_query' not in initial_search_queries_repo_df.columns:
            initial_search_queries_repo_df['cleaned_search_query'] = initial_search_queries_repo_df['search_query'].str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]

        is_large = False
        search_queries_repo_df = check_for_joins_in_older_queries(repo_join_output_path, initial_search_queries_repo_df, join_unique_field, repo_filter_fields, subset_terms, is_large)
        search_queries_user_df = check_for_joins_in_older_queries(user_join_output_path, initial_search_queries_user_df, join_unique_field, user_filter_fields, subset_terms, is_large)

        tqdm.pandas(desc="Fill missing language data")
        search_queries_repo_df = search_queries_repo_df.groupby(['full_name']).progress_apply(fill_missing_language_data, is_repo=True)
        search_queries_user_df = search_queries_user_df.groupby(['login']).progress_apply(fill_missing_language_data, is_repo=False)
        
        if use_set_search_cleaning:
            search_queries_repo_df, search_queries_user_df = fix_results(search_queries_repo_df, search_queries_user_df)
        search_queries_repo_df = clean_search_queries_data(search_queries_repo_df, 'full_name', 'repo', True, grouped_translated_terms_df, use_set_search_cleaning)
        search_queries_user_df = clean_search_queries_data(search_queries_user_df, 'login', 'user', False, grouped_translated_terms_df, use_set_search_cleaning)
    search_queries_repo_df = search_queries_repo_df.drop_duplicates(subset=['full_name', 'cleaned_search_query'])
    search_queries_user_df = search_queries_user_df.drop_duplicates(subset=['login', 'cleaned_search_query'])
    return search_queries_repo_df, search_queries_user_df

def generate_needs_checking(df: pd.DataFrame, join_output_path: str, id_field: str) -> pd.DataFrame:
    """
    Generates the needs checking data that has previously been cleaned.

    :param df: The DataFrame to generate the needs checking data from
    :param join_output_path: The path to the join output file
    :param id_field: The ID field to use ('full_name' for repos, 'login' for users)
    :return: The needs checking data
    """
    needs_checking = df[(df.finalized_language.isna()) & ((df.keep_resource.isna()) | (df.keep_resource == True))]

    if os.path.exists(join_output_path):
        existing_df = read_csv_file(join_output_path)
        needs_checking = existing_df[(existing_df[id_field].isin(needs_checking[id_field])) & (existing_df.finalized_language.isna())]
        if len(needs_checking) > 0:
            df = pd.concat([existing_df, needs_checking])
        else:
            df = existing_df

    needs_checking_ids = df[(df['finalized_language'].isna())][id_field].unique().tolist()
    df.loc[df.detected_language.isna(), 'detected_language'] = None
    df.loc[df.natural_language.isna(), 'natural_language'] = None
    df = df.reset_index(drop=True)

    return df, needs_checking_ids

def process_needs_checking(df: pd.DataFrame, needs_checking_ids: List[str], id_field: str, join_output_path: str, print_fields: List[str]) -> pd.DataFrame:
    """
    Processes the needs checking data.

    :param df: The DataFrame to process
    :param needs_checking_ids: The IDs to process
    :param id_field: The ID field to use ('full_name' for repos, 'login' for users)
    :param join_output_path: The path to the join output file
    :param print_fields: The fields to print
    :return: The processed DataFrame
    """
    entity_type = 'Repo' if id_field == 'full_name' else 'User'
    for index, id in enumerate(needs_checking_ids):
        all_rows = df[(df[id_field] == id)]
        print(f"On {index} out of {len(needs_checking_ids)}")
        print(f"This {entity_type.capitalize()} {all_rows[id_field].unique()} ")
        for field in print_fields:
            print(f"{entity_type.capitalize()} {field}: {all_rows[field].unique()}")

        # Input answer
        answer = console.input("stay in the dataset? (y/n)")
        keep_resource = True
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
            f"Is the finalized language: [bold blue] {potential_language} [/] of this {id_field} correct? ")
        finalized_language = None
        if language_answers != 'n':
            finalized_language = potential_language
        if language_answers == 'n':
            final_language = console.input("What is the correct language? ")
            finalized_language = final_language
        df.loc[(df[id_field] == id), 'keep_resource'] = keep_resource
        df.loc[(df[id_field] == id), 'finalized_language'] = finalized_language
        df.to_csv(join_output_path, index=False)
        print(u'\u2500' * 10)
    return df

def double_check_languages(df: pd.DataFrame, id_field: str, join_output_path: str, print_fields: List[str]) -> pd.DataFrame:
    """
    Double checks the languages for the search queries data.

    :param df: The DataFrame to double check
    :param id_field: The ID field to use ('full_name' for repos, 'login' for users)
    :param join_output_path: The path to the join output file
    :param print_fields: The fields to print
    :return: The DataFrame with the languages double checked
    """
    subset_search_df = df.drop_duplicates(subset=[id_field, 'finalized_language'])
    double_check = subset_search_df[id_field].value_counts().reset_index().rename(
        columns={'index': id_field, id_field: 'count'}).sort_values('count', ascending=False)
    double_check = double_check[double_check['count'] > 1]
    entity_type = 'Repo' if id_field == 'full_name' else 'User'
    for _, row in tqdm(double_check.iterrows(), total=len(double_check), desc=f"Double Checking {id_field.capitalize()}s"):
        needs_updating = df[df[id_field] == row[id_field]]
        unique_detected_languages = needs_updating.detected_language.unique().tolist()
        if len(unique_detected_languages) > 1:
            print(f"{entity_type.capitalize()} {row[id_field]}")
            for field in print_fields:
                print(f"{entity_type.capitalize()} {field}: {needs_updating[field].unique()}")
            print(f"{entity_type.capitalize()} Finalized Language: {needs_updating.finalized_language.tolist()}")
            final_language = console.input("What is the correct language? ")
            df.loc[(df[id_field] == row[id_field]), 'finalized_language'] = final_language
            df.to_csv(join_output_path, index=False)
            print(u'\u2500' * 10)
        else:
            df.loc[(df[id_field] == row[id_field]), 'finalized_language'] = unique_detected_languages[0]
            df.to_csv(join_output_path, index=False)
    return df

if __name__ == "__main__":
    subset_terms = ["Digital Humanities"]
    console = Console()
    data_directory_path = "../../datasets"
    initial_repo_output_path = f"{data_directory_path}/repo_data/"
    repo_output_path = f"{data_directory_path}/large_files/entity_files/repos_dataset.csv"
    initial_repo_join_output_path = f"{data_directory_path}/large_files/join_files/search_queries_repo_join_dataset.csv"
    repo_join_output_path = f"{data_directory_path}/derived_files/initial_search_queries_repo_join_subset_dh_dataset.csv"

    initial_user_output_path = f"{data_directory_path}/user_data/"
    user_output_path = f"{data_directory_path}/entity_files/users_dataset.csv"
    org_output_path = f"{data_directory_path}/entity_files/orgs_dataset.csv"
    initial_user_join_output_path = f"{data_directory_path}/join_files/search_queries_user_join_dataset.csv"
    user_join_output_path = f"{data_directory_path}/derived_files/initial_search_queries_user_join_subset_dh_dataset.csv"

    verify_search_results = False

    if verify_search_results:
        search_queries_repo_df, search_queries_user_df = verify_results_exist(initial_repo_join_output_path, repo_join_output_path, initial_user_join_output_path, user_join_output_path, subset_terms)

        search_queries_repo_df.to_csv(f"{data_directory_path}/derived_files/initial_search_queries_repo_join_subset_dh_dataset.csv", index=False)
        search_queries_user_df.to_csv(f"{data_directory_path}/derived_files/initial_search_queries_user_join_subset_dh_dataset.csv", index=False)
    else:

        search_queries_repo_df = read_csv_file(f"{data_directory_path}/derived_files/initial_search_queries_repo_join_subset_dh_dataset.csv")
        search_queries_user_df = read_csv_file(f"{data_directory_path}/derived_files/initial_search_queries_user_join_subset_dh_dataset.csv")

    search_queries_repo_df, needs_checking_repos = generate_needs_checking(search_queries_repo_df, repo_join_output_path, 'full_name')
    search_queries_user_df, needs_checking_users = generate_needs_checking(search_queries_user_df, user_join_output_path, 'login')

    # Call the function for both the repo and user data
    user_print_fields = ['URL', 'Type', 'Bio', 'Location', 'Natural Language', 'Detected Language', 'Search Query', 'Search Query Term', 'Search Query Source Term']
    repo_print_fields = ['URL', 'Description', 'Natural Language', 'Detected Language', 'Search Query', 'Search Query Term', 'Search Query Source Term']
    search_queries_user_df = process_needs_checking(search_queries_user_df, needs_checking_users, 'login', user_join_output_path, user_print_fields)
    search_queries_repo_df = process_needs_checking(search_queries_repo_df, needs_checking_repos, 'full_name', repo_join_output_path, repo_print_fields)

    search_queries_repo_df = double_check_languages(search_queries_repo_df, 'full_name', repo_join_output_path, repo_print_fields)
    search_queries_user_df = double_check_languages(search_queries_user_df, 'login', user_join_output_path, user_print_fields)


search_queries_repo_df.to_csv(repo_join_output_path, index=False)
search_queries_user_df.to_csv(user_join_output_path, index=False)