from rich import print
from rich.console import Console
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


def clean_search_queries_data(search_df: object, join_field: str) -> object:
    """Clean the search queries data and try to determine as much as possible the exact language using automated language detection and natural language processing.
    :param search_df: The search queries data
    :type search_df: pandas.DataFrame
    :param join_field: The field to join the search queries data to the repo data
    :type join_field: str
    :return: The cleaned search queries data
    :rtype: pandas.DataFrame"""
    search_df['finalized_language'] = None
    search_df['keep_resource'] = True
    search_df['cleaned_search_query'] = search_df['search_query'].str.replace(
        '%22', '"').str.replace('%3A', ":").str.split('&page').str[0]
    search_df = search_df.drop_duplicates(
        subset=[join_field, 'cleaned_search_query'])
    search_df['finalized_language'] = None
    english_langs = 'en, ny, ha, ig, lb, mg, sm, sn, st, tl, yo'
    english_langs = english_langs.split(', ')
    search_df.loc[search_df.detected_language.isin(
        english_langs), 'finalized_language'] = search_df.detected_language
    search_df.loc[search_df.natural_language == search_df.detected_language,
                  'finalized_language'] = search_df.detected_language
    search_df.loc[(search_df.detected_language.str.contains('zh', na=False)) & (search_df.natural_language == 'zh'), 'finalized_language'] = search_df.loc[(
        search_df.detected_language.str.contains('zh', na=False)) & (search_df.natural_language == 'zh'), 'detected_language']
    search_df.loc[(search_df.natural_language.str.contains('fr')) & (
        search_df.detected_language.str.contains('fr')), 'finalized_language'] = 'fr'
    search_df.loc[(search_df.natural_language == 'xh, zu') & (search_df.finalized_language.isna()), 'finalized_language'] = search_df.loc[(
        search_df.natural_language == 'xh, zu') & (search_df.detected_language.notna()), 'detected_language']
    search_df.loc[(search_df.finalized_language.notna()) & (
        search_df.detected_language_confidence < 0.5), 'finalized_language'] = None
    if join_field == 'full_name':

        search_df.loc[(search_df.finalized_language.notna()) & (
        search_df.description.str.len() < 30), 'finalized_language'] = None
        search_df.loc[(search_df.detected_language.isna()) & (
            search_df.description.isna()), 'finalized_language'] = None
        search_df.loc[(search_df.detected_language.isna()) & (
            search_df.description.isna()) & (search_df['size'] < 1), 'keep_resource'] = False
    if join_field == 'login':
        search_df.loc[(search_df.finalized_language.notna()) & (
            search_df.bio.str.len() < 30), 'finalized_language'] = None
        search_df.loc[(search_df.detected_language.isna()) & (
            search_df.bio.isna()), 'finalized_language'] = None
    return search_df


console = Console()
initial_repo_output_path = "../data/repo_data/"
repo_output_path = "../data/large_files/entity_files/repos_dataset.csv"
repo_join_output_path = "../data/derived_files/updated_search_queries_repo_join_subset_dh_dataset.csv"

initial_user_output_path = "../data/user_data/"
user_output_path = "../data/entity_files/users_dataset.csv"
user_join_output_path = "../data/derived_files/search_queries_user_join_subset_dh_dataset.csv"
search_queries_repo_df = pd.read_csv(repo_join_output_path)
search_queries_user_df = pd.read_csv(user_join_output_path)


# CHECK REPOS
# repo_join_field = 'full_name'
# if 'finalized_language' not in search_queries_repo_df.columns:
#     search_queries_repo_df = clean_search_queries_data(
#         search_queries_repo_df, repo_join_field)

# needs_checking = search_queries_repo_df[(search_queries_repo_df['keep_resource'] == True) & (search_queries_repo_df['finalized_language'].isna())]
# needs_checking.loc[needs_checking.detected_language.isna(), 'detected_language'] = None
# needs_checking.loc[needs_checking.natural_language.isna(), 'natural_language'] = None
# needs_checking = needs_checking.reset_index(drop=True)
# print(needs_checking)
# for index, row in needs_checking.iterrows():
#     all_rows = search_queries_repo_df[(search_queries_repo_df['full_name'] == row.full_name)]
#     print(f"On {index} out of {len(needs_checking)}")
#     print(f"This repo {row.full_name} ")
#     print(f"Repo URL: {row.html_url}")
#     print(f"Repo Description: {row.description}")
#     print(f"Repo Natural Language: {all_rows.natural_language.unique()}")
#     print(f"Repo Detected Language: {all_rows.detected_language.unique()}")
#     print(f"Repo Search Query: {all_rows.search_query.unique()}")
#     print(f"Repo Search Query Term: {all_rows.search_term.unique()}")
#     print(f"Repo Search Query Source Term: {all_rows.search_term_source.unique()}")
#     # Input answer
#     answer = console.input("stay in the dataset? (y/n)")
#     if answer == 'n':
#         row.keep_resource = False

#     detected_languages = all_rows[all_rows.detected_language.notna()].detected_language.unique().tolist()
#     natural_languages = all_rows[all_rows.natural_language.notna()].natural_language.unique().tolist()

#     detected_languages = detected_languages[0] if len(detected_languages) == 1 else str(detected_languages).replace('[', '').replace(']', '')
#     natural_languages = natural_languages[0] if len(natural_languages) == 1 else str(natural_languages).replace('[', '').replace(']', '')
#     potential_language = detected_languages if len(detected_languages) != 0 else natural_languages
#     potential_language = potential_language if len(potential_language) != 0 else 'None'

#     if ',' in potential_language:
#         if 'fr' in potential_language:
#             potential_language = 'fr'
#         elif 'en' in potential_language:
#             potential_language = 'en'
#         elif 'xh' in potential_language:
#             potential_language = 'en'

#     language_answers = console.input(
#         f"Is the finalized language: [bold blue] {potential_language} [/] of this repo correct? ")
#     if language_answers != 'n':
#         row.finalized_language = potential_language
#     if language_answers == 'n':
#         final_language = console.input("What is the correct language? ")
#         row.finalized_language = final_language
#     search_queries_repo_df.loc[(search_queries_repo_df.full_name == row.full_name), 'keep_resource'] = row.keep_resource
#     search_queries_repo_df.loc[(search_queries_repo_df.full_name == row.full_name), 'finalized_language'] = row.finalized_language
#     search_queries_repo_df.to_csv(repo_join_output_path, index=False)
#     print(u'\u2500' * 10)

# subset_search_df = search_queries_repo_df.drop_duplicates(
#     subset=['full_name', 'finalized_language'])
# double_check = subset_search_df.full_name.value_counts().reset_index().rename(
#     columns={'index': 'full_name', 'full_name': 'count'}).sort_values('count', ascending=False)
# double_check = double_check[double_check['count'] > 1]
# for index, row in double_check.iterrows():
#     needs_updating = search_queries_repo_df[search_queries_repo_df.full_name == row.full_name]
#     print(f"Repo {row.full_name}")
#     print(f"Repo URL: {needs_updating.html_url.unique()}")
#     print(f"Repo Description: {needs_updating.description.unique()}")
#     print(f"Repo Natural Language: {needs_updating.natural_language.tolist()}")
#     print(f"Repo Detected Language: {needs_updating.detected_language.tolist()}")
#     print(f"Repo Search Query: {needs_updating.search_query.unique()}")
#     print(f"Repo Search Query Term: {needs_updating.search_term.unique()}")
#     print(f"Repo Search Query Source Term: {needs_updating.search_term_source.unique()}")
#     print(f"Repo Finalized Language: {needs_updating.finalized_language.tolist()}")
#     final_language = console.input("What is the correct language? ")
#     search_queries_repo_df.loc[(search_queries_repo_df.full_name == row.full_name), 'finalized_language'] = final_language
#     search_queries_repo_df.to_csv(repo_join_output_path, index=False)
#     print(u'\u2500' * 10)

# CHECK USER
user_join_field = 'login'
if 'finalized_language' not in search_queries_user_df.columns:
    search_queries_user_df = clean_search_queries_data(
    search_queries_user_df, user_join_field)

needs_checking = search_queries_user_df[(search_queries_user_df['keep_resource'] == True) & (
    search_queries_user_df['finalized_language'].isna())]
needs_checking.loc[needs_checking.detected_language.isna(), 'detected_language'] = None
needs_checking.loc[needs_checking.natural_language.isna(), 'natural_language'] = None
needs_checking = needs_checking.reset_index(drop=True)
for index, row in needs_checking.iterrows():
    all_rows = search_queries_user_df[(
        search_queries_user_df['login'] == row.login)]
    print(f"On {index} out of {len(needs_checking)}")
    print(f"This user {row.login} ")
    print(f"User URL: {row.html_url}")
    print(f"User Type: {row.type}")
    print(f"User Bio: {row.bio}")
    print(f"User Location: {row.location}")
    print(f"User Natural Language: {all_rows.natural_language.unique()}")
    print(f"User Detected Language: {all_rows.detected_language.unique()}")
    print(f"User Search Query: {all_rows.search_query.unique()}")
    print(f"User Search Query Term: {all_rows.search_term.unique()}")
    print(
        f"User Search Query Source Term: {all_rows.search_term_source.unique()}")
    # Input answer
    answer = console.input("stay in the dataset? (y/n)")
    if answer == 'n':
        row.keep_resource = False

    detected_languages = all_rows[all_rows.detected_language.notna(
    )].detected_language.unique().tolist()
    natural_languages = all_rows[all_rows.natural_language.notna(
    )].natural_language.unique().tolist()

    detected_languages = row.detected_language if len(detected_languages) == 1 else str(
        detected_languages).replace('[', '').replace(']', '')
    natural_languages = row.natural_language if len(natural_languages) == 1 else str(
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
    if language_answers != 'n':
        row.finalized_language = potential_language
    if language_answers == 'n':
        final_language = console.input("What is the correct language? ")
        row.finalized_language = final_language
    search_queries_user_df.loc[(
        search_queries_user_df.login == row.login), 'keep_resource'] = row.keep_resource
    search_queries_user_df.loc[(search_queries_user_df.login ==
                                row.login), 'finalized_language'] = row.finalized_language
    search_queries_user_df.to_csv(user_join_output_path, index=False)
    print(u'\u2500' * 10)

subset_search_df = search_queries_user_df.drop_duplicates(
    subset=['login', 'finalized_language'])
double_check = subset_search_df.login.value_counts().reset_index().rename(
    columns={'index': 'login', 'login': 'count'}).sort_values('count', ascending=False)
double_check = double_check[double_check['count'] > 1]
for index, row in double_check.iterrows():
    needs_updating = search_queries_user_df[search_queries_user_df.login == row.login]
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
