from rich import print
from rich.console import Console
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

console = Console()
initial_repo_output_path = "../data/repo_data/"
repo_output_path = "../data/large_files/entity_files/repos_dataset.csv"
repo_join_output_path = "../data/derived_files/updated_search_queries_repo_join_subset_dh_dataset.csv"

initial_user_output_path = "../data/user_data/"
user_output_path = "../data/entity_files/users_dataset.csv"
user_join_output_path = "../data/derived_files/search_queries_user_join_subset_dh_dataset.csv"
search_queries_repo_df = pd.read_csv(repo_join_output_path)
search_queries_user_df = pd.read_csv(user_join_output_path)


needs_checking = search_queries_repo_df[(search_queries_repo_df['keep_repo'] == True) & (search_queries_repo_df['finalized_language'].isna())]
needs_checking.loc[needs_checking.detected_language.isna(), 'detected_language'] = None
needs_checking = needs_checking.reset_index(drop=True)
for index, row in needs_checking.iterrows():
    all_rows = search_queries_repo_df[(search_queries_repo_df['full_name'] == row.full_name)]
    print(f"On {index} out of {len(needs_checking)}")
    print(f"This repo {row.full_name} ")
    print(f"Repo URL: {row.html_url}")
    print(f"Repo Description: {row.description}")
    print(f"Repo Natural Language: {all_rows.natural_language.unique()}")
    print(f"Repo Detected Language: {all_rows.detected_language.unique()}")
    print(f"Repo Search Query: {all_rows.search_query.unique()}")
    print(f"Repo Search Query Term: {all_rows.search_term.unique()}")
    print(f"Repo Search Query Source Term: {all_rows.search_term_source.unique()}")
    # Input answer
    answer = console.input("stay in the dataset? (y/n)")
    if answer == 'n':
        row.keep_repo = False

    detected_languages = all_rows[all_rows.detected_language.notna()].detected_language.unique().tolist()
    natural_languages = all_rows[all_rows.natural_language.notna()].natural_language.unique().tolist()

    detected_languages = row.detected_language if len(detected_languages) == 1 else str(detected_languages).replace('[', '').replace(']', '')
    natural_languages = row.natural_language if len(natural_languages) == 1 else str(natural_languages).replace('[', '').replace(']', '')

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
    if language_answers != 'n':
        row.finalized_language = potential_language
    if language_answers == 'n':
        final_language = console.input("What is the correct language? ")
        row.finalized_language = final_language
    search_queries_repo_df.loc[(search_queries_repo_df.full_name == row.full_name), 'keep_repo'] = row.keep_repo
    search_queries_repo_df.loc[(search_queries_repo_df.full_name == row.full_name), 'finalized_language'] = row.finalized_language
    search_queries_repo_df.to_csv(repo_join_output_path, index=False)
    print(u'\u2500' * 10)