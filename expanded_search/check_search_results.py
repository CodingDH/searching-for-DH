from rich import print
from rich.console import Console
import pandas as pd
import warnings
warnings.filterwarnings('ignore')


console = Console()
initial_repo_output_path = "../data/repo_data/"
repo_output_path = "../data/large_files/entity_files/repos_dataset.csv"
repo_join_output_path = "../data/derived_files/search_queries_repo_join_subset_dh_dataset.csv"

initial_user_output_path = "../data/user_data/"
user_output_path = "../data/entity_files/users_dataset.csv"
user_join_output_path = "../data/derived_files/search_queries_user_join_subset_dh_dataset.csv"
search_queries_repo_df = pd.read_csv(repo_join_output_path)
search_queries_user_df = pd.read_csv(user_join_output_path)

search_queries_repo_df['keep_repo'] = True
for index, row in search_queries_repo_df.iterrows():
    print(f"This repo {row.full_name} ")
    print(f"Repo URL: {row.html_url}")
    print(f"Repo Description: {row.description}")
    print(f"Repo Natural Language: {row.natural_language}")
    print(f"Repo Detected Language: {row.detected_language}")
    print(f"Repo Search Query: {row.search_query}")
    print(f"Repo Search Query Term: {row.search_term}")
    print(f"Repo Search Query Source Term: {row.search_term_source}")
    # Input answer
    answer = console.input("stay in the dataset? (y/n)")
    if answer == 'save':
        search_queries_repo_df.to_csv(repo_join_output_path, index=False)
    if answer == 'n':
        search_queries_repo_df.loc[index, 'keep_repo'] = False
    language_answers = console.input(
        f"Is the finalized language: [bold blue] {row.finalized_language} [/] of this repo correct? ")
    if language_answers == 'save':
        search_queries_repo_df.to_csv(repo_join_output_path, index=False)
    if language_answers == 'n':
        final_language = console.input("What is the correct language? ")
        search_queries_repo_df.loc[index, 'finalized_language'] = final_language
    print(u'\u2500' * 10)

search_queries_repo_df.to_csv(repo_join_output_path, index=False)

