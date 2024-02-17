import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import rich
from rich.console import Console
console = Console()
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")
from data_generation_scripts.general_utils import get_new_entities
import time

import apikey
# Load auth token
auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()


def create_queries_directories(entity_type: str, cleaned_terms: pd.DataFrame) -> pd.DataFrame:
    """
    Function to create directories for the queries
    """
    queries = []
    for _, subdir, _ in tqdm(os.walk(data_directory_path + f"/searched_{entity_type}_data"), desc="Walking through directories"):
        for directory in subdir:
            for file in os.listdir(data_directory_path + f"/searched_{entity_type}_data/" + directory):
                if file.endswith(".csv"):
                    search_term_source = directory.replace("_", " ").title()
                    if 'searched' in file:
                        search_term = file.replace(".csv", "").split(f'{entity_type}s_searched_')[1].replace("+", " ").replace("&#39;", "'")
                        if '20' in search_term:
                            search_term = search_term.split("_20")[0]
                    else:
                        search_term = search_term_source
                    
                    subset_cleaned_terms = cleaned_terms[(cleaned_terms.search_term_source == search_term_source) & (cleaned_terms.search_term == search_term)]
                    if not subset_cleaned_terms.empty:
                        subset_cleaned_terms['file_path'] = f"{data_directory_path}/searched_{entity_type}_data/{directory}/{file}"
                        subset_cleaned_terms["file_name"] = file
                        queries.append(subset_cleaned_terms)
    queries_df = pd.concat(queries)
    queries_df = queries_df.reset_index(drop=True)
    search_queries_dfs = []
    for _, row in tqdm(queries_df.iterrows(), total=queries_df.shape[0], desc="Processing queries"):
        df = pd.read_csv(row.file_path, encoding='utf-8-sig')
        df["search_file_name"] = row.file_name
        search_queries_dfs.append(df)
    search_queries_df = pd.concat(search_queries_dfs)
    return search_queries_df, queries_df

if __name__ == "__main__":
    data_directory_path = "../../new_datasets"
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

    search_user_queries_df, user_queries_df = create_queries_directories("user", cleaned_terms)
    search_org_queries_df = search_user_queries_df[search_user_queries_df['type'] == 'Organization']
    search_org_queries_df = search_org_queries_df[search_org_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]
    search_user_queries_df = search_user_queries_df[search_user_queries_df['type'] == 'User']
    search_user_queries_df = search_user_queries_df[search_user_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]
    search_repo_queries_df, repo_queries_df = create_queries_directories("repo", cleaned_terms)
    search_repo_queries_df = search_repo_queries_df[search_repo_queries_df.search_term_source.isin(cleaned_terms.search_term_source.unique())]
    write_only_new = True
    entity_type = "repos"
    potential_new_entities_df = search_repo_queries_df.drop_duplicates(subset=['full_name'])
    # potential_new_entities_df = potential_new_entities_df[0:10]
    temp_entity_dir = f"{data_directory_path}/historic_data/entity_files/all_repos/"
    entity_progress_bar = tqdm(total=potential_new_entities_df.shape[0], desc="Processing entities")
    error_file_path = f"{data_directory_path}/error_logs/repo_errors.csv"
    console.print(f"Error file path: {error_file_path}")
    # get_new_entities(entity_type, potential_new_entities_df, temp_entity_dir, entity_progress_bar, error_file_path, write_only_new)