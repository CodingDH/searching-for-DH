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

import apikey
# Load auth token
auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()

if __name__ == "__main__":

    data_directory_path = get_data_directory_path() 
    target_terms: list = ["Public History", "Digital History", "Digital Cultural Heritage", "Cultural Analytics", "Computational Humanities", "Computational Social Science", "Digital Humanities"]

    initial_core_users, initial_core_orgs, initial_core_repos = get_data_from_search_terms(data_directory_path, target_terms, return_search_queries=False)
    error_file_path = f"{data_directory_path}/error_logs/repo_errors.csv"
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        subset_core_repos = initial_core_repos[~initial_core_repos.full_name.isin(error_df.full_name)]
    else:
        subset_core_repos = initial_core_repos
    owner_cols = [col for col in subset_core_repos.columns if col.startswith("owner.")]
    expanded_owners = subset_core_repos[owner_cols]
    expanded_owners = expanded_owners.rename(columns={col: col.split("owner.")[1] for col in owner_cols})
    expanded_owners = expanded_owners[expanded_owners.login.notna()]
    expanded_users = expanded_owners[expanded_owners["type"] == "User"]
    expanded_orgs = expanded_owners[expanded_owners["type"] == "Organization"]
    write_only_new = False
    retry_errors = False
    entity_type = "users"
    potential_new_entities_df = expanded_users.drop_duplicates(subset=['login'])
    # potential_new_entities_df = potential_new_entities_df[0:10]
    temp_entity_dir = f"{data_directory_path}/historic_data/entity_files/all_users/"
    entity_progress_bar = tqdm(total=potential_new_entities_df.shape[0], desc="Processing entities")
    error_file_path = f"{data_directory_path}/error_logs/user_errors.csv"
    console.print(f"Error file path: {error_file_path}")
    get_new_entities(entity_type, potential_new_entities_df, temp_entity_dir, entity_progress_bar, error_file_path, write_only_new, retry_errors)

    entity_type = "orgs"
    potential_new_entities_df = expanded_orgs.drop_duplicates(subset=['login'])
    # potential_new_entities_df = potential_new_entities_df[0:10]
    temp_entity_dir = f"{data_directory_path}/historic_data/entity_files/all_orgs/"
    entity_progress_bar = tqdm(total=potential_new_entities_df.shape[0], desc="Processing entities")
    error_file_path = f"{data_directory_path}/error_logs/org_errors.csv"
    console.print(f"Error file path: {error_file_path}")
    get_new_entities(entity_type, potential_new_entities_df, temp_entity_dir, entity_progress_bar, error_file_path, write_only_new, retry_errors)