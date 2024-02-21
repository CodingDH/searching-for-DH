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
from data_generation_scripts.generate_entity_interactions import *
from ast import literal_eval
import apikey

import apikey
# Load auth token
auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()

def process_firstpass_repos(entity_type:str, initial_core_entities: pd.DataFrame, data_directory_path: str):
    """
    Function to get the first pass of repositories for users and orgs

    Args:
    entity_type (str): The type of entity to process
    initial_core_entities (pd.DataFrame): The dataframe containing the initial core entities
    data_directory_path (str): The path to the data directory
    """
    error_file_path = os.path.join(data_directory_path, 'error_logs', f"{entity_type}_errors.csv")
    if os.path.exists(error_file_path):
        error_df = read_csv_file(error_file_path)
        subset_core_entities = initial_core_entities[~initial_core_entities.login.isin(error_df.login)]
    else:
        subset_core_entities = initial_core_entities

    entity_interaction_df = read_csv_file(os.path.join(data_directory_path, 'metadata_files', "entity_interactions.csv"))
    url_column = "repos_url"
    subset_entity_interaction_df = entity_interaction_df[(entity_interaction_df.url_column == url_column) & (entity_interaction_df.entity_type == entity_type[:-1])]
    interaction_directory_path = subset_entity_interaction_df.file_directory.values[0]
    interaction_type = subset_entity_interaction_df.interaction_type.values[0]
    source_column = subset_entity_interaction_df['source'].values[0]
    target_column = subset_entity_interaction_df['target'].values[0]
    threshold_limit = 1000
    write_only_new = False
    retry_errors = True
    console.print(f"Processing {interaction_directory_path} {entity_type[:-1]}")
    console.print(f"Threshold limit: {threshold_limit}")
    console.print(f"Source column: {source_column}")
    console.print(f"Target column: {target_column}")
    console.print(f"Retry errors: {retry_errors}")
    get_entities_interactions(subset_core_entities, url_column, entity_type, interaction_directory_path, interaction_type, threshold_limit, source_column, target_column, retry_errors, write_only_new)

def process_firstpass_repo_owners(entity_type: str, expanded_owners: pd.DataFrame, data_directory_path: str, write_only_new: bool, retry_errors: bool):
    """
    Function to get the first pass of owners for repositories

    Args:
    entity_type (str): The type of entity to process
    expanded_owners (pd.DataFrame): The dataframe containing the expanded owners
    data_directory_path (str): The path to the data directory
    write_only_new (bool): Whether to write only new entities
    retry_errors (bool): Whether to retry errors
    """
    potential_new_entities_df = expanded_owners[expanded_owners["type"] == entity_type.capitalize()].drop_duplicates(subset=['login'])
    temp_entity_dir = os.path.join(data_directory_path, 'historic_data', 'entity_files', f"all_{entity_type}")
    entity_progress_bar = tqdm(total=potential_new_entities_df.shape[0], desc="Processing entities")
    error_file_path = os.path.join(data_directory_path, 'error_logs', f"{entity_type}_errors.csv")
    console.print(f"Error file path: {error_file_path}")
    get_new_entities(entity_type, potential_new_entities_df, temp_entity_dir, entity_progress_bar, error_file_path, write_only_new, retry_errors)

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

    write_only_new = False
    retry_errors = False

    # Now you can call the function for 'users' and 'orgs'
    process_firstpass_repo_owners('users', expanded_owners, data_directory_path, write_only_new, retry_errors)
    process_firstpass_repo_owners('orgs', expanded_owners, data_directory_path, write_only_new, retry_errors)

    # Now you can call the function for 'orgs' and 'users'
    process_firstpass_repos('orgs', initial_core_orgs, data_directory_path)
    process_firstpass_repos('users', initial_core_users, data_directory_path)