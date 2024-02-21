import os
import pandas as pd
from tqdm import tqdm
from rich.console import Console
console = Console()
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")
from data_generation_scripts.general_utils import get_new_entities, get_data_from_search_terms, get_data_directory_path
from data_generation_scripts.generate_entity_metadata import get_count_metadata

import apikey
# Load auth token
auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

console = Console()

def process_initial_results(search_queries_df: pd.DataFrame, data_directory_path: str, entity_type: str):
    """
    Function to process the initial results from the search queries
    
    Args:
    search_queries_df (pd.DataFrame): The dataframe containing the search queries
    data_directory_path (str): The path to the data directory
    entity_type (str): The type of entity to process
    """
    write_only_new = False
    retry_errors = False
    drop_column = "full_name" if entity_type == "repo" else "login"
    potential_new_entities_df = search_queries_df.drop_duplicates(subset=[drop_column])
    # potential_new_entities_df = potential_new_entities_df[0:10]
    temp_entity_dir = os.path.join(data_directory_path, "historic_data", "entity_files", f"all_{entity_type}s")
    entity_progress_bar = tqdm(total=potential_new_entities_df.shape[0], desc="Processing entities")
    error_file_path = os.path.join(data_directory_path, "error_logs", f"{entity_type}_errors.csv")
    console.print(f"Error file path: {error_file_path}")
    get_new_entities(f"{entity_type}s", potential_new_entities_df, temp_entity_dir, entity_progress_bar, error_file_path, write_only_new, retry_errors)

def process_entities_counts(entity_type: str, initial_core_entities: pd.DataFrame, entity_column: str, data_directory_path: str):
    """
    Function to process the entities counts

    Args:
    entity_type (str): The type of entity to process
    initial_core_entities (pd.DataFrame): The dataframe containing the initial core entities
    entity_column (str): The column containing the entity name
    data_directory_path (str): The path to the data directory
    """
    error_file_path = os.path.join(data_directory_path, "error_logs", f"{entity_type}_errors.csv")
    return_df = False
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        subset_core_entities = initial_core_entities[~initial_core_entities[entity_column].isin(error_df[entity_column])]
    else:
        subset_core_entities = initial_core_entities
    get_count_metadata(subset_core_entities, entity_type, f"{data_directory_path}/historic_data/entity_files/all_{entity_type}/", return_df)

if __name__ == "__main__":
    data_directory_path = get_data_directory_path()
    target_terms: list = ["Public History", "Digital History", "Digital Cultural Heritage", "Cultural Analytics", "Computational Humanities", "Computational Social Science", "Digital Humanities"]
    search_user_queries_df, search_org_queries_df, search_repo_queries_df = get_data_from_search_terms(data_directory_path, target_terms, return_search_queries=True)

    process_initial_results(search_user_queries_df, data_directory_path, "user")
    process_initial_results(search_org_queries_df, data_directory_path, "org")
    process_initial_results(search_repo_queries_df, data_directory_path, "repo")

    initial_core_users, initial_core_orgs, initial_core_repos = get_data_from_search_terms(data_directory_path, target_terms, return_search_queries=False)
    
    # Now you can call the function for 'repos', 'users' and 'orgs'
    process_entities_counts('repos', initial_core_repos, 'full_name', data_directory_path)
    process_entities_counts('users', initial_core_users, 'login', data_directory_path)
    process_entities_counts('orgs', initial_core_orgs, 'login', data_directory_path)