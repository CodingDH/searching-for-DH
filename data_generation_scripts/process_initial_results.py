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
from data_generation_scripts.general_utils import get_new_entities, get_data_from_search_terms, get_data_directory_path
import time

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


if __name__ == "__main__":
    data_directory_path = get_data_directory_path()
    target_terms: list = ["Public History", "Digital History", "Digital Cultural Heritage", "Cultural Analytics", "Computational Humanities", "Computational Social Science", "Digital Humanities"]
    search_user_queries_df, search_org_queries_df, search_repo_queries_df = get_data_from_search_terms(data_directory_path, target_terms, return_search_queries=True)

    process_initial_results(search_user_queries_df, data_directory_path, "user")
    process_initial_results(search_org_queries_df, data_directory_path, "org")
    process_initial_results(search_repo_queries_df, data_directory_path, "repo")
    