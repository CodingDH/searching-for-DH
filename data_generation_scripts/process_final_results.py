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

def get_entity_files_from_expanded_users(expanded_owners, data_directory_path):
    """
    Get the files for the expanded users
    """
    user_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_users/")
    org_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_orgs/")
    cleaned_user_files = [f.split("_coding_dh_")[0] for f in user_files if f.endswith(".csv")]
    cleaned_org_files = [f.split("_coding_dh_")[0] for f in org_files if f.endswith(".csv")]
    existing_owners = expanded_owners[(expanded_owners.login.isin(cleaned_user_files)) | (expanded_owners.login.isin(cleaned_org_files))]
    finalized_user_logins = existing_owners[existing_owners['type'] == 'User'].login.unique()
    finalized_org_logins = existing_owners[existing_owners['type'] == 'Organization'].login.unique()
    finalized_user_files = [f"{login}_coding_dh_user.csv" for login in finalized_user_logins]
    finalized_org_files = [f"{login}_coding_dh_org.csv" for login in finalized_org_logins]
    expanded_core_users = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_users/", finalized_user_files)
    expanded_core_orgs = read_combine_files(f"{data_directory_path}/historic_data/entity_files/all_orgs/", finalized_org_files)
    return expanded_core_users, expanded_core_orgs

def get_entity_files_from_expanded_repos(user_df, user_repo_interaction_df, data_directory_path):
    repo_files = os.listdir(f"{data_directory_path}/historic_data/entity_files/all_repos/")
    cleaned_repo_files = [f.split("_coding_dh_")[0].replace("_", "/", 1) for f in repo_files if f.endswith(".csv")]

if __name__ == "__main__":

    data_directory_path = get_data_directory_path() 
    target_terms: list = ["Digital Humanities"]

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
    expanded_core_users, expanded_core_orgs = get_entity_files_from_expanded_users(expanded_owners, data_directory_path)

    error_file_path = f"{data_directory_path}/error_logs/user_errors.csv"
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        subset_core_users = initial_core_users[~initial_core_users.login.isin(error_df.login)]
    else:
        subset_core_users = initial_core_users
    
    error_file_path = f"{data_directory_path}/error_logs/org_errors.csv"
    if os.path.exists(error_file_path):
        error_df = pd.read_csv(error_file_path)
        subset_core_orgs = initial_core_orgs[~initial_core_orgs.login.isin(error_df.login)]
    else:
        subset_core_orgs = initial_core_orgs

        