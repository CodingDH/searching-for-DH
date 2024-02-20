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

auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}
stargazers_auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request', 'Accept': 'application/vnd.github.v3.star+json'}

console = Console()

data_directory_path = get_data_directory_path()


def get_entities_interactions(entity_df: pd.DataFrame, url_column: str, entity_type: str, interaction_directory_path: str, interaction_type: str, threshold_limit: int, source_column: str, target_column: str, retry_errors: bool=False, write_only_new: bool=False):
    entity_type_singular = entity_type[:-1]
    active_auth_headers = auth_headers.copy() if 'stargazers' not in url_column else stargazers_auth_headers.copy()
    threshold_file_path = os.path.join(data_directory_path, "historic_data", "over_threshold", interaction_directory_path.replace('/', ''), ".csv")
    metadata_file_path = os.path.join(data_directory_path, "metadata_files", f"{entity_type}_url_cols.csv")
    metadata_df = read_csv_file(metadata_file_path)
    subset_metadata_df = metadata_df[metadata_df.url_column == url_column]
    count_column = subset_metadata_df.count_column.values[0]
    error_file_path = os.path.join(data_directory_path, "error_logs", f"{entity_type_singular}_interaction_errors.csv")
    if os.path.exists(error_file_path):
        error_drop_fields = [source_column, target_column, 'error_url']
        clean_write_error_file(error_file_path, error_drop_fields)
        if retry_errors == False:
            error_df = pd.read_csv(error_file_path)
            entity_df = entity_df[(~entity_df[source_column].isin(error_df[source_column])) & (~entity_df[target_column].isin(error_df[target_column]))]
            if entity_df.empty:
                console.print(f"All {entity_type_singular} have been processed for {interaction_type}", style="bold green")
                return
    
    drop_columns = ["coding_dh_id", "Unnamed: 0"]
    progress_bar = tqdm(total=entity_df.shape[0], desc=f"Processing {interaction_directory_path} {entity_type_singular}")
    for _, row in entity_df.iterrows():
        try:
            if (row[count_column] == None) or (row[count_column] == 0):
                console.print(f"Skipping {row[source_column]} as it has no {interaction_type}")
                progress_bar.update(1)
                continue
            elif row[count_column] > threshold_limit:
                over_threshold_df = pd.DataFrame([{f"{source_column}": row[source_column], f"{count_column}": row[count_column], 'threshold_check_date': datetime.now().strftime("%Y-%m-%d"), 'threshold_limit': threshold_limit, 'url_column': row[url_column]}])
                console.print(f"Saving {row[source_column]} as it has {row[count_column]} {interaction_type} which is over the threshold limit of {threshold_limit}")
                if os.path.exists(threshold_file_path):
                    over_threshold_df.to_csv(threshold_file_path, mode='a', header=False, index=False)
                else:
                    over_threshold_df.to_csv(threshold_file_path, index=False)
                progress_bar.update(1)
                continue
            else:
                entity_name = row[source_column].replace("/", "_")
                file_path = os.path.join(data_directory_path, interaction_directory_path, f"{entity_name}_{interaction_type}_{url_column}.csv")
                if os.path.exists(file_path):
                    existing_df = read_csv_file(file_path)
                    existing_df["coding_dh_date"] = pd.to_datetime(existing_df["coding_dh_date"])
                    existing_df = existing_df.sort_values(by="coding_dh_date", ascending=False)
                    grouped_columns = [source_column, target_column]
                    subset_existing_df = existing_df.groupby(grouped_columns).first().reset_index()
                    subset_existing_df = drop_columns(subset_existing_df, drop_columns)
                else:
                    subset_existing_df = pd.DataFrame()
                    
                query = row[url_column].split('{')[0] + '?per_page=100&page=1' if '{' in row[url_column] else row[url_column] + '?per_page=100&page=1'

                if 'check_state' in metadata_df.columns:
                    if subset_metadata_df['check_state']:
                        query = query.replace('?', '?state=all&')
                response, status_code = make_request_with_rate_limiting(query, active_auth_headers)

                dfs = []
                additional_data = {'source': row[source_column], 'url_column': row[url_column], 'interaction_type': interaction_directory_path.replace('/', ''), 'target_column': row[target_column]}
                
                if response is None:
                    log_error_to_file(error_file_path, query, status_code, additional_data)
                    progress_bar.update(1)
                    continue
                else:
                    response_data = response.json()
                    response_df = pd.json_normalize(response_data)
                    if "message" in response_df.columns:
                        console.print(f"Error for {row[source_column]} and {row[target_column]}: {response_df.message.values[0]}", style="bold red")
                        log_error_to_file(error_file_path, additional_data, status_code, query)
                        progress_bar.update(1)
                        continue
                    dfs.append(response_df)
                    while "next" in response.links.keys():
                        next_url = response.links["next"]["url"]
                        response, status_code = make_request_with_rate_limiting(next_url, active_auth_headers)
                        if response is None:
                            log_error_to_file(error_file_path, next_url, status_code, additional_data)
                            progress_bar.update(1)
                            continue
                        else:
                            response_data = response.json()
                            response_df = pd.json_normalize(response_data)
                            if "message" in response_df.columns:
                                console.print(f"Error for {row[source_column]} and {row[target_column]}: {response_df.message.values[0]}", style="bold red")
                                log_error_to_file(error_file_path, additional_data, status_code, next_url)
                                progress_bar.update(1)
                                continue
                        dfs.append(response_df)
                    if dfs:
                        combined_response_df = pd.concat(dfs)
                        combined_response_df[f"{entity_type_singular}_id"] = row.id
                        combined_response_df[f"{entity_type_singular}_url"] = row.url
                        combined_response_df[f"{entity_type_singular}_html_url"] = row.html_url
                        combined_response_df[f"{source_column}"] = row[source_column]
                        combined_response_df[f"{url_column}"] = row[url_column]
                        combined_response_df["coding_dh_date"] = datetime.now().strftime("%Y-%m-%d")
                        concat_df = pd.concat([subset_existing_df, combined_response_df])
                        grouped_df = concat_df.groupby(grouped_columns)
                        processed_files = []
                        for _, group in tqdm(grouped_df, desc=f"Grouping files"):
                            subset_columns = ["coding_dh_date"]
                            group = sort_groups_add_coding_dh_id(group, subset_columns)
                            processed_files.append(group)
                        final_processed_df = pd.concat(processed_files).reset_index(drop=True)
                        final_processed_df.to_csv(file_path, index=False)

        except Exception as e:
            console.print(f"Error for {row[source_column]} and {row[target_column]}: {e}", style="bold red")
            log_error_to_file(error_file_path, additional_data, status_code, query)
            progress_bar.update(1)
            continue
    progress_bar.close()
        

