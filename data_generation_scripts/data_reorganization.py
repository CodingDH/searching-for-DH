import os
import sys

import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime

sys.path.append("..")
from data_generation_scripts.general_utils import read_csv_file
import rich
from rich.console import Console

console = Console()

def group_file_paths(current_path: str, older_path: str) -> pd.DataFrame:
    """
    Group file paths based on their full name. The function reads files,
    formats dates, and groups the data, keeping unique entries or the oldest entry per group.

    Returns:
    pd.DataFrame: Grouped and processed DataFrame.
    """
    file_paths_dfs = []

    for main_dir, _, files in os.walk(current_path):
        older_dir = main_dir.replace(current_path, older_path)
        if os.path.exists(older_dir) and ('archived_data' not in main_dir):
            older_files = os.listdir(older_dir)
            for file in files:
                if file.endswith(".csv"):
                    subset_file = os.path.splitext(file)[0]
                    for older_file in older_files:
                        subset_older_file = older_file.split("_202")[0]
                        if subset_file == subset_older_file:
                            file_dict = {
                                'file_path': os.path.join(main_dir, file),
                                'subset_file': subset_file,
                                'dir_path': main_dir,
                                'older_file_path': os.path.join(older_dir, older_file)
                            }
                            file_paths_dfs.append(file_dict)
    processed_files_df = pd.DataFrame(file_paths_dfs)
    processed_files_df['grouped_dir_path'] = processed_files_df.dir_path.str.split("datasets/").str[1].str.split("/").str[0]
    return processed_files_df

def format_file(file_path, date):
    """
    Read a CSV file and add a formatted date column.
    """
    file_df = read_csv_file(file_path)
    file_df['coding_dh_date'] = pd.to_datetime(date)
    return file_df

def sort_groups_add_id(group, subset_columns):
    group = group.drop_duplicates(subset=group.columns.difference(subset_columns))
    if (group.drop(columns=subset_columns).nunique() > 1).any():
        group = group.sort_values('coding_dh_date')
        group['coding_dh_id'] = np.arange(len(group))
    else:
        group = group.sort_values('coding_dh_date').iloc[0:1]
        group['coding_dh_id'] = 0
    return group

def drop_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df

def process_and_group_files(file_group, overwrite_files=False):
    """
    Process and group files based on their full name. The function reads files,
    formats dates, and groups the data, keeping unique entries or the oldest entry per group.

    Parameters:
    file_group (pd.DataFrame): DataFrame with file paths and related information.

    Returns:
    pd.DataFrame: Grouped and processed DataFrame.
    """

    new_grouped_dir_path = file_group.iloc[0]['new_grouped_dir_path']
    final_path = file_group.iloc[0]['file_path'].replace(file_group["grouped_dir_path"].iloc[0], new_grouped_dir_path)
    if (os.path.exists(final_path)) and (not overwrite_files):
        console.print(f"File already exists: {final_path}", style="bold red")
        return
    else:
        console.print(f"Processing file: {file_group.iloc[0]['subset_file']}", style="bold green")
        # Process the current file
        current_date = datetime.today().strftime('%Y-%m-%d')


        columns_to_drop = ['org_query_time', 'user_query_time', 'repo_query_time', 'search_query_time']

        # Process the existing file
        existing_file = format_file(file_group.iloc[0]['file_path'], current_date)
        existing_file = drop_columns(existing_file, columns_to_drop)

        print(file_group['older_file_path'])
        print(file_group)
        # Process the older file
        older_date = "202" + file_group['older_file_path'].iloc[0].split('_202')[1].replace("_", "-").split(".")[0]
        older_date = older_date.split(" ")[0]

        older_file = format_file(file_group['older_file_path'], older_date)
        older_file = drop_columns(older_file, columns_to_drop)

        # Combine and group files
        combined_files = pd.concat([existing_file, older_file])
        has_search_query = file_group.iloc[0]['has_search_query']

        if has_search_query:
            if 'search_query' not in combined_files.columns:
                final_path = file_group.iloc[0]['file_path'].replace(file_group.iloc[0]['grouped_dir_path'], new_grouped_dir_path)
                console.print(final_path)
                if not os.path.exists(os.path.dirname(final_path)):
                    os.makedirs(os.path.dirname(final_path))
                existing_file.to_csv(final_path, index=False)
                return
            
            combined_files['cleaned_search_query'] = combined_files.search_query.str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]

        group_column = file_group.iloc[0]['grouped_column']

        if file_group.target.isnull().all():
            grouped_columns = [group_column]
        else:
            grouped_columns = [group_column, file_group.target.iloc[0]]
        grouped_files = combined_files.groupby(grouped_columns)
        processed_files = []
        for _, group in tqdm(grouped_files, desc="Grouping files"):
            if has_search_query:
                subset_columns = ['coding_dh_date', 'search_query']
            else:
                subset_columns = ['coding_dh_date']
            group = sort_groups_add_id(group, subset_columns)
            processed_files.append(group)

        final_df = pd.concat(processed_files).reset_index(drop=True)
        
        if not os.path.exists(os.path.dirname(final_path)):
            os.makedirs(os.path.dirname(final_path))
        # Append to the final file if it exists, otherwise create a new file
        console.print(final_path)
        if os.path.exists(final_path):
            final_df.to_csv(final_path, mode='a', header=False, index=False)
        else:
            final_df.to_csv(final_path, index=False)

def deduplicate_final_file(file_group):
    new_grouped_dir_path = file_group.iloc[0]['new_grouped_dir_path']
    final_path = file_group.iloc[0]['file_path'].replace(file_group["grouped_dir_path"].iloc[0], new_grouped_dir_path)
    final_df = read_csv_file(final_path)
    group_column = file_group.iloc[0]['grouped_column']

    if file_group.target.isnull().all():
        grouped_columns = [group_column]
    else:
        grouped_columns = [group_column, file_group.target.iloc[0]]
    duplicated_files = final_df[final_df.duplicated(subset=grouped_columns + ['coding_dh_date'], keep=False)]
    if len(duplicated_files) > 0:
        console.print(f"Removing {len(duplicated_files)} duplicated files from {final_path}", style="bold red")
        final_df = final_df.drop(columns=['coding_dh_id'])
        tqdm.pandas(desc="Sorting groups")
        final_df = final_df.groupby(grouped_columns).progress_apply(sort_groups_add_id, subset_columns=grouped_columns + ['coding_dh_date']).reset_index(drop=True)
        final_path = final_path.replace("updated_join", "updated_deduped_join")
        final_df.to_csv(final_path, index=False)
    else:
        console.print(f"No duplicated files in {final_path}", style="bold green")
    return



if '__main__' == __name__:

    # # Get the file paths
    # local_current_path = "../../datasets/large_files/join_files/"
    # local_older_path = "../../datasets/older_files/large_files/join_files/"
    # original_files_df = group_file_paths(local_current_path, local_older_path)
    # if 'join_files' in local_current_path:
    #     cols_df = pd.read_csv("../../datasets/derived_files/file_totals.csv")
    #     df = cols_df[['file_name', 'source', 'target']]
    #     df = df.rename(columns={'file_name': 'file_path', 'target': 'grouped_column', 'source': 'target'})
    #     original_files_df = original_files_df.merge(df, on='file_path', how='left')
    #     original_files_df.loc[original_files_df.subset_file.str.contains("search_queries_user"), "grouped_column"] = "login"
    #     original_files_df.loc[(original_files_df.grouped_column.isna()) & original_files_df.subset_file.str.contains("org"), "grouped_column"] = "login"
    #     original_files_df.loc[original_files_df.subset_file.str.contains("search_queries_repo"), "grouped_column"] = "full_name"
    #     original_files_df["has_search_query"] = False
    #     original_files_df.loc[original_files_df.subset_file.str.contains("search_queries"), "has_search_query"] = True
    #     original_files_df["new_grouped_dir_path"] = "updated_join_files"
    # if ('repo_data' in local_current_path) or ('user_data' in local_current_path):
    #     original_files_df["grouped_column"] = "full_name" if 'repo_data' in local_current_path else "login"
    #     original_files_df["has_search_query"] = True
    #     original_files_df["new_grouped_dir_path"] = "searched_repo_data" if 'repo_data' in local_current_path else "searched_user_data"
    
    # # tqdm.pandas(desc="Processing files")
    # # overwrite_files = False
    # # original_files_df.groupby(['subset_file', 'older_file_path']).progress_apply(process_and_group_files, overwrite_files=overwrite_files)

    # tqdm.pandas(desc="Deduplicating files")
    # exclude_files = ['user_subscriptions_join_dataset', 'user_starred_join_dataset']
    # original_files_df[~original_files_df.subset_file.isin(exclude_files)].groupby(['subset_file', 'older_file_path']).progress_apply(deduplicate_final_file)
    # for file in os.listdir("../../datasets/older_files/large_files/entity_files"):
    #     if ("repos" in file) and (not file.startswith('.')):
    #         df = pd.read_csv(os.path.join("../../datasets/older_files/large_files/entity_files", file))
    #         temp_dir = "../../datasets/temp/temp_repos/"
    #         older_date = "202" + file.split('_202')[1].replace("_", "-").split(".")[0]
    #         older_date = older_date.split(" ")[0]
    #         columns_to_drop = ['org_query_time', 'user_query_time', 'repo_query_time', 'search_query_time']
    #         df = drop_columns(df, columns_to_drop)
    #         subset_columns = ['coding_dh_date']
    #         for _, row in tqdm(df.iterrows(), desc=f"Processing files for {file}", total=len(df)):
    #             full_name = row['full_name']
    #             full_name = str(full_name).replace("/", "_")
    #             row_df = pd.DataFrame([row])
    #             row_df['coding_dh_date'] = pd.to_datetime(older_date)
    #             temp_path = os.path.join(temp_dir, full_name + "_coding_dh_repo.csv")
    #             if os.path.exists(temp_path):
    #                 temp_df = pd.read_csv(temp_path)
    #                 temp_df['coding_dh_date'] = pd.to_datetime(temp_df['coding_dh_date'])
    #                 combined_df = pd.concat([temp_df, row_df])
    #                 processed_combined_df = sort_groups_add_id(combined_df, subset_columns)
    #             else:
    #                 processed_combined_df = row_df
    #                 processed_combined_df['coding_dh_id'] = 0
    #             processed_combined_df.to_csv(temp_path, index=False)
    subset_path ="../../datasets/older_files/large_files/join_files/"
    older_files = os.listdir(subset_path)
    older_files = [f for f in older_files if f.endswith(".csv") and ("starred" in f) ]
    from datetime import datetime
    current_date = datetime.today().strftime('%Y-%m-%d')
    df = pd.read_csv("../../datasets/updated_join_files/join_files/user_starred_join_dataset.csv")
    drop_cols = ['org_query_time', 'user_query_time', 'repo_query_time', 'search_query_time', 'user_repos_url']
    df = drop_columns(df, drop_cols)
    df['coding_dh_date'] = pd.to_datetime(current_date)
    new_temp_dir = "../../new_datasets/historic_data/join_files/user_starred_join_dataset"
    if not os.path.exists(new_temp_dir):
        os.makedirs(new_temp_dir)
    
    grouped_columns = ["user_login", "full_name"]
    # grouped_files = df.groupby(grouped_columns)
    # for group in grouped_columns:
    #     subset_columns = ['coding_dh_date']
    #     group = sort_groups_add_id(group, subset_columns)
    #     user_login = group.user_login.iloc[0].replace(" ", "_").replace("/", "_")
    #     file_path = f"{new_temp_dir}/{user_login}_user_repo_starred_url.csv"
    #     group.to_csv(file_path, index=False)

    # df = df.drop(columns=['coding_dh_id'])
    processed_older_files = []
    for file in tqdm(older_files, desc="Processing older files"):
        older_date = "202" + file.split('_202')[1].replace("_", "-").split(".")[0]
        older_date = older_date.split(" ")[0]
        older_df = format_file(os.path.join(subset_path, file), older_date)
        older_df = drop_columns(older_df, drop_cols)
        older_df['coding_dh_date'] = pd.to_datetime(older_date)
        processed_older_files.append(older_df)
    combined_df = pd.concat([df] + processed_older_files)
    grouped_files = combined_df.groupby(grouped_columns)
    for group_key, group_df in tqdm(grouped_files, desc="Grouping files"):
        subset_columns = ['coding_dh_date']
        user_login = group_df.user_login.iloc[0].replace(" ", "_").replace("/", "_")
        file_path = f"{new_temp_dir}/{user_login}_user_repo_starred_url.csv"
        group_df = sort_groups_add_id(group_df, subset_columns)
        group_df.to_csv(file_path, index=False)
    



            