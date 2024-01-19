import os
import pandas as pd
import numpy as np
from tqdm import tqdm
import rich
from rich.console import Console
console = Console()

def group_file_paths():
    current_path = "../../datasets/"
    older_path = "../../datasets/older_files/"
    file_paths_dfs = []

    for dir, _, files in os.walk(current_path):
        older_dir = dir.replace(current_path, older_path)
        if os.path.exists(older_dir) and ('archived_data' not in dir):
            older_files = os.listdir(older_dir)
            for file in files:
                if file.endswith(".csv"):
                    subset_file = os.path.splitext(file)[0]
                    for older_file in older_files:
                        subset_older_file = older_file.split("_202")[0]
                        if subset_file == subset_older_file:
                            file_dict = {
                                'file_path': os.path.join(dir, file),
                                'subset_file': subset_file,
                                'dir_path': dir,
                                'older_file_path': os.path.join(older_dir, older_file)
                            }
                            file_paths_dfs.append(file_dict)
    files_df = pd.DataFrame(file_paths_dfs)
    files_df['grouped_dir_path'] = files_df.dir_path.str.split("datasets/").str[1].str.split("/").str[0]
    return files_df

def annotate_file_paths(files_df):
    subset_files_df = files_df[['grouped_dir_path', 'subset_file']].drop_duplicates()
    for index, row in subset_files_df.iterrows():
        console.print(f"Annotating file: [bold blue]{row['grouped_dir_path']}[/bold blue], subset_file_path [bold blue]{row['subset_file']}[/bold blue], row [bold blue]{index}[/bold blue] of [bold blue]{len(subset_files_df)}[/bold blue]", style="bold green")

        sample_files_df = files_df[(files_df.grouped_dir_path == row['grouped_dir_path']) & (files_df.subset_file == row['subset_file'])]
        console.print(sample_files_df.file_path.values, style="bold blue")

        has_search_query = False
        has_search_query_input = console.input("Does the file have a search query column? (y/n): ")
        if has_search_query_input == 'y':
            has_search_query = True
        
        random_file = sample_files_df.sample(1)
        existing_df = pd.read_csv(random_file.iloc[0]['file_path'])
        console.print(existing_df.columns, style="bold blue")
        grouped_column_input = console.input("What column should the file be grouped by? (e.g. 'url'): ")

        console.print(f"Current grouped dir {row['grouped_dir_path']}", style="bold green")
        new_grouped_dir_path_input = console.input("What should the new grouped dir be? (e.g. 'coding_dh'): ")

        files_df.loc[(files_df.grouped_dir_path == row['grouped_dir_path']) & (files_df.subset_file == row['subset_file']), 'grouped_dir_path'] = new_grouped_dir_path_input
        files_df.loc[(files_df.grouped_dir_path == row['grouped_dir_path']) & (files_df.subset_file == row['subset_file']), 'grouped_column'] = grouped_column_input
        files_df.loc[(files_df.grouped_dir_path == row['grouped_dir_path']) & (files_df.subset_file == row['subset_file']), 'has_search_query'] = has_search_query
    return files_df

        

def format_file(file_path, date):
        """
        Read a CSV file and add a formatted date column.
        """
        file_df = pd.read_csv(file_path)
        file_df['coding_dh_date'] = pd.to_datetime(date)
        return file_df

def process_and_group_files(file_group, group_column, new_grouped_dir_path, has_search_query=False):
    """
    Process and group files based on their full name. The function reads files,
    formats dates, and groups the data, keeping unique entries or the oldest entry per group.

    Parameters:
    file_group (pd.DataFrame): DataFrame with file paths and related information.

    Returns:
    pd.DataFrame: Grouped and processed DataFrame.
    """

    
    console.print(f"Processing file: {file_group.iloc[0]['subset_file']}", style="bold green")
    # Process the current file
    current_date = "2024-01-13"
    existing_file = format_file(file_group.iloc[0]['file_path'], current_date)

    # Process older files
    older_files = []
    for _, row in file_group.iterrows():
        older_date = "202" + row['older_file_path'].split('_202')[1].replace("_", "-").split(".")[0]
        older_file = format_file(row['older_file_path'], older_date)
        older_files.append(older_file)

    # Combine and group files
    combined_files = pd.concat([existing_file] + older_files)

    if has_search_query:
        if 'search_query' not in combined_files.columns:
            final_path = file_group.iloc[0]['file_path'].replace(row['grouped_dir_path'], new_grouped_dir_path)
            console.print(final_path)
            if not os.path.exists(os.path.dirname(final_path)):
                os.makedirs(os.path.dirname(final_path))
            existing_file.to_csv(final_path, index=False)
            return

        combined_files['cleaned_search_query'] = combined_files.search_query.str.replace('%22', '"').str.replace('"', '').str.replace('%3A', ':').str.split('&page').str[0]

    grouped_files = combined_files.groupby(group_column)
    processed_files = []
    for name, group in grouped_files:
        if has_search_query:
            subset_columns = ['coding_dh_date', 'search_query']
        else:
            subset_columns = ['coding_dh_date']
        group = group.drop_duplicates(subset=group.columns.difference(subset_columns))
        if (group.drop(columns=subset_columns).nunique() > 1).any():
            group = group.sort_values('coding_dh_date')
            group['coding_dh_id'] = np.arange(len(group))
        else:
            group = group.sort_values('coding_dh_date').iloc[0:1]
            group['coding_dh_id'] = 0
        processed_files.append(group)

    final_df = pd.concat(processed_files).reset_index(drop=True)
    final_path = file_group.iloc[0]['file_path'].replace(row["grouped_dir_path"], new_grouped_dir_path)
    console.print(final_path)
    if not os.path.exists(os.path.dirname(final_path)):
        os.makedirs(os.path.dirname(final_path))
    final_df.to_csv(final_path, index=False)

if '__main__' == __name__:

    # Get the file paths
    files_df = group_file_paths()
    files_df = annotate_file_paths(files_df)
    # tqdm.pandas(desc="Processing files")
    # # Apply the function to the grouped DataFrame
    # files_df.groupby('subset_file').progress_apply(process_and_group_files)
