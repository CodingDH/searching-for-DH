from rich import print
from rich.console import Console
import pandas as pd
import os


final_missing_users_df = pd.read_csv(
    "../data/derived_files/missing_users_for_cleaning.csv")
final_missing_combined_counts = pd.read_csv(
    "../data/derived_files/missing_users_combined_counts.csv")

contributors_df = pd.read_csv(
    '../data/join_files/repo_contributors_join_dataset.csv', low_memory=False)
forks_df = pd.read_csv('../data/join_files/repo_forks_join_dataset.csv', low_memory=False)
stargazers_df = pd.read_csv(
    '../data/join_files/repo_stargazers_join_dataset.csv', low_memory=False)
subscribers_df = pd.read_csv(
    '../data/join_files/repo_subscribers_join_dataset.csv', low_memory=False)
issues_df = pd.read_csv(
    '../data/large_files/join_files/repo_issues_join_dataset.csv', low_memory=False)
issue_comments_df = pd.read_csv(
    '../data/large_files/join_files/issues_comments_join_dataset.csv', low_memory=False)
pulls_df = pd.read_csv(
    '../data/large_files/join_files/repo_pulls_join_dataset.csv', low_memory=False)
pull_comments_df = pd.read_csv(
    '../data/large_files/join_files/pulls_comments_join_dataset.csv', low_memory=False)
console = Console()
files_dict = {
    'contributor': contributors_df,
    'fork': forks_df,
    'stargazer': stargazers_df,
    'subscriber': subscribers_df,
    'issue': issues_df,
    'issue_comment': issue_comments_df,
    'pull': pulls_df,
    'pull_comment': pull_comments_df
}
login_dict = {
    'contributor': 'login',
    'fork': 'owner.login',
    'stargazer': 'user.login',
    'subscriber': 'login',
    'issue': 'user.login',
    'issue_comment': 'user.login',
    'pull': 'user.login',
    'pull_comment': 'user.login'
}
if os.path.exists('../data/derived_files/missing_users_for_cleaned.csv'):
    cleaned_users = pd.read_csv(
        '../data/derived_files/missing_users_for_cleaned.csv')
    final_missing_users_df = final_missing_users_df[~final_missing_users_df['login'].isin(
        cleaned_users['login'])]
final_missing_users_df['keep_repo'] = False
for index, row in final_missing_users_df.iterrows():
    if row.keep_repo:
        continue
    else:
        console.print(f"[bold cyan]{row.login}[/]",
                      row.bio)
        activities = final_missing_combined_counts[final_missing_combined_counts.login == row.login]
        for index, activity in activities.iterrows():
            console.print(
                f"[bold red]{activity.user_type}[/] {activity.counts}")
            subset_df = files_dict[activity.user_type]
            login = login_dict[activity.user_type]
            subset_df = subset_df[subset_df[login] == row.login]
            console.print(f"These activities are on these repos: {subset_df.repo_full_name.unique().tolist()}")

        response = console.input(f"Keep repo?")
        if response == 'y':
            final_missing_users_df.loc[index, 'keep_repo'] = True

final_missing_users_df.to_csv('../data/derived_files/missing_users_for_cleaned.csv', index=False)