from rich import print
from rich.console import Console
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
import os
from typing import Optional, List
import sys
sys.path.append('..')


initial_core_users = pd.read_csv("../data/derived_files/initial_core_users.csv")
initial_core_repos = pd.read_csv("../data/derived_files/initial_core_repos.csv")
initial_core_orgs = pd.read_csv("../data/derived_files/initial_core_orgs.csv")

firstpass_core_users = pd.read_csv("../data/derived_files/firstpass_core_users.csv")
firstpass_core_repos = pd.read_csv("../data/derived_files/firstpass_core_repos.csv")
firstpass_core_orgs = pd.read_csv("../data/derived_files/firstpass_core_orgs.csv")

firstpass_core_repos = firstpass_core_repos[firstpass_core_repos.fork == 'False']
combined_core_users = pd.concat([initial_core_users, firstpass_core_users])
combined_core_repos = pd.concat([initial_core_repos, firstpass_core_repos])
combined_core_orgs = pd.concat([initial_core_orgs, firstpass_core_orgs])

contributors_df = pd.read_csv('../data/large_files/join_files/repo_contributors_join_dataset.csv')
contributors_df = contributors_df[contributors_df.repo_full_name.isin(combined_core_repos.full_name.unique())]
forks_df = pd.read_csv('../data/large_files/join_files/repo_forks_join_dataset.csv')
forks_df = forks_df[forks_df.repo_full_name.isin(combined_core_repos.full_name.unique())]
subscribers_df = pd.read_csv('../data/join_files/repo_subscribers_join_dataset.csv')
subscribers_df = subscribers_df[subscribers_df.repo_full_name.isin(combined_core_repos.full_name.unique())]

stargazers_df = pd.read_csv('../data/large_files/join_files/repo_stargazers_join_dataset.csv')
stargazers_df = stargazers_df[stargazers_df.repo_full_name.isin(combined_core_repos.full_name.unique())]

issues_df = pd.read_csv('../data/large_files/join_files/repo_issues_join_dataset.csv')
issues_df = issues_df[issues_df.repo_full_name.isin(combined_core_repos.full_name.unique())]

issue_comments_df = pd.read_csv('../data/large_files/join_files/issues_comments_join_dataset.csv')
issue_comments_df = issue_comments_df[issue_comments_df.repo_full_name.isin(combined_core_repos.full_name.unique())]

pulls_df = pd.read_csv('../data/large_files/join_files/repo_pulls_join_dataset.csv')
pulls_df = pulls_df[pulls_df.repo_full_name.isin(combined_core_repos.full_name.unique())]

pull_comments_df = pd.read_csv('../data/large_files/join_files/pulls_comments_join_dataset.csv')
pull_comments_df = pull_comments_df[pull_comments_df.repo_full_name.isin(combined_core_repos.full_name.unique())]

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

def process_results(initial_df, top_users, output_path):
    for _, row in initial_df[initial_df.keep_account != False].iterrows():
        subset_top_users = top_users[top_users.login == row.login]

        print(f"{row.login} has {row.followers} followers and are following {row.following} users. Their bio is: {row.bio}.") 
        print(f"There url is {row.html_url}. They have {row.public_repos} public repos and {row.public_gists} public gists.")
        print("The activities associated with this account include:")
        for _, second_row in subset_top_users.iterrows():
            print(f"{second_row.type}: {second_row.counts}")
            df = files_dict[second_row.type]
            login_value = login_dict[second_row.type]
            repos = df[df[login_value] == row.login].repo_full_name.unique()
            print(repos)
        keep_resource = False
        answer = console.input("stay in the dataset? (y/n)")
        if answer == 'y':
            keep_resource = True
        initial_df.loc[initial_df.login == row.login, 'keep_account'] = keep_resource
        initial_df.to_csv(output_path, index=False)

    initial_df.to_csv(output_path, index=False)


too_many_followers = pd.read_csv("../data/derived_files/too_many_followers.csv")
if 'keep_acccount' not in too_many_followers.columns:
    too_many_followers['keep_account'] = None
top_users = pd.read_csv("../data/derived_files/top_users.csv")


too_many_following = pd.read_csv("../data/derived_files/too_many_following.csv")
if 'keep_acccount' not in too_many_following.columns:
    too_many_following['keep_account'] = None
console = Console()

# process_results(too_many_followers, top_users, "../data/derived_files/too_many_followers.csv")
process_results(too_many_following, top_users, "../data/derived_files/too_many_following.csv")