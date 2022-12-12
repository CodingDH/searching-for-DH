from rich import print
from rich.console import Console
import json
import codecs
import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
import requests
import apikey
import sys
sys.path.append("..")
from data_generation_scripts.utils import *


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}


def get_dh_terms():
    #Get the list of terms to search for
    dh_df = pd.DataFrame([json.load(codecs.open('../data/metadata_files/en.Digital humanities.json', 'r', 'utf-8-sig'))])
    dh_df = dh_df.melt()
    dh_df.columns = ['language', 'term']
    # Combine German and English terms because of identical spelling (should maybe make this a programatic check)
    dh_df.loc[dh_df.language == 'de', 'language'] = 'de_en'

    humanities_df = pd.DataFrame([json.load(codecs.open('../data/metadata_files/additional_humanities.json', 'r', 'utf-8-sig'))])
    humanities_df = humanities_df.melt()
    humanities_df.columns = ['language', 'term']
    # Combine German and English terms because of identical spelling (should maybe make this a programatic check)
    humanities_df.loc[humanities_df.language == 'de', 'language'] = 'de_en'
    terms_df = pd.concat([dh_df, humanities_df])
    return terms_df

def year_queries(search_url, dh_term, language):
    
    search_queries_dfs = []
    params = "&per_page=100&page=1"
    first_year = 2008
    current_year = datetime.now().year
    current_day = datetime.now().day
    current_month = datetime.now().month
    years = list(range(first_year, current_year+1))
    for year in years:
        if year == current_year:
            query = search_url + f"%22{dh_term}%22+created%3A{year}-01-01..{year}-{current_month}-{current_day}+sort:created{params}"
            results_dict = {'term': dh_term, 'language': language}
            results_dict['search_query'] = query
            search_queries_dfs.append(pd.DataFrame([results_dict]))
        else:
            query = search_url + f"%22{dh_term}%22+created%3A{year}-01-01..{year}-12-31+sort:created{params}"
            results_dict = {'term': dh_term, 'language': language}
            results_dict['search_query'] = query
            search_queries_dfs.append(pd.DataFrame([results_dict]))
    return search_queries_dfs

def generate_search_queries_archive(load_existing_data):
    if load_existing_data:
        query_df = pd.read_csv('../data/metadata_files/search_queries_archive.csv')
    else:
        queries_dfs = []
        terms_df = get_dh_terms()
        terms_progress_bar = tqdm(total=len(terms_df), desc="Generating Search Queries Archive", position=0)
        for _, row in terms_df.iterrows():
            terms_progress_bar.update(1)
            results_dict = {'term': row.term, 'language': row.language}
            search_query = row.term.replace(' ', '+')
            search_topics_query = "https://api.github.com/search/topics?q=" + search_query
            results_dict = {'term': row.term, 'language': row.language}
            results_dict['search_query'] = search_topics_query
            queries_dfs.append(pd.DataFrame([results_dict]))

            response = requests.get(search_topics_query, headers=auth_headers)
            data = get_response_data(response, search_topics_query)
            if data['total_count'] > 0:
                for item in data['items']:
                    # dh_term = item['name']
                    # Topics are joined by hyphens rather than plus signs in queries
                    tagged_query = item['name'].replace(' ', '-')
                    repos_tagged_query = "https://api.github.com/search/repositories?q=topic:" + tagged_query + "&per_page=100&page=1"
                    total_tagged_results = check_total_results(repos_tagged_query)
                    if total_tagged_results > 0:
                        if total_tagged_results > 1000:
                            search_url = "https://api.github.com/search/repositories?q=topic:"
                            language = row.language
                            years = year_queries(search_url, tagged_query, language)
                            queries_dfs.extend(years)
                        else:
                            results_dict = {'term': row.term, 'language': row.language}
                            results_dict['search_query'] = repos_tagged_query
                            queries_dfs.append(pd.DataFrame([results_dict]))

            search_repos_query = "https://api.github.com/search/repositories?q=" + search_query + "&per_page=100&page=1"
            total_search_results = check_total_results(search_repos_query)
            if total_search_results > 0:
                if total_search_results > 1000:
                    search_url = "https://api.github.com/search/repositories?q="
                    language = row.language
                    years = year_queries(search_url, search_query, language)
                    queries_dfs.extend(years)
                else:
                    results_dict = {'term': row.term, 'language': row.language}
                    results_dict['search_query'] = search_repos_query
                    queries_dfs.append(pd.DataFrame([results_dict]))

            search_users_query = "https://api.github.com/search/users?q=" + search_query + "&per_page=100&page=1"
            total_user_search_results = check_total_results(search_users_query)
            if total_user_search_results > 0:
                if total_user_search_results > 1000:
                    search_url = "https://api.github.com/search/users?q="
                    language = row.language
                    years = year_queries(search_url, search_query, language)
                    queries_dfs.extend(years)
                else:
                    results_dict = {'term': row.term, 'language': row.language}
                    results_dict['search_query'] = search_users_query
                    queries_dfs.append(pd.DataFrame([results_dict]))
        terms_progress_bar.close()
        query_df = pd.concat(queries_dfs)
        
        query_df.to_csv('../data/metadata_files/search_queries_archive.csv', index=False)
    return query_df

# load_existing_data = True
# query_df = generate_search_queries_archive(load_existing_data)
# query_df = query_df[query_df.search_query.notna()]
# query_df.loc[query_df.term == 'Digital+Humanities', 'term'] = 'Digital Humanities'
# avoid_terms = ['มนุษยศาสตร์ดิจิทัล', 'デジタル・ヒューマニティーズ']
# double_check_queries = query_df[(query_df.term.str.contains(' ')==False) & (query_df.term.isin(avoid_terms) == False)]
# user_df = pd.read_csv("../data/entity_files/users_dataset.csv")
# repo_df = pd.read_csv("../data/large_files/entity_files/repos_dataset.csv", low_memory=False)
# search_queries_repo_join_df = pd.read_csv("../data/join_files/search_queries_repo_join_dataset.csv")
# search_queries_user_join_df = pd.read_csv("../data/join_files/search_queries_user_join_dataset.csv")
# org_members_df = pd.read_csv('../data/join_files/org_members_dataset.csv')
# filter_searched_repos = search_queries_repo_join_df.copy()
# grouped_repo_queries_df = filter_searched_repos.groupby(['id', 'full_name']).size().reset_index(name='counts')
# filter_searched_repos['keep_repo'] = False
# filter_searched_repos.loc[filter_searched_repos['id'].isin(grouped_repo_queries_df[grouped_repo_queries_df['counts'] > 1]['id']), 'keep_repo'] = True
# filter_searched_repos.loc[filter_searched_repos['owner.login'].isin(search_queries_user_join_df.login), 'keep_repo'] = True
# filter_searched_repos.loc[filter_searched_repos['owner.login'].isin(org_members_df.login), 'keep_repo'] = True
# filter_searched_repos.loc[(filter_searched_repos.search_query.isin(double_check_queries.search_query) == False) & (filter_searched_repos.keep_repo == False), 'keep_repo'] = True

console = Console()
search_queries_repo_df = pd.read_csv('../data/join_files/search_queries_repo_join_dataset.csv')
search_queries_user_df = pd.read_csv('../data/join_files/search_queries_user_join_dataset.csv')

subset_search_queries_repo_df = search_queries_repo_df[search_queries_repo_df.natural_language != 'en']
subset_search_queries_user_df = search_queries_user_df[search_queries_user_df.natural_language != 'en']

repo_languages = subset_search_queries_repo_df.language.unique().tolist()
user_languages = subset_search_queries_user_df.language.unique().tolist()

for language in repo_languages:
    console.print(f"[bold red]Repo[/] [bold blue]{language}[/]")
    for index, row in subset_search_queries_repo_df[subset_search_queries_repo_df.language == language].iterrows():
        console.print(f"[bold cyan]{row.full_name}[/]", row.description, row.search_query, row.html_url)
        response = console.input(f"Keep repo?")
        if response == 'n':
            subset_search_queries_repo_df.drop(index, inplace=True)
        
for language in user_languages:
    console.print(f"[bold red]User[/] [bold blue]{language}[/]")
    for index, row in subset_search_queries_user_df[subset_search_queries_user_df.language == language].iterrows():
        console.print(f"[bold cyan]{row.login}[/]", row.description, row.search_query, row.html_url)
        response = console.input(f"Keep user?")
        if response == 'n':
            subset_search_queries_user_df.drop(index, inplace=True)

final_search_queries_repo_df = pd.concat([search_queries_repo_df[search_queries_repo_df.natural_language == 'en'], subset_search_queries_repo_df])
final_search_queries_user_df = pd.concat([search_queries_user_df[search_queries_user_df.natural_language == 'en'], subset_search_queries_user_df])

final_search_queries_repo_df.to_csv('../data/join_files/search_queries_repo_join_dataset.csv', index=False)
final_search_queries_user_df.to_csv('../data/join_files/search_queries_user_join_dataset.csv', index=False)


# # test_df['keep_repo'] = True
# for index, row in filter_searched_repos.iterrows():
#     if row.keep_repo:
#         continue
#     else:

#         console.print(f"[bold cyan]{row.full_name}[/]", row.description, row.search_query)
#         response = console.input(f"Keep repo?")
#         if response == 'y':
#             filter_searched_repos.loc[index, 'keep_repo'] = True
#             # test_df.drop(index, inplace=True)

# filter_searched_repos.to_csv('../data/derived_files/cleaned_search_queries_repo_join_dataset.csv', index=False)

# if __name__ == "__main__":
#     search_queries_archive_df = generate_search_queries_archive(load_existing_data=False)
    # search_queries_archive_df.to_csv('../data/metadata_files/search_queries_archive.csv', index=False)
    # print(search_queries_archive_df)
