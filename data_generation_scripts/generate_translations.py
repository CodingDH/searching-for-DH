import time
from tqdm import tqdm
import json
import pandas as pd
import codecs
import apikey
import requests
from bs4 import BeautifulSoup
import lxml
import html


from google.cloud import translate_v2 as translate
from google.oauth2 import service_account

key_path = apikey.load("GOOGLE_TRANSLATE_CREDENTIALS")
key_path = key_path.replace('/Volumes/Samsung_T5/','/Users/zleblanc/')
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

translate_client = translate.Client(
    credentials=credentials)

def get_directionality():
    url = "https://meta.wikimedia.org/wiki/Template:List_of_language_names_ordered_by_code"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('table')[0]
    df = pd.read_html(str(table))[0]
    df.to_csv(
        "../data/metadata_files/iso_639_choices_directionality_wikimedia.csv", index=False)
    return df


def get_translation(row):
    try:
        dh_term = row.term_source
        target_language = row.language
        text_result = translate_client.translate(
            dh_term, target_language=target_language)
        row['translated_term'] = text_result['translatedText']
    except:
        row['translated_term'] = None
    return row

def generate_translated_terms():
    dh_df = pd.DataFrame([json.load(codecs.open(
        '../data/metadata_files/en.Digital humanities.json', 'r', 'utf-8-sig'))])
    dh_df = dh_df.melt()
    dh_df.columns = ['language', 'term']
    iso_languages = pd.read_csv("../data/metadata_files/iso_639_choices.csv")
    iso_languages = iso_languages.rename(
    columns={'name': 'language_name'})
    merged_dh = pd.merge(dh_df, iso_languages, on='language', how='outer')
    merged_dh['term_source'] = 'Digital Humanities'
    target_terms = ["Humanities", "Public History", "Digital History",
                    "Digital Cultural Heritage", "Cultural Analytics", "Computational Humanities"]
    languages_dfs = []
    for term in target_terms:
        humanities_df = iso_languages.copy()
        humanities_df['term_source'] = term
        languages_dfs.append(humanities_df)
    languages_dfs.append(merged_dh)
    final_df = pd.concat(languages_dfs)
    final_df = final_df.reset_index(drop=True)
    tqdm.pandas("Translating")
    final_df = final_df.progress_apply( get_translation, axis=1)
    final_df.to_csv(
        "../data/derived_files/translated_dh_terms.csv", index=False)

    directionality_df = get_directionality()
    merged_df = pd.merge(directionality_df[['code', 'directionality', 'English language name',
             'local language name']], final_df, on='code', how="left")
    merged_df['term'] = merged_df['term'].apply(lambda x: html.unescape(x))
    merged_df['translated_term'] = merged_df['translated_term'].apply(lambda x: html.unescape(x))
    return merged_df

def check_detect_language(row, is_repo=False):
    text = row.description if is_repo else row.bio
    # if len(text) == 0:
    #     text = row.name.replace('-', ' ').replace('_', ' ')
    if pd.notna(text) and len(text) > 1:  # Additional check if text is not NaN
        try:
            result = translate_client.detect_language(text)
            row['detected_language'] = result['language']
            row['detected_language_confidence'] = result['confidence']
        except:
            row['detected_language'] = None
            row['detected_language_confidence'] = None
    else:
        row['detected_language'] = None
        row['detected_language_confidence'] = None
    return row


if __name__ == '__main__':
    initial_repo_output_path = "../data/repo_data/"
    repo_output_path = "../data/large_files/entity_files/repos_dataset.csv"
    repo_join_output_path = "../data/large_files/join_files/search_queries_repo_join_dataset.csv"

    initial_user_output_path = "../data/user_data/"
    user_output_path = "../data/entity_files/users_dataset.csv"
    user_join_output_path = "../data/join_files/search_queries_user_join_dataset.csv"
    search_queries_repo_join_dataset = pd.read_csv(repo_join_output_path)
    search_queries_user_join_dataset = pd.read_csv(user_join_output_path)
    user_df = pd.read_csv(user_output_path)
    user_cols = pd.read_csv('../data/metadata_files/users_dataset_cols.csv')
    user_df =user_df[user_cols.columns]
    subset_dh_repos = search_queries_repo_join_dataset[search_queries_repo_join_dataset['search_term_source'] == 'Digital Humanities']
    subset_dh_users = search_queries_user_join_dataset[search_queries_user_join_dataset['search_term_source'] == 'Digital Humanities']
    cols = list(set(subset_dh_users.columns) & set(user_cols.columns))
    joined_users = pd.merge(subset_dh_users, user_df, on=cols, how='left')

    tqdm.pandas(desc='Detecting language')
    subset_dh_repos.description = subset_dh_repos.description.fillna('')
    subset_dh_repos = subset_dh_repos.progress_apply(check_detect_language, axis=1, is_repo=True)
    joined_users.bio = joined_users.bio.fillna('')
    joined_users = joined_users.progress_apply(check_detect_language, axis=1, is_repo=False)
    subset_dh_repos.to_csv('../data/derived_files/search_queries_repo_join_subset_dh_dataset.csv', index=False)
    joined_users.to_csv('../data/derived_files/search_queries_user_join_subset_dh_dataset.csv', index=False)
