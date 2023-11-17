from typing import Any, Dict
import json
import pandas as pd
import codecs
import apikey
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
import os
import html
import warnings
warnings.filterwarnings('ignore')
from google.cloud import translate_v2 as translate
from google.oauth2 import service_account

# Load Google Cloud credentials. You can get your own credentials by following the instructions here: https://cloud.google.com/translate/docs/setup and saving them with apikey.save("GOOGLE_TRANSLATE_CREDENTIALS", "path/to/your/credentials.json")
key_path = apikey.load("GOOGLE_TRANSLATE_CREDENTIALS")
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

translate_client = translate.Client(credentials=credentials)

def get_directionality(directionality_path: str) -> pd.DataFrame:
    """
    Function to get language directionality from Wikimedia
    
    Parameters
    ----------
    directionality_path : str
    Returns a dataframe with language directionality
    """
    # Read in the directionality data if it exists, otherwise scrape it from Wikimedia
    if os.path.exists(directionality_path):
        df = pd.read_csv(directionality_path)
    else:
        # Get the directionality data from Wikimedia using BeautifulSoup
        url = "https://meta.wikimedia.org/wiki/Template:List_of_language_names_ordered_by_code"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find_all('table')[0]
        df = pd.read_html(str(table))[0]
        df.to_csv(
            directionality_path, index=False)
    return df

def get_translation(row: pd.Series) -> pd.Series:
    """
    Function to get a translation of a term

    Parameters
    ----------
    row : pd.Series
        A row of a dataframe with a term and a language
    Returns a series with the translated term
    """
    time.sleep(1)
    try:
        dh_term = row.term_source
        target_language = row.language
        text_result = translate_client.translate(
            dh_term, target_language=target_language)
        row['translated_term'] = text_result['translatedText']
    except Exception as e:
        print(f"Error translating {dh_term} in {target_language}: {e}")
        row['translated_term'] = None
    return row

def generate_translated_terms_for_dh_others(directory_path: str, directionality_df: pd.DataFrame, target_terms: list, rerun_code: bool) -> pd.DataFrame:
    """
    Function to generate a dataframe with translated terms. This function assumes you want to at the very least translate Digital Humanities, along with other terms.

    Parameters
    ----------
    directory_path : str
        A path to the directory with the datasets
    directionality_df : pd.DataFrame
        A dataframe with language directionality
    target_terms : list
        A list of terms to translate in all ISO 639-1 languages
    rerun_code : bool
        A boolean indicating whether to rerun the code

    Returns a dataframe with translated terms
    """
    cleaned_dh_output_path = "/derived_files/cleaned_translated_dh_terms.csv"

    grouped_dh_output_path = "/derived_files/grouped_cleaned_translated_dh_terms.csv"
    grouped_others_output_path = "/derived_files/grouped_translated_others_terms.csv"
    # Load the existing datasets if they exist and rerun_code is False
    if os.path.exists(cleaned_dh_output_path) and os.path.exists(grouped_dh_output_path) and rerun_code == False:
        cleaned_dh = pd.read_csv(cleaned_dh_output_path)
        grouped_dh_terms = pd.read_csv(grouped_dh_output_path)
    else:
        # Load the existing translations of DH terms that were found on GitHub
        dh_df = pd.DataFrame([json.load(codecs.open(
            f'{directory_path}/metadata_files/en.Digital humanities.json', 'r', 'utf-8-sig'))])
        dh_df = dh_df.melt()
        dh_df.columns = ['language', 'term']
        # Load in all ISO 639-1 languages
        iso_languages = pd.read_csv(f"{directory_path}/metadata_files/iso_639_choices.csv")
        iso_languages = iso_languages.rename(columns={'name': 'language_name'})
        # Merge the DH terms with the ISO 639-1 languages
        merged_dh = pd.merge(dh_df, iso_languages, on='language', how='outer')
        merged_dh['term_source'] = 'Digital Humanities'
        
        # Generate a dataframe with all the terms we want to translate in all ISO 639-1 languages
        languages_dfs = []
        for term in target_terms:
            humanities_df = iso_languages.copy()
            humanities_df['term_source'] = term
            languages_dfs.append(humanities_df)
        languages_dfs.append(merged_dh)
        final_df = pd.concat(languages_dfs)
        final_df = final_df.reset_index(drop=True)
        
        # Translate the terms
        tqdm.pandas(desc="Translating terms")
        translated_terms = final_df.progress_apply(get_translation, axis=1)

        # Subset to just the terms that were translated
        cleaned_dh = translated_terms[(translated_terms.translated_term.notna())]
        # Deal with the German edge case
        cleaned_dh.loc[(cleaned_dh.term.notna() == True) & (
        cleaned_dh.language == 'de'), 'term'] = cleaned_dh.translated_term

        # Otherwise use the original term
        cleaned_dh.loc[(cleaned_dh.term.isna() == True), 'term'] = cleaned_dh.translated_term

        # Deal with character encoding
        cleaned_dh['term'] = cleaned_dh['term'].apply(lambda x: html.unescape(x))
        cleaned_dh['translated_term'] = cleaned_dh['translated_term'].apply(lambda x: html.unescape(x))
        cleaned_dh = cleaned_dh.rename(columns={'language': 'code'})
        # Save the cleaned terms
        cleaned_dh.to_csv(f'{directory_path}{cleaned_dh_output_path}', index=False)

        cleaned_dh = pd.read_csv(f'{directory_path}{cleaned_dh_output_path}')
        # Subset directionality to LTR and RTL languages
        directionality_df = directionality_df[directionality_df.directionality.isin(['ltr', 'rtl'])]

        # Merge the directionality data with the cleaned terms
        merged_lang_terms = pd.merge(directionality_df[['code', 'directionality', 'English language name', 'local language name']], cleaned_dh, on='code', how="outer")
        merged_lang_terms = merged_lang_terms[merged_lang_terms.code != "see also Test languages"]
        print(f"Our data now contains info for {merged_lang_terms[merged_lang_terms.term.notna()]['English language name'].nunique()} but we also are missing terms for the following number of languages {merged_lang_terms[merged_lang_terms.term.isna()]['English language name'].nunique()}")

        # Subset to just Digital Humanities
        subset_dh = merged_lang_terms[merged_lang_terms.term_source == 'Digital Humanities']
        print(f"If we subset to just Digital Humanities to our data now contains info for {subset_dh[subset_dh.term.notna()]['English language name'].nunique()} but we also are missing terms for the following number of languages {subset_dh[subset_dh.term.isna()]['English language name'].nunique()}")

        grouped_dh_terms = subset_dh.groupby(['term_source','term']).agg({'code': ','.join, 'term': 'count', 'English language name': ', '.join }).reset_index(level=0)
        grouped_dh_terms['final_term'] = grouped_dh_terms.index
        grouped_dh_terms = grouped_dh_terms.reset_index(level=0, drop=True).sort_values(by='term', ascending=False)

        grouped_dh_terms[grouped_dh_terms.code.str.contains(',')][[ 'English language name', 'final_term']].to_csv(f'{directory_path}/derived_files/dh_terms_with_multiple_codes.csv', index=False)
        grouped_dh_terms.to_csv(f'{directory_path}{grouped_dh_output_path}', index=False)

        # Subset to non-DH terms
        subset_others = merged_lang_terms[merged_lang_terms.term_source != 'Digital Humanities']
        print(f"If we subset to just non-Digital Humanities to our data now contains info for {subset_others[subset_others.term.notna()]['English language name'].nunique()} but we also are missing terms for the following number of languages {subset_others[subset_others.term.isna()]['English language name'].nunique()}")

        grouped_others_terms = subset_others.groupby(['term_source','term']).agg({'code': ','.join, 'term': 'count', 'English language name': ', '.join }).reset_index(level=0)
        grouped_others_terms['final_term'] = grouped_others_terms.index
        grouped_others_terms = grouped_others_terms.reset_index(level=0, drop=True).sort_values(by='term', ascending=False)

        grouped_others_terms[grouped_others_terms.code.str.contains(',')][[ 'English language name', 'final_term']].to_csv(f'{directory_path}/derived_files/others_terms_with_multiple_codes.csv', index=False)

        grouped_others_terms.to_csv(f'{directory_path}{grouped_others_output_path}', index=False)
    return cleaned_dh, grouped_dh_terms

if __name__ == '__main__':
    directory_path = "../../datasets" # Change this to the path to your datasets directory
    directionality_path = f"{directory_path}/metadata_files/iso_639_choices_directionality_wikimedia.csv"
    target_terms: list = ["Humanities", "Public History", "Digital History", "Digital Cultural Heritage", "Cultural Analytics", "Computational Humanities"]
    rerun_code = True
    directionality_df = get_directionality(directionality_path)
    generate_translated_terms_for_dh_others(directory_path, directionality_df, target_terms, rerun_code)
