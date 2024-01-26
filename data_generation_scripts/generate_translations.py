# Standard library imports
import codecs
import html
import json
import os
import time
import warnings

# Local application/library specific imports
import apikey
# Related third-party imports
import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import translate_v2 as translate
from google.oauth2 import service_account
from rich.console import Console
from tqdm import tqdm

warnings.filterwarnings('ignore')
# Load Google Cloud credentials. You can get your own credentials by following the instructions here: https://cloud.google.com/translate/docs/setup and saving them with apikey.save("GOOGLE_TRANSLATE_CREDENTIALS", "path/to/your/credentials.json")
key_path = apikey.load("GOOGLE_TRANSLATE_CREDENTIALS")
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

translate_client = translate.Client(credentials=credentials)

console = Console()

def check_detect_language(row: pd.Series, is_repo:bool=False) -> pd.Series:
    """
    Checks the detected language of a row of text using Google Cloud Translate API

    Parameters
    ----------
    row : pd.Series
        A row of a dataframe with a text column
    is_repo : bool
        A boolean indicating whether the text is a repo description or a bio
    Returns a series with the detected language and confidence score
    """
    text = row.description if is_repo else row.bio
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
        console.print(f"Error translating {dh_term} in {target_language}: {e}", style="bold red")
        row['translated_term'] = None
    return row

def generate_initial_terms(target_terms: list, data_directory_path: str, dh_exists: bool) -> pd.DataFrame:
    """
    Function to generate a dataframe with translated terms. This function assumes you want to at the very least translate Digital Humanities.

    Parameters
    ----------
    target_terms : list
        A list of terms to translate in all ISO 639-1 languages
    data_directory_path : str
        A path to the directory with the datasets
    dh_exists : bool
        A boolean indicating whether Digital Humanities terms already exist
    Returns a dataframe with translated terms
    """
    if dh_exists:
        # Load the existing translations of DH terms that were found on GitHub
        dh_df = pd.DataFrame([json.load(codecs.open(
            f'{data_directory_path}/metadata_files/en.Digital humanities.json', 'r', 'utf-8-sig'))])
        dh_df = dh_df.melt()
        dh_df.columns = ['language', 'term']
        # Load in all ISO 639-1 languages
        iso_languages = pd.read_csv(f"{data_directory_path}/metadata_files/iso_639_choices.csv")
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
    if dh_exists:
        languages_dfs.append(merged_dh)
    final_df = pd.concat(languages_dfs)
    final_df = final_df.reset_index(drop=True)  
    # Translate the terms
    tqdm.pandas(desc="Translating terms")
    translated_terms = final_df.progress_apply(get_translation, axis=1)

    # Subset to just the terms that were translated
    cleaned_dh = translated_terms[(translated_terms.translated_term.notna())]
    # Deal with the German edge case
    cleaned_dh.loc[(cleaned_dh.term.notna()) & (
    cleaned_dh.language == 'de'), 'term'] = cleaned_dh.translated_term

    # Otherwise use the original term
    cleaned_dh.loc[(cleaned_dh.term.isna()), 'term'] = cleaned_dh.translated_term

    # Deal with character encoding
    cleaned_dh['term'] = cleaned_dh['term'].apply(lambda x: html.unescape(x))
    cleaned_dh['translated_term'] = cleaned_dh['translated_term'].apply(lambda x: html.unescape(x))
    cleaned_dh = cleaned_dh.rename(columns={'language': 'code'})
    return cleaned_dh

def generate_translated_terms(data_directory_path: str, directionality_df: pd.DataFrame, target_terms: list, rerun_code: bool) -> pd.DataFrame:
    """
    Function to generate a dataframe with translated terms. This function assumes you want to at the very least translate Digital Humanities, along with other terms.

    Parameters
    ----------
    data_directory_path : str
        A path to the directory with the datasets
    directionality_df : pd.DataFrame
        A dataframe with language directionality
    target_terms : list
        A list of terms to translate in all ISO 639-1 languages
    rerun_code : bool
        A boolean indicating whether to rerun the code

    Returns a dataframe with translated terms
    """
    cleaned_terms_output_path = f"{data_directory_path}/derived_files/cleaned_translated_terms.csv"

    grouped_terms_output_path = f"{data_directory_path}/derived_files/grouped_cleaned_translated_terms.csv"
    # Load the existing datasets if they exist and rerun_code is False
    if os.path.exists(cleaned_terms_output_path) and os.path.exists(grouped_terms_output_path) and (not rerun_code):
        cleaned_terms_df = pd.read_csv(cleaned_terms_output_path)
        grouped_terms_df = pd.read_csv(grouped_terms_output_path)
    else:
        # Load existing translations if they exist
        if os.path.exists(cleaned_terms_output_path):
            console.print("Loading existing cleaned terms", style="bold green")
            cleaned_terms_df = pd.read_csv(cleaned_terms_output_path)
            existing_target_terms = cleaned_terms_df.term_source.unique().tolist()
            # Handle Digital Humanities slightly differently because we generate the translations from existing files from other scholars
            dh_exists = ['Digital Humanities' in term for term in existing_target_terms]
            dh_exists = True if len(dh_exists) > 0 else False
            # Find any target terms
            updated_target_terms = [term for term in target_terms if term not in existing_target_terms]
            console.print(f"Missing terms: {updated_target_terms}", style="bold red")
        else:
            # If no existing translations, all target terms are considered "missing"
            cleaned_terms_df = pd.DataFrame()
            updated_target_terms = target_terms
            dh_exists = True if "Digital Humanities" in target_terms else False
        console.print(f"Generating initial terms for {updated_target_terms}", style="bold green")
        # Generate initial terms for any updated_target_terms
        if len(updated_target_terms) > 0:
            new_cleaned_terms_df = generate_initial_terms(updated_target_terms, data_directory_path, dh_exists)
            if ('code' not in cleaned_terms_df.columns) and len(cleaned_terms_df) > 0:
                cleaned_terms_df = cleaned_terms_df.rename(columns={'language': 'code'})
            cleaned_terms_df = pd.concat([cleaned_terms_df, new_cleaned_terms_df])
            # Save the cleaned terms
            cleaned_terms_df.to_csv(f'{cleaned_terms_output_path}', index=False)
        cleaned_terms_df = pd.read_csv(f'{cleaned_terms_output_path}')
        # Subset directionality to LTR and RTL languages
        directionality_df = directionality_df[directionality_df.directionality.isin(['ltr', 'rtl'])]

        # Merge the directionality data with the cleaned terms
        merged_lang_terms_df = pd.merge(directionality_df[['code', 'directionality', 'English language name', 'local language name']], cleaned_terms_df, on='code', how="outer")
        merged_lang_terms_df = merged_lang_terms_df[merged_lang_terms_df.code != "see also Test languages"]

        console.print(f"Our data now contains info for {merged_lang_terms_df[merged_lang_terms_df.term.notna()]['English language name'].nunique()} but we also are missing terms for the following number of languages {merged_lang_terms_df[merged_lang_terms_df.term.isna()]['English language name'].nunique()}", style="bold green")

        grouped_terms_df = merged_lang_terms_df.groupby(['term_source','term']).agg({
            'code': ','.join, 
            'term': 'count', 
            'English language name': ', '.join, 
            'directionality': lambda x: ','.join(set(x))
        }).reset_index(level=0)
        grouped_terms_df = grouped_terms_df.rename(columns={'term': 'counts'})
        grouped_terms_df['term'] = grouped_terms_df.index
        grouped_terms_df = grouped_terms_df.reset_index(drop=True)
        grouped_terms_df['directionality_counts'] = grouped_terms_df.directionality.str.split(',').str.len()
        needs_directional_specifications = grouped_terms_df[grouped_terms_df.directionality_counts > 1]       
        for index, row in needs_directional_specifications.iterrows():
            console.print(f"Need to specify directionality for {row.term} in {row['English language name']}", style="bold blue")
            languages = row['English language name'].split(', ')
            for direction in ['ltr', 'rtl']:
                console.print(f"Directionality: {direction}", style="bold green")
                total_directionalities = directionality_df[(directionality_df['English language name'].isin(languages)) & (directionality_df.directionality == direction)]
                console.print(f"Total languages with directionality {direction}: {len(total_directionalities)}", style="bold green")
                console.print(f"Languages: {total_directionalities['English language name'].tolist()}", style="bold blue")
            input_directionality = console.input("Enter directionality (ltr or rtl): ")
            grouped_terms_df.loc[index, 'directionality'] = input_directionality
        grouped_terms_df = grouped_terms_df.drop(columns=['directionality_counts'])
        grouped_terms_df.to_csv(f'{grouped_terms_output_path}', index=False)
    return cleaned_terms_df, grouped_terms_df

if __name__ == '__main__':
    local_data_directory_path = "../../datasets" # Change this to the path to your datasets directory
    existing_directionality_path = f"{local_data_directory_path}/metadata_files/iso_639_choices_directionality_wikimedia.csv"
    local_target_terms: list = ["Humanities", "Public History", "Digital History", "Digital Cultural Heritage", "Cultural Analytics", "Computational Humanities", "Computational Social Science"]
    should_rerun_code = True
    existing_directionality_df = get_directionality(existing_directionality_path)
    generate_translated_terms(local_data_directory_path, existing_directionality_df, local_target_terms, should_rerun_code)
