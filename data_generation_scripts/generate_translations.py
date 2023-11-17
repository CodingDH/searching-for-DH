from typing import Any, Dict
import json
import pandas as pd
import codecs
import apikey
import requests
from bs4 import BeautifulSoup
from google.cloud import translate_v2 as translate
from google.oauth2 import service_account

# Load Google Cloud credentials. You can get your own credentials by following the instructions here: https://cloud.google.com/translate/docs/setup and saving them with apikey.save("GOOGLE_TRANSLATE_CREDENTIALS", "path/to/your/credentials.json")
key_path: str = apikey.load("GOOGLE_TRANSLATE_CREDENTIALS")
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

translate_client = translate.Client(credentials=credentials)

def get_directionality() -> pd.DataFrame:
    """
    Function to get language directionality from Wikimedia
    
    Returns a dataframe with language directionality
    """
    url: str = "https://meta.wikimedia.org/wiki/Template:List_of_language_names_ordered_by_code"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find_all('table')[0]
    df: pd.DataFrame = pd.read_html(str(table))[0]
    df.to_csv(
        "../data/metadata_files/iso_639_choices_directionality_wikimedia.csv", index=False)
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
    try:
        dh_term: str = row.term_source
        target_language: str = row.language
        text_result: Dict[str, Any] = translate_client.translate(
            dh_term, target_language=target_language)
        row['translated_term'] = text_result['translatedText']
    except Exception as e:
        row['translated_term'] = None
    return row

def generate_translated_terms() -> pd.DataFrame:
    """
    Function to generate a dataframe with translated terms

    Returns a dataframe with translated terms
    """
    dh_df: pd.DataFrame = pd.DataFrame([json.load(codecs.open(
        '../data/metadata_files/en.Digital humanities.json', 'r', 'utf-8-sig'))])
    dh_df = dh_df.melt()
    dh_df.columns = ['language', 'term']
    iso_languages: pd.DataFrame = pd.read_csv("../data/metadata_files/iso_639_choices.csv")
    iso_languages = iso_languages.rename(columns={'name': 'language_name'})
    merged_dh: pd.DataFrame = pd.merge(dh_df, iso_languages, on='language', how='outer')
    merged_dh['term_source'] = 'Digital Humanities'
    target_terms: list = ["Humanities", "Public History", "Digital History", ...]
    merged_dh = merged_dh[merged_dh.term.isin(target_terms)]
    translated_terms: pd.DataFrame = merged_dh.apply(get_translation, axis=1)
    return translated_terms

if __name__ == '__main__':
    get_directionality()
    generate_translated_terms()
