# Scripts

Current Order:

1. `generate_translations.py` to generate the translations for the search terms.

This script uses the Google Cloud Translate API to translate a list of terms into all ISO 639-1 languages. It requires a series of terms to translate, and uses ISO 639-1 codes to generate the translations, along with data from wikimedia to get directionality of languages. The script assumes that you will at the very least be searching for DH, though you can include other terms in the `target_terms` variable. But it will subset out the data for DH separate from the rest of the terms. All data gets saved to either metadata or derived data folders in the `datasets` repository. The script also checks the detected language of a text using the Google Cloud Translate API.
   
The script assumes that you have a valid Google Cloud Translate API key and that it's saved in a specific location, which is loaded at the beginning of the script. It also assumes that you have a list of ISO 639-1 languages and a list of language directionality from Wikimedia. If these files do not exist in the specified directory, the script will attempt to scrape the language directionality data from Wikimedia. The script also assumes that the user wants to translate the term "Digital Humanities" along with other terms specified in the target_terms list. The script can be rerun by setting the rerun_code variable to True.

2. `generate_expanded_search_data.py` to generate the initial searched data using GitHub's search API
   
This script uses the GitHub search API to fetch and process data relevant to our target terms. It includes several functions that handle different aspects of this process, such as fetching data, processing search data, combining dataframes, and preparing terms and directories. It assumes that a GitHub API key is available and correctly loaded into the script. The script also assumes that the data directory paths provided exist and are accessible.
   
The main function, `get_initial_search_datasets`, orchestrates the entire process. It first checks the rate limit of the GitHub API and then proceeds to fetch and process data related to repositories, users, and organizations. The data is then saved to specified paths. If the `load_existing_data` flag is set to True, the function will attempt to load existing data from the specified paths instead of fetching new data. The script also handles errors , logging them to a CSV file for later review. It's important to note that the script is designed to handle large datasets and implements rate limiting to avoid exceeding the GitHub API's usage limits.

3. `check_clean_search_results.py` to check and finalize language of our initial search results.

This script focuses on cleaning, detecting, and finalizing our language results for our initial search results and filtering out any results that are not relevant to our questions. The script includes several functions that handle different aspects of this process, such as identifying entries that need checking, processing these entries, and double-checking languages.

The main functionality of the script is to process the DataFrames to identify entries where the 'finalized_language' is not set and the 'keep_resource' is either not set or True. These entries are then further processed and potentially updated based on manual user input (i.e. you will need to determine how to assess the GitHub entity). The script also handles cases where there are multiple entries for the same repository or user with different 'finalized_language' values, prompting the user to specify the correct language. The updated DataFrames are then saved back to their respective CSV files.

4. `check_search_results.py` to check the language detection and search results (particularly for repos with no size)
5. `generate_repo_users_interactions.py` to generate the repo users interactions (use the relevant Notebook)
6. `generate_repo_users_interactions.py` to generate the repo users interactions (use the relevant Notebook)