# Scripts

Current Order:

1. `generate_translations.py` to generate the translations for the search terms.
   This script requires a series of terms to translate, and uses ISO 639-1 codes to generate the translations, along with data from wikimedia to get directionality of languages. The script assumes that you will at the very least be searching for DH, though you can include other terms in the `target_terms` variable. But it will subset out the data for DH separate from the rest of the terms. All data gets saved to either metadata or derived data folders in the `datasets` repository.
2. `generate_search_data.py` OR `generate_expanded_search_data.py` (depending on if you want multiple language and term results)
3. `generate_translations.py` to detect languages in the repositories
4. `check_search_results.py` to check the language detection and search results (particularly for repos with no size)
5. `generate_repo_users_interactions.py` to generate the repo users interactions (use the relevant Notebook)
6. `generate_repo_users_interactions.py` to generate the repo users interactions (use the relevant Notebook)