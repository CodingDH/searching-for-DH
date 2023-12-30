# Notebooks

This folder contains the notebooks used to clean the data. The notebooks are numbered in the order they were used.

1. `ProcessInitialResults.ipynb` to process the initial search results and check the language detection.

This notebook checks if we are missing any of the search results from our entity files, and then creates our core files: `core_repos`, `core_users`, `core_orgs` that represent our core initial files. This should be run after `generate_translations.py`; `generate_expanded_search_data.py`; and `check_clean_search_results.py`/