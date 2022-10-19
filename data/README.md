# Data Files Description

## Data Files

Three types of files:
1. Repo or User or Org files
2. Join files
3. Additional metadata files for cleaning and analysis

### Repo or User or Org files

These files contain all unique list of repos and users identified, and also any expanded metadata for those entities. These files are all stored in the `entity_files` folder.

1. `repos_dataset.csv`
2. `users_dataset.csv`
3. `orgs_dataset.csv`

### Join files

These files are our join tables. They are all stored in the `join_files` folder.

#### Repo User Interactions

1. `repo_forks_join_dataset.csv`
2. `search_queries_join_dataset.csv`
3. `repo_issues_join_dataset.csv`
4. `repo_contributors_join_dataset.csv`
5. `repo_stargas_join_dataset.csv`
6. `repo_subscribers_join_dataset.csv`

(Could still get PRs and Pulls, also comments and commits?)

#### User Repos Interactions

1. `user_repos_join_dataset.csv`
2. `user_subscribers_join_dataset.csv`

#### User User Interactions

1. `user_followers_join_dataset.csv`
2. `user_following_join_dataset.csv`

#### Org User Interactions

1. `org_users_join_dataset.csv`
2. `org_repos_join_dataset.csv`

### Additional metadata files for cleaning and analysis

These files are stored in the `metadata_files` folder.

1. `en.Digital humanities.json` - a list of DH terms in multiple languages that was downloaded from <https://github.com/WeKeyPedia/convergences>
2. `repo_url_cols.csv` - a dataset for each of the repo user interactions that contains a list of relevant columns and whether there is a count field in the API for that interaction. We use this in the `generate_repo_user_interactions.py` script.
3. `users_dataset_cols.csv` - a dataset for the relevant user columns we want to keep in our `users_dataset.csv` file. Otherwise we get a lot of additional columns for auth users (i.e us) when we pull the data, which creates dozens of nulls.
4. `repo_headers.csv` - a dataset containing just the column headers for our `repo_dataset.csv` so that we can use it to create empty DataFrame to append to.
5. `iso_639_choices.csv` - a dataset that takes our language codes from `en.Digital humanities.json` and converts them to the full language names.


## Directory Structure

Currently, the directory structure is as follows:

```bash
├── data
│   ├── README.md
│   ├── entity_files
│   │   ├── orgs_dataset.csv
│   │   ├── repos_dataset.csv
│   │   └── users_dataset.csv
│   ├── join_files
│   │   ├── org_repos_join_dataset.csv
│   │   ├── org_users_join_dataset.csv
│   │   ├── repo_contributors_join_dataset.csv
│   │   ├── repo_forks_join_dataset.csv
│   │   ├── repo_issues_join_dataset.csv
│   │   ├── repo_stargazers_join_dataset.csv
│   │   ├── repo_subscribers_join_dataset.csv
│   │   ├── search_queries_join_dataset.csv
│   │   ├── user_followers_join_dataset.csv
│   │   ├── user_following_join_dataset.csv
│   │   ├── user_repos_join_dataset.csv
│   │   └── user_subscribers_join_dataset.csv
│   └── metadata_files
│       ├── en.Digital humanities.json
│       ├── iso_639_choices.csv
│       ├── repo_headers.csv
│       └── repo_url_cols.csv 
│   └── archived_files (not currently used)
│   └── repo_data (original repo data)
│   └── temp (where you can store temp files)
│   └── error_logs (where you can store error logs)
│   └── large_datasets (where you can store large datasets that will not get pushed up and live in Google Drive)
```
