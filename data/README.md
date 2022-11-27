# Data Files Description

## Data Files

Four types of files:
1. Repo or User or Org files
2. Join files
3. Additional metadata files for cleaning and analysis
4. Derived files

### Repo or User or Org files

These files contain all unique list of repos and users identified, and also any expanded metadata for those entities. These files are all stored in the `entity_files` folder.

1. `repos_dataset.csv`
2. `users_dataset.csv`
3. `orgs_dataset.csv`

### Join files

These files are our join tables. They are all stored in the `join_files` folder.

#### Search Interactions

1. `search_queries_repo_join_dataset.csv`
2. `search_queries_user_join_dataset.csv`

#### Repo User Interactions

1. `repo_forks_join_dataset.csv`
2. `repo_issues_join_dataset.csv`
3. `repo_contributors_join_dataset.csv`
4. `repo_stargazers_join_dataset.csv`
5. `repo_subscribers_join_dataset.csv`
6. `repo_issues_join_dataset.csv`
7. `issues_comments_join_dataset.csv`
8. `repo_pulls_join_dataset.csv`
9. `pulls_comments_join_dataset.csv`
10. `repo_commits_join_dataset.csv`

(Could still get PRs and Pulls, also comments and commits?)

#### User Repos Interactions

1. `user_repos_join_dataset.csv`
2. `user_starred_join_dataset.csv`

#### User User Interactions

1. `user_followers_join_dataset.csv`
2. `user_following_join_dataset.csv`

#### User Org Interactions

1. `user_orgs_join_dataset.csv`

#### Org User Interactions

1. `org_members_join_dataset.csv`
   <!-- should change this to org_users_join_dataset.csv -->
2. `org_repos_join_dataset.csv`

### Additional metadata files for cleaning and analysis

These files are stored in the `metadata_files` folder.

1. `en.Digital humanities.json` - a list of DH terms in multiple languages that was downloaded from <https://github.com/WeKeyPedia/convergences>
2. `repo_url_cols.csv` - a dataset for each of the repo user interactions that contains a list of relevant columns and whether there is a count field in the API for that interaction. We use this in the `generate_repo_user_interactions.py` script.
3. `users_dataset_cols.csv` - a dataset for the relevant user columns we want to keep in our `users_dataset.csv` file. Otherwise we get a lot of additional columns for auth users (i.e us) when we pull the data, which creates dozens of nulls.
4. `repo_headers.csv` - a dataset containing just the column headers for our `repo_dataset.csv` so that we can use it to create empty DataFrame to append to.
5. `org_headers.csv` - a dataset containing just the column headers for our `org_dataset.csv` so that we can use it to create empty DataFrame to append to.
6. `iso_639_choices.csv` - a dataset that takes our language codes from `en.Digital humanities.json` and converts them to the full language names.
7. `search_repo_headers.csv` - a dataset containing just the column headers for our `search_repo_dataset.csv` so that we can use it to create empty DataFrame to append to.
8. `search_user_headers.csv` - a dataset containing just the column headers for our `search_user_dataset.csv` so that we can use it to create empty DataFrame to append to.
9. `excluded_users.csv` - a dataset containing a list of users that we want to exclude from our analysis. This is because they are either bots or they are not relevant to our analysis. This is a manual process, and we will need to update this list as we go along.

### Derived Files

These are files where we have made some interpretation of the data and added some additional columns. These files are stored in the `derived_files` folder.

1. `core_users.csv` - a dataset that contains a list of users that are core to our analysis. The thresholding is detailed in our notebooks.
2. `core_repos.csv` - a dataset that contains a list of repos that are core to our analysis. The thresholding is detailed in our notebooks.
<!-- May want to add dataset at the end of these files for naming consistency -->

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
│   └── user_data (original user data)
│   └── temp (where you can store temp files)
│   └── error_logs (where you can store error logs)
│   └── derived_files
│   └── large_files (where you can store large datasets that will not get pushed up and live in Google Drive)
│       └── entity_files
│       └── join_files
│       └── archived_files
│   └── older_files (where timestamped versions of datasets will live)
│       └── entity_files
│       └── join_files
│       └── archived_files
│       └── repo_data
│       └── user_data
│       └── large_files
│           └── entity_files
│           └── join_files
```
