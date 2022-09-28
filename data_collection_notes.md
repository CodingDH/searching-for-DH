## Place to List Overview of Data Collection


1. Repositories
   - Languages
   - Topics
   - Tags
   - Community Profile
   - Description
   - Date Created/Modified
   - Organization
   
2. Users
   - Description
   - Rate of Activity
   - Organizations

3. Organizations
   - Description

4. Interactions
    - Forks (counting number of forks per repo and who has forked what)
    - Teams (counting number of teams per repo and who is on which team)
    - Contributors (counting number of contributors per repo and who has contributed what)
      - What is a collaborator??
    - Starrers (counting number of starrers per repo and who has starred what)
    - Watchers (counting number of watchers per repo and who has watched what)
    - Followers/Following (counting number of followers per user and who is following who)
    - Issues (counting number of issues per repo and who has opened/closed what)
    - PRs (counting number of PRs per repo and who has opened/closed what)

### Entity Relationship Diagram of the Data Collection

```mermaid
erDiagram
 REPO {
  string id
  string fullname
  link url
  string description
  string language
  string topics
  string tags
  string community_profile
  string date_created
  string org_id FK
 }
 USER {
  string id
  string username
  link url
  string org_id FK
 }
ORG {
  string id
  string name
  link url
  }
 REPO }|--|{ INTERACTIONS: has
 USER }|--|{ INTERACTIONS: has
 ORG }|--|{ USER: has
 ORG }|--|{ REPO: has
 
 INTERACTIONS {
  string id
  string repo_id FK
  string user_id FK
  string type_of_interaction
 }
```

### File Naming Conventions

Three central files:
1. `repos_dataset.csv`
2. `users_dataset.csv`
3. `orgs_dataset.csv`

These contain all unique list of repos and users identified, and also any expanded metadata for those entities.

Other files are our join tables:
1. `forks_join_dataset.csv`
2. `search_queries_join_dataset.csv`
3. `followers_following_dataset.csv`