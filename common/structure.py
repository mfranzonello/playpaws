''' Personal account variables '''

import json

with open('./jsons/structure.json') as f:
    json_dict = json.load(f)

BITIO_USERNAME = json_dict['BITIO_USERNAME']
BITIO_DBNAME = json_dict['BITIO_DBNAME']
SPOTIFY_USER_ID = json_dict['SPOTIFY_USER_ID']
SPOTIFY_USERNAME = json_dict['SPOTIFY_USERNAME']
GCP_S_PROJECT_ID = json_dict['GCP_S_PROJECT_ID']
GCP_S_ACCOUNT_NAME = json_dict['GCP_S_ACCOUNT_NAME']
GITHUB_REPOSITORY_ID = json_dict['GITHUB_REPOSITORY_ID']
GITHUB_ENVIRONMENT_NAME = json_dict['GITHUB_ENVIRONMENT_NAME']