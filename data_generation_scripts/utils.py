import re
import time
from time import sleep
import pandas as pd
import requests
import os
from tqdm import tqdm
import apikey
import json
import codecs
import shutil
from datetime import datetime


auth_token = apikey.load("DH_GITHUB_DATA_PERSONAL_TOKEN")

auth_headers = {'Authorization': f'token {auth_token}','User-Agent': 'request'}

def check_rate_limit():
    # Checks for rate limit so that you don't hit issues with Github API. Mostly for search API that has a 30 requests per minute https://docs.github.com/en/rest/rate-limit
    url = 'https://api.github.com/rate_limit'
    response = requests.get(url, headers=auth_headers)
    rates_df = pd.json_normalize(response.json())
    return rates_df

def check_total_pages(url):
    # Check total number of pages to get from search. Useful for not going over rate limit
    response = requests.get(f'{url}?per_page=1', headers=auth_headers)
    if response.status_code != 200:
        print('hit rate limiting. trying to sleep...')
        time.sleep(120)
        response = requests.get(url, headers=auth_headers)
        total_pages = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
    else:
        total_pages = 1 if len(response.links) == 0 else re.search('\d+$', response.links['last']['url']).group()
    return total_pages

def check_total_results(url):
    # Check total results from api call
    response = requests.get(url, headers=auth_headers)
    if response.status_code != 200:
        print('hit rate limiting. trying to sleep...')
        time.sleep(120)
        response = requests.get(url, headers=auth_headers)
        data = response.json()
    else:
        data = response.json()
    return data['total_count']