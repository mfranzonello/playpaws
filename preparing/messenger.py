''' Checks mail to see if there are round notifications '''

from datetime import datetime, timedelta
import time
import requests
import json

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from common.secret import get_secret
from common.locations import GCP_TOKEN_URI, GCP_AUTH_URL, APP_ALIAS

class Mailer:
    complete_phrase = 'The Votes Are In'

    def __init__(self, alias=None):
        token_info = self.get_token()
        info = {'token': token_info['access_token'],
                'refresh_token': get_secret('GCP_REFRESH_TOKEN'),
                'token_uri': GCP_TOKEN_URI,
                'client_id': get_secret('GCP_CLIENT_ID'),
                'client_secret': get_secret('GCP_CLIENT_SECRET'),
                'scopes': [token_info['scope']],
                'expiry': token_info['expiry']}
        self.credentials = Credentials.from_authorized_user_info(info)

        self.user_id = self.get_user_id(alias)
        self.recorder = Recorder()

    def get_token(self):
        ''' refresh GCP token '''
        url = f'{GCP_AUTH_URL}/o/oauth2/token'
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'Accept': 'application/json'}

        payload = {'grant_type': 'refresh_token',
                   'client_id': get_secret('GCP_CLIENT_ID'),
                   'client_secret': get_secret('GCP_CLIENT_SECRET'),
                   'refresh_token': get_secret('GCP_REFRESH_TOKEN')}

        response = requests.post(url, headers=headers, data=payload)
        
        if response.ok:
            token_info = response.json()
            token_info['expiry'] = int(time.time()) + token_info['expires_in']

        return token_info

    def get_user_id(self, alias=None):
        if alias is None:
            user_id = 'me'
        else:
            user_id = None ## figure out user_id based on email
        return user_id

    def check_mail(self, alias='me'):
        ''' check if there are new results to update '''
        dt = self.recorder.get_time('check_mail')
        if (dt is None):
            dt = datetime.today() - timedelta(hours=hours) 
        d = int(dt.timestamp())
        query = f'from:{APP_ALIAS} subject:{self.complete_phrase} after:{d}'

        with build('gmail', 'v1', credentials=credentials) as service:
            messages = service.users().messages().list(userId=alias, q=query).execute()

        update_needed = len(messages)

        if not update_needed:
            self.mark_as_read()

        return update_needed

    def mark_as_read(self):
        self.recorder_set_time('check_mail')

class Recorder:
    def __init__(self, filename='checked'):
        self.jason = f'./preparing/{filename}.json'

    def set_time(self, item):
        with open(self.jason, 'r+') as f:
            j  = json.load(f)
            j[item] = datetime.now().isoformat()
            f.seek(0)
            json.dump(j, f)
            f.truncate()
        
    def get_time(self, item):
        with open(self.jason, 'r+') as f:
            j = json.load(f)
            dt = datetime.fromisoformat(j[item])
            
        return dt