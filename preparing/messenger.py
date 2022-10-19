''' Checks mail to see if there are round notifications '''

from datetime import datetime, timedelta
import time
import requests
import re

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

from common.secret import get_secret
from common.locations import GCP_TOKEN_URI, GCP_AUTH_URL, APP_ALIAS
from common.calling import Caller
from common.words import Texter
from common.calling import Recorder

class Mailer(Caller):
    complete_phrase = 'The Votes Are In'

    def __init__(self, database, alias=None):
        super().__init__()

        self.database = database

        token_info = self.refresh_token()
        info = {'token': token_info['access_token'],
                'refresh_token': get_secret('GCP_REFRESH_TOKEN'),
                'token_uri': GCP_TOKEN_URI,
                'client_id': get_secret('GCP_CLIENT_ID'),
                'client_secret': get_secret('GCP_CLIENT_SECRET'),
                'scopes': [token_info['scope']],
                'expiry': token_info['expiry']}
        self.credentials = Credentials.from_authorized_user_info(info)

        self.user_id = self.get_user_id(alias)
        self.texter = Texter()
        self.recorder = Recorder()

    def refresh_token(self):
        ''' refresh GCP token '''
        url = f'{GCP_AUTH_URL}/o/oauth2/token'
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'Accept': 'application/json'}

        payload = {'grant_type': 'refresh_token',
                   'client_id': get_secret('GCP_CLIENT_ID'),
                   'client_secret': get_secret('GCP_CLIENT_SECRET'),
                   'refresh_token': get_secret('GCP_REFRESH_TOKEN')}

        token_info = self.get_token(url, headers=headers, data=payload)

        return token_info

    def get_user_id(self, alias=None):
        if alias is None:
            user_id = 'me'
        else:
            user_id = None ## figure out user_id based on email
        return user_id

    def check_mail(self, hours=24):
        ''' check if there are new results to update '''
        print(f'Checking mail...')

        dt = self.recorder.get_time('check_mail')
        if (dt is None):
            dt = datetime.today() - timedelta(hours=hours) 
        d = int(dt.timestamp())
        query = f'from:{APP_ALIAS} subject:{self.complete_phrase} after:{d}'

        with build('gmail', 'v1', credentials=self.credentials) as service:
            messages = service.users().messages().list(userId=self.user_id, q=query).execute()

        update_needed = (messages['resultSizeEstimate'] > 0)
        league_ids = None

        if not update_needed:
            print('\t...no new messages.')
            self.mark_as_read()
        else:
            message_leagues = []
            with build('gmail', 'v1', credentials=self.credentials) as service:
                for m in messages['messages']:
                    mail = service.users().messages().get(userId=self.user_id, id=m['id']).execute()
                    s = re.search('round of the(.*?)league', mail['snippet'])
                    if len(s.groups()):
                        message_leagues.append(s.group(1).strip())

            if len(message_leagues):
                leagues_df = self.database.get_leagues()
                query = leagues_df.query('league_name in @message_leagues')
                league_names = query['league_name'].to_list()
                league_ids = query['league_id'].to_list()

                print(f'\t...new data detected for {self.texter.get_plurals(league_names)["text"]}.')
            else:
                print(f'\t...messages not understood.')

        return league_ids

    def mark_as_read(self):
        self.recorder.set_time('check_mail')