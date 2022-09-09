from dateutil.parser import parse
from io import BytesIO
from zipfile import ZipFile

import requests
import browser_cookie3 as browsercookie
from pandas import read_csv

from common.secret import get_secret, set_secret
from common.words import Texter
from display.streaming import Streamable

class Scraper(Streamable):
    headers_get = {'X-Requested-With': 'XMLHttpRequest'}
    headers_post = {'authority': 'musicleague.app'}

    def __init__(self, stripper):
        super().__init__()
        self.stripper = stripper
        self.main_url = stripper.main_url
        
        #self.cj = browsercookie.chrome(domain_name=self.main_url.replace('https://', ''))
        self.cj = {get_secret('ML_COOKIE_NAME'): get_secret('ML_COOKIE_VALUE')}
        
    def reset_cookie(self):
        domain_name = self.main_url.replace('https://', '')
        cj = browsercookie.chrome(domain_name=domain_name)
        cookie_name = list(cj._cookies[f'.{domain_name}']['/'].keys())[0]
        cookie_value = cj._cookies[f'.{domain_name}']['/'][cookie_name].value
        set_secret('ML_COOKIE_NAME', cookie_name)
        set_secret('ML_COOKIE_VALUE', cookie_value)

    def get_zip_file(self, main_url, league_url):
        url = f'{main_url}{league_url}'
        return self.get_content(url)
    
    def get_content(self, url):
        self.streamer.print(f'\t...requesting response from {url}')

        url = f'{url}data'.replace('/l/', '/api/v1/leagues/')

        method = requests.post
        headers = self.headers_post

        try:
            response = method(url, cookies=self.cj, headers=headers, timeout=10)
            if response.ok:
                item = ZipFile(BytesIO(response.content))
            else:
                item = None
        except TimeoutError:
            print('timeout error')
            item = None

        return item

class Stripper(Streamable):                                 
    def __init__(self, main_url):
        super().__init__()

        self.main_url = main_url

        self.texter = Texter()

    def unzip_results(self, results):
        sheets = ['competitors', 'rounds', 'submissions', 'votes']
        dfs = {}
        with results as z:
            for fn in sheets:
                with z.open(f'{fn}.csv') as f:
                    dfs[fn] = read_csv(f)

        # clean up players
        players = dfs['competitors'].rename(columns={'ID': 'player_id', 'Name': 'player'})

        # clean up rounds
        rounds = dfs['rounds'].rename(columns={'ID': 'round_id', 'Name': 'round', 
                                               'Description': 'description', 'Playlist URL': 'playlist_url'})
        rounds.loc[:, 'date'] = rounds.apply(lambda x: parse(x['Created']).date(),
                                             axis=1)
        
        # clean up songs
        songs = dfs['submissions'].rename(columns={'Submitter ID': 'player_id', 'Round ID': 'round_id',
                                                   'Spotify URI': 'track_url'})        
        songs.loc[:, 'song_id'] = songs.index
        songs = songs.merge(players, on=['player_id']).merge(rounds, on=['round_id']).rename(columns={'player': 'submitter'})

        # clean up votes
        votes = dfs['votes'].rename(columns={'Voter ID': 'player_id', 'Round ID': 'round_id', 'Points Assigned': 'vote',
                                             'Spotify URI': 'track_url'})
        votes = votes.merge(players, on=['player_id']).merge(rounds, on=['round_id']).merge(songs, on=['round_id', 'track_url'])

        # change URI to URL
        ## <FUTURE> keep as URI
        songs.loc[:, 'track_url'] = songs.apply(lambda x: x['track_url'].replace('spotify:track:',
                                                                                 'https://open.spotify.com/track/'),
                                                axis=1)

        votes.loc[:, 'track_url'] = votes.apply(lambda x: x['track_url'].replace('spotify:track:',
                                                                                 'https://open.spotify.com/track/'),
                                                axis=1)

        # drop columns
        players = players[['player_id', 'player']]
        rounds = rounds[['round_id', 'round', 'description', 'playlist_url', 'date']]
        songs = songs[['round', 'song_id', 'submitter', 'track_url']]
        votes = votes[votes['vote']!=0][['player', 'song_id', 'vote']]

        return players, rounds, songs, votes