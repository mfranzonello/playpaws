from dateutil.parser import parse
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta

import requests
from pandas import read_csv, DataFrame

from common.secret import get_secret, set_secret
from common.words import Texter
from display.streaming import Streamable

class Scraper(Streamable):
    app_url = 'https://app.musicleague.com' ## move this elsewhere?

    def __init__(self):
        super().__init__()
        
        self.cj = {get_secret('ML_COOKIE_NAME'): get_secret('ML_COOKIE_VALUE')}
        
    def call_api(self, method, player_id=None, league_id=None, round_id=None, end=None, jason=None):
        call_method = {'get': requests.get,
                       'post': requests.post,
                       'put': requests.put}[method]
        
        url = f'{self.app_url}/api/v1'
        url += f'/users/{player_id}' if player_id else ''
        url += f'/leagues/{league_id}'if league_id else ''
        url += f'/rounds/{round_id}' if round_id else ''
        url += f'/{end}' if end else ''

        self.streamer.print(f'\t...requesting {method} response from {url.replace(self.app_url, "")}')

        try:
            response = call_method(url=url, cookies=self.cj, json=jason, timeout=10)
            if response.ok:
                r_content = response.content
                r_jason = response.json() if len(r_content) and response.headers.get('Content-Type').startswith('application/json') else None

            else:
                r_content = None
                r_jason = None

        except TimeoutError:
            r_content = None
            r_jason = None

        return r_content, r_jason

    def get_my_leagues(self):
        ''' see all the leagues I am part of '''
        _, me_jason = self.call_api('get', end='me')
        my_id = me_jason['id']

        _, leagues_jason = self.call_api('get', player_id=my_id, end='leagues')

        return leagues_jason

    def get_due_date(self, league_id, round_id):
        _, round_jason = self.call_api('get', league_id=league_id, round_id=round_id)
        return round_jason

    def post_due_date(self, league_id, round_id, round_jason):
        self.call_api('put', league_id=league_id, round_id=round_id, jason=round_jason)

    def get_round_details(self, league_id):
        _, round_jason = self.call_api('get', league_id=league_id, end='rounds')
        return round_jason

    def get_outstanding(self, league_id, round_id, period):
        _, standing_jason = self.call_api('get', league_id=league_id, round_id=round_id, end=f'{period}s')
        return standing_jason
        
    def get_data_zip(self, league_id):
        ''' get zipped CSV files of all exported league data '''
        r_content, _ = self.call_api('post', league_id=league_id, end='data')
        
        if r_content:
            item = ZipFile(BytesIO(r_content))
        else:
            item = None

        return item

class Stripper(Streamable):  
    timestring = '%Y-%m-%dT%H:%M:%SZ' ## MOVE TO STRIPPER

    '''
    round parameters:
        'completed'
        'description'
        'downvotesPerUser'
        'highStakes'
        'id'
        'leagueId'
        'maxDownvotesPerSong'
        'maxUpvotesPerSong'
        'name'
        'playlistUrl'
        'sequence'
        'songPerUser'
        'startDate'
        'status'
        'submissionsDue'
        'templateId'
        'upvotesPerUser'
        'votesDue'

    participation status
        {'haveNotVoted/Submitted': ['id']
         'haveVoted/Submitted': ['id']}

    status types:
        "NOT_STARTED"
        "ACCEPTING_SUBMISSIONS"
        "ACCEPTING_VOTES"
        "COMPLETE"

    '''

    def __init__(self):
        super().__init__()

        self.texter = Texter()

    def unzip_results(self, data_zip):
        sheets = ['competitors', 'rounds', 'submissions', 'votes']
        dfs = {}
        with data_zip as z:
            for fn in sheets:
                with z.open(f'{fn}.csv') as f:
                    dfs[fn] = read_csv(f)

        # clean up players
        players = dfs['competitors'].rename(columns={'ID': 'player_id', 'Name': 'player_name'})

        # check for data
        if len(dfs['rounds']):

            # clean up rounds
            rounds = dfs['rounds'].rename(columns={'ID': 'round_id', 'Name': 'round_name', 
                                                   'Description': 'description', 'Playlist URL': 'playlist_url'})
        
            rounds.loc[:, 'date'] = rounds.apply(lambda x: parse(x['Created']).date(),
                                                 axis=1)
        
            # clean up songs
            songs = dfs['submissions'].rename(columns={'Submitter ID': 'player_id', 'Round ID': 'round_id',
                                                       'Spotify URI': 'track_uri'})        
            songs.loc[:, 'song_id'] = songs.index
            songs = songs.merge(players, on=['player_id']).merge(rounds, on=['round_id']).rename(columns={'player_id': 'submitter_id'})

            # clean up votes
            votes = dfs['votes'].rename(columns={'Voter ID': 'player_id', 'Round ID': 'round_id', 'Points Assigned': 'vote',
                                                 'Spotify URI': 'track_uri'})
            votes = votes.merge(players, on=['player_id']).merge(rounds, on=['round_id']).merge(songs, on=['round_id', 'track_uri'])

            # drop columns
            players = players[['player_id', 'player_name']]
            rounds = rounds[['round_id', 'round_name', 'description', 'playlist_url', 'date']]
            songs = songs[['round_id', 'song_id', 'submitter_id', 'track_uri']]
            votes = votes[votes['vote']!=0][['player_id', 'song_id', 'vote']]
            
            return players, rounds, songs, votes

        else:
            self.streamer.print('\t...league results are empty')

    def extract_my_leagues(self, leagues_jason):
        ''' turn raw league data into dataframe '''
        leagues_df = DataFrame([[l['id'],
                            [m['user']['id'] for m in l['members'] if m['isAdmin']][0],
                            parse(l['created']),
                            l['name']] for l in leagues_jason],
                        columns=['league_id', 'creator_id', 'date', 'league_name'])

        return leagues_df

    def parse_date(self, date):
        return parse(date)

    def has_occured(self, date):
        return date >= datetime.now(due_date.tzinfo)

    def push_date(date, days=0, hours=0):
        return datetime.strftime(due_date + timedelta(days=days, hours=hours), self.timestring)

    def extract_outstanding_players(self, standing_jason, status, inactive_players):
        if status == 'ACCEPTING_SUBMISSIONS':
           have_not = 'Submitted'
        elif status == 'ACCEPTING_VOTES':
            have_not = 'Voted'

        outstanding_players = [i['id'] for i in standing_jason[f'haveNot{have_not}'] if i['id'] not in inactive_players]
        return outstanding_players

    def is_outstanding(self, due_date, hours_left=0):
        time_diff = due_date - datetime.now(due_date.tzinfo)
        hours_diff = (time_diff.days * 24) + (time_diff.seconds/60**2)
        outstanding = hours_diff <= hours_left

        return outstanding

    def extract_open_rounds(self, round_jason):
        open_rounds = DataFrame([[r['id'], r['status'], parse(r['startDate']),
                            parse(r['submissionsDue']), parse(r['votesDue'])] \
                            for r in round_jason if r['status'] != 'COMPLETED'],
                        columns=['round_id', 'status', 'date', 'submit_date', 'vote_date'])

        return open_rounds

    def extract_deadline_types(self, status):
        if status in ['NOT_STARTED', 'ACCEPTING_SUBMISSIONS']:
            dl_types= ['submissions', 'votes']
        elif status in ['ACCEPTING_VOTES']:
            dl_types = ['votes']
        else:
            dl_types = []

        return dl_types

    def extract_status(self, status, submit_date, vote_date):
        if 'ACCEPTING' in status:
            if status == 'ACCEPTING_SUBMISSIONS':
                period = 'submission'
                due_date = submit_date
            elif status == 'ACCEPTING_VOTES':
                period = 'vote'
                due_date = vote_date

            return period, due_date

    def is_complete(self, status):
        return status == 'COMPLETE'