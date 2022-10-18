''' Pull data from MusicLeague API and normalize '''

from dateutil.parser import parse
from io import BytesIO
from zipfile import ZipFile
from datetime import datetime, timedelta
import re
import json

import requests
from pandas import read_csv, DataFrame, concat
from bs4 import BeautifulSoup

from common.secret import get_secret
from common.calling import Caller
from common.words import Texter
from common.locations import APP_URL
from display.streaming import Streamable

class Scraper(Streamable, Caller):
    def __init__(self):
        super().__init__()
        
        self.cj = {get_secret('ML_COOKIE_NAME'): get_secret('ML_COOKIE_VALUE')}
        
    def call_api(self, method, player_id=None, league_id=None, round_id=None, end=None, jason=None):      
        url = f'{APP_URL}/api/v1'
        url += f'/users/{player_id}' if player_id else ''
        url += f'/leagues/{league_id}'if league_id else ''
        url += f'/rounds/{round_id}' if round_id else ''
        url += f'/{end}' if end else ''

        content, jason = self.invoke_api(url, method=method, cookies=self.cj, json=jason)

        return content, jason

    def get_my_leagues(self):
        ''' see all the leagues I am part of '''
        _, me_jason = self.call_api('get', end='me')
        my_id = me_jason['id']

        _, leagues_jason = self.call_api('get', player_id=my_id, end='leagues')

        return leagues_jason

    def get_due_dates(self, league_id, round_id):
        _, round_jason = self.call_api('get', league_id=league_id, round_id=round_id)
        return round_jason

    def post_due_dates(self, league_id, round_id, round_jason):
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

    def get_round_data(self, league_id, round_id):
        _, round_data = self.call_api('get', league_id=league_id, round_id=round_id)
        return round_data

    def update_round_data(self, league_id, round_id, round_data):
        self.call_api('put', league_id=league_id, round_id=round_id, jason=round_data)

    def get_league_data(self, league_id):
        _, league_data = self.call_api('get', league_id=league_id)
        return league_data

    def update_league_data(self, league_id, league_data):
        self.call_api('put', league_id=league_id, jason=league_data)
        

class Stripper(Streamable):  
    timestring = '%Y-%m-%dT%H:%M:%SZ'
    timestring2 = timestring.replace('%SZ', '%S.%fZ')
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
        
            rounds.loc[:, 'created_date'] = rounds.apply(lambda x: parse(x['Created']).date(),
                                                 axis=1)
        
            # clean up songs
            songs = dfs['submissions'].rename(columns={'Submitter ID': 'player_id', 'Round ID': 'round_id',
                                                       'Spotify URI': 'track_uri', 'Comment': 'comment',
                                                       'Created': 'Created_s'})        
            songs.loc[:, 'song_id'] = songs.index
            songs = songs.merge(players, on=['player_id']).merge(rounds, on=['round_id']).rename(columns={'player_id': 'submitter_id'})
            songs.loc[:, 'created_date'] = songs.apply(lambda x: parse(x['Created_s']).date(),
                                        axis=1)

            # clean up votes
            votes = dfs['votes'].rename(columns={'Voter ID': 'player_id', 'Round ID': 'round_id', 'Points Assigned': 'vote',
                                                 'Spotify URI': 'track_uri', 'Comment': 'comment',
                                                 'Created': 'Created_v'})
            votes = votes.merge(players, on=['player_id']).merge(rounds, on=['round_id']).merge(songs.drop(columns=['comment']), on=['round_id', 'track_uri'])
            votes.loc[:, 'created_date'] = votes.apply(lambda x: parse(x['Created_v']).date(),
                            axis=1)

            # drop columns
            players = players[['player_id', 'player_name']]
            rounds = rounds[['round_id', 'round_name', 'description', 'playlist_url', 'created_date']]
            songs = songs[['round_id', 'song_id', 'submitter_id', 'track_uri', 'comment', 'created_date']]
            votes = votes[['player_id', 'song_id', 'vote', 'comment', 'created_date']] ##[votes['vote']!=0]
            tracks = songs[['track_uri']].rename(columns={'track_uri': 'uri'}).drop_duplicates()
            
            return players, rounds, songs, votes, tracks

        else:
            self.streamer.print('\t...league results are empty')

    def extract_my_leagues(self, leagues_jason):
        ''' turn raw league data into dataframe '''
        leagues_df = DataFrame([[l['id'],
                            [m['user']['id'] for m in l['members'] if m['isAdmin']][0],
                            parse(l['created']),
                            l['name'],
                            l['description']] for l in leagues_jason],
                        columns=['league_id', 'creator_id', 'created_date', 'league_name', 'description'])

        return leagues_df

    def parse_date(self, date):
        return parse(date)

    def has_occured(self, date):
        return date >= datetime.now(date.tzinfo)

    def push_date(self, date, days=0, hours=0):
        return datetime.strftime(date + timedelta(days=days, hours=hours), self.timestring)

    def extract_outstanding_players(self, standing_jason, status, inactive_players):
        if status == 'ACCEPTING_SUBMISSIONS':
           have_not = 'Submitted'
        elif status == 'ACCEPTING_VOTES':
            have_not = 'Voted'

        outstanding_players = [i['id'] for i in standing_jason[f'haveNot{have_not}'] if i['id'] not in inactive_players]
        return outstanding_players

    def is_outstanding(self, date, hours_left=0):
        time_diff = date - datetime.now(date.tzinfo)
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

    def get_creator_phrases(self):
        phrases = ['chosen by', 'created by ', 'submitted by ', 'theme is from ', 'theme from ']

        return phrases

    def clean_tracks(self, tracks_df):
        # strip featured artists and remix call outs from track title
        ## add binary for Remix status?
        tracks_df[['title', 'mix']] = tracks_df.apply(lambda x: self.clean_title(x['unclean']),
                                                      axis=1, result_type='expand')

        return tracks_df

    def clean_title(self, title):
        ''' remove featuring artists and pull remixes '''
        # remove remixes
        original_title = title
        title, mix1 = self.texter.remove_parenthetical(title, ['Remix', 'Mix'], position='end', case_sensitive=True)
        title, mix2 = self.texter.remove_parenthetical(title, ['Remix', 'Mix'], position='end', parentheses=[['- ', '$']], case_sensitive=True)
        title, mix3 = self.texter.remove_parenthetical(title, ['Remix', 'Mix'], position='end', parentheses=[['- ', ';']], case_sensitive=True)

        mixes = list(filter(None, [mix1, mix2, mix3]))
        mix = mixes[0] if len(mixes) else None
        if mix and (mix[-1] == '-'):
            # leftover hash
            mix = mix[:-1]
        if (title != original_title) and not mix:
            # default remix
            mix = '< remix >'

        # remove live versions
        original_title = title
        title, _ = self.texter.remove_parenthetical(title, ['Live'], position='start', parentheses=[['- ', '$']], middle=[' with '])
        title, _ = self.texter.remove_parenthetical(title, ['Live'], position='end', parentheses=[['- ', '$']], case_sensitive=True)
        title, _ = self.texter.remove_parenthetical(title, ['Live'], position='start', case_sensitive=True)
        if (title != original_title) and not mix:
            # default remix
            mix = '< live >'

        # remove featured artists
        title, _ = self.texter.remove_parenthetical(title, ['with ', 'Duet with '], position='start') ##<- should with only be for []?
        title, _ = self.texter.remove_parenthetical(title, ['feat'], position='start') # there can be with and feat together
        title, _ = self.texter.remove_parenthetical(title, ['feat. '], position='start', parentheses=[['', '$']])
        
        # remove radio edit and remasters
        title = self.texter.drop_dash(title)

        return title, mix

    def extract_wiki_list(self, genres_text):
        # get article text
        noodles = genres_text['parse']['text']['*']
   
        # strain article text
        soup = BeautifulSoup(noodles, 'html.parser')

        # find category headers and text
        all_categories = [mw.getText() for mw in soup.find_all('span', {'class': 'mw-headline'})]
        categories = all_categories[:all_categories.index('Other') + 1]
        headers = [soup.find('span', {'class': 'mw-headline'}, text=c) for c in categories]

        # find genre list items
        genres = [li.getText() for li in soup.find_all('li')]
        
        return categories, genres, headers

    def extract_genres(self, categories, genres, headers):
        # find first list items after each category header
        first_items = [h.find_next('li').getText() for h in headers]
        
        # add genres
        genres_df = DataFrame([], columns=['name', 'wiki_category'])
        genres_df.loc[:, 'name'] = genres

        # add categories to first item and fill down
        for i, fi in enumerate(first_items):
            genres_df.loc[genres_df['name']==fi, 'wiki_category'] = categories[i]

        genres_df = concat([genres_df.ffill().dropna(),
                            DataFrame(zip(categories, categories),
                                      columns=['name', 'wiki_category'])], ignore_index=True)
        
        # drop anything in the other category
        genres_df = genres_df[genres_df['wiki_category'] != 'Other'].reset_index(drop=True)

        return genres_df

    def clean_up_genres(self, genres_df):
        # switch to lowercase
        for col in ['name', 'wiki_category']:
            genres_df[col] = genres_df[col].str.lower()

        # group single line and multilne genres 
        sl_genres = genres_df[~genres_df['name'].str.contains('\n')]
        ml_genres = \
            genres_df[genres_df['name'].str.contains('\n')]['name'].str.split('\n', expand=True).reset_index()\
            .melt(id_vars='index', value_name='name').dropna()[['name', 'index']]\
            .merge(genres_df[['wiki_category']], left_on='index', right_on=genres_df.index)[['name', 'wiki_category']]

        genres_df = concat([sl_genres, ml_genres],
                           ignore_index=True).sort_values(['wiki_category', 'name'])
        genres_df.loc[:, 'name'] = genres_df['name'].apply(self.remove_date_period)
        genres_df.drop_duplicates('name', inplace=True)

        # split up categories with parentheses
        genres_df.loc[:, 'wiki_category'] = genres_df['wiki_category'].apply(self.pull_out_parenthetical)

        return genres_df

    def pull_out_parenthetical(self, text):
        searched = re.search('[\(\[].*?[\)\]]', text)
        if searched:
            text0 = re.sub('[\(\[].*?[\)\]]', '', text)
            text1 = searched.group(0)[1:-1]
            text = f'{text0} and {text1}'

        return text

    def remove_date_period(self, text): ## consider adding way to strip out (classical)
        if text[-1] == ':':
            text = text[:-1]
        text = re.sub('[\(\[].*?[\)\]]', '', text).strip()
        return text

    def get_time(self, datestring):
        ts = self.timestring2 if ('.' in datestring) else self.timestring
        date = datetime.strptime(datestring, ts)
        return date

    def str_time(self, date):
        datestring = date.strftime(self.timestring)
        return datestring

    def round_up_time(self, date=None, hours=0, round_minutes=30):
        if not date:
            date = datetime.now()
        new_date_0 = date + timedelta(hours=hours)
        new_date = new_date_0 + (datetime.min - new_date_0) % timedelta(minutes=round_minutes)
        return new_date