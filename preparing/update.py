from common.words import Texter
from preparing.extract import Stripper, Scraper
from preparing.audio import Spotter, FMer

class Updater:
    def __init__(self, database):
        self.database = database
        self.stripper = Stripper()
        self.scraper = Scraper()
        self.texter = Texter()

        self.spotter = Spotter()

    def update_musicleague(self):
        print('Updating database')

        # get information from home page
        my_leagues_df = self.stripper.extract_my_leagues(self.scraper.get_my_leagues())
        self.database.store_leagues(my_leagues_df)

        leagues_df = self.database.get_leagues()
        league_titles = leagues_df['league_name']
        league_ids = leagues_df['league_id']

        # store home page information
        for league_title, league_id in zip(league_titles, league_ids):
            print(f'Investigating {league_title}...')
            
            # get information from league page
            self.update_league(league_title, league_id) 

            self.update_creators(league_id)

    def update_league(self, league_title, league_id):
        # get the zip file from each league
        data_zip = self.scraper.get_data_zip(league_id)
        results = self.stripper.unzip_results(data_zip)

        if results is not None:

            players, rounds, songs, votes = results

            # update song_ids
            song_ids = self.database.get_song_ids(league_id, songs)

            songs = songs.merge(song_ids, on='song_id').drop(columns=['song_id']).rename(columns={'new_song_id': 'song_id'})
            votes = votes.merge(song_ids, on='song_id').drop(columns=['song_id']).rename(columns={'new_song_id': 'song_id'})

            # store data
            self.database.store_songs(songs, league_id)
            self.database.store_votes(votes, league_id)
            self.database.store_rounds(rounds, league_id)
            self.database.store_players(players, league_id=league_id)

    def update_creators(self, league_id):
        rounds_df = self.database.get_uncreated_rounds(league_id)

        if len(rounds_df):
            members_df = self.database.get_members(league_id)
            player_ids = members_df[['player_id']]
            players = self.database.get_player_names().merge(player_ids, how='right', on='player_id')[['player_id', 'player_name']]

            league_creator_id = self.database.get_league_creator(league_id)
        
            rounds_df[['creator_id', 'capture']] = rounds_df.apply(lambda x: \
                                             self.find_creator(x['description'], players, league_creator_id),
                                                                axis=1, result_type='expand')

            self.database.store_rounds(rounds_df, league_id)

    def find_creator(self, description, players, league_creator_id):
        creator = None
        creator_id = None
        captured = None
        
        if description:
            player_names = players['player_name'].values
            # first look for item in Created By, Submitted By, etc
            words = ['chosen by', 'created by ', 'submitted by ', 'theme is from ', 'theme from ']
            if not creator:
                _, captured = self.texter.remove_parenthetical(description, words,
                                                               position='start', parentheses='all_end')
                creator = self.texter.find_closest_match(captured, player_names)

            # then look to see if a player name is in the description
            if not creator:
                for player_name in player_names:
                    if player_name in description:
                        creator = player_name
                        break

            creator = self.texter.find_closest_match(creator, player_names)

            # finally, default to league creator
            if creator:
                creator_id = players.query('player_name == @creator')['player_id'].iloc[0]
            else:
                creator_id = league_creator_id
        
        return creator_id, captured

class Extender:
    def __init__(self, database):
        self.stripper = Stripper()
        self.scraper = Scraper()
        self.database = database

    def update_deadlines(self, league_id, round_id, status, days=0, hours=0):
        ''' move out deadlines for a round '''
        dl_types = self.stripper.extract_deadline_types(status)
        round_jason = self.scraper.get_due_dates(league_id, round_id)
        
        if round_jason:
            for dl in dl_types:
                due_date = self.stripper.parse_date(round_jason[f'{dl}Due'])

                if self.stripper.has_passed(due_date):
                    round_jason[f'{dl}Due'] = self.stripper.push_date(date, days, hours)
                    
            self.scraper.post_due_dates(league_id, round_id, round_jason)
            
    def check_outstanding(self, league_id, round_id, status, submit_date, vote_date,
                          inactive_players=[], hours_left=0):
        ''' find which rounds need to be modified based on who hasn't played '''
        extracted = self.stripper.extract_status(status, submit_date, vote_date)

        if extracted:
            period, due_date = extracted

            standing_jason = self.scraper.get_outstanding(league_id, round_id, period)
            outstanding_players = self.stripper.extract_outstanding_players(standing_jason, status, inactive_players)
            
            if len(outstanding_players):
                outstanding = self.stripper.is_outstanding(due_date, hours_left)
            else:
                outstanding = False

        else:
            outstanding = None

        return outstanding

    def check_open_rounds(self, league_id, inactive_players, hours_left):
        ''' find which rounds are current '''
        round_jason = self.scraper.get_round_details(league_id)
        open_rounds = self.stripper.extract_open_rounds(round_jason)
        
        open_rounds.loc[:, 'outstanding'] = open_rounds.apply(lambda x: self.check_outstanding(league_id,
                                                                                               x['round_id'],
                                                                                               x['status'],
                                                                                               x['submit_date'],
                                                                                               x['vote_date'], 
                                                                                               inactive_players,
                                                                                               hours_left=hours_left),
                                                          axis=1)

        if open_rounds['outstanding'].sum():
            open_rounds.loc[:, 'outstanding'] = open_rounds.apply(lambda x: not self.stripper.is_complete(x['status']),
                                                                  axis=1)
            
        return open_rounds

    def extend_deadlines(self, league_id, days, hours, hours_left, inactive_players=[]):
        open_rounds = self.check_open_rounds(league_id, inactive_players=inactive_players, hours_left=hours_left)
        open_rounds.sort_values(by='date', ascending=False, inplace=True)

        for i in open_rounds[open_rounds['outstanding'].fillna(False)].index:
            self.update_deadlines(league_id, open_rounds['round_id'][i], open_rounds['status'][i],
                                  days=days, hours=hours)

    def extend_all_deadlines(self, days=1, hours=0, hours_left=24):
        inactive_players = self.database.get_inactive_players()
        league_ids = self.database.get_extendable_leagues()

        for league_id in league_ids:
            # extend cascading deadlines when the open round has a deadline coming up and outstanding voters
            self.extend_deadlines(league_id, days, hours, hours_left, inactive_players)

class Musician:
    def __init__(self, database):
        self.database = database

        self.spotter = Spotter()
        self.fmer = FMer()

    def update_spotify(self):
        self.spotter.update_database(self.database)

    def update_lastfm(self):
        self.fmer.update_database(self.database)

    def output_playlists(self):
        self.spotter.output_playlists(self.database)
