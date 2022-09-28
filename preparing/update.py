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
        my_leagues_df = self.scraper.get_my_leagues()
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
        html_zip = self.scraper.get_zip_file(league_id)
        results = self.stripper.unzip_results(html_zip)

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
