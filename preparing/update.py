from common.words import Texter
from preparing.extract import Stripper, Scraper
from preparing.audio import Spotter, FMer

class Updater:
    def __init__(self, database):
        self.database = database
        self.main_url = database.main_url

        self.stripper = Stripper(self.main_url)
        self.scraper = Scraper(self.stripper)
        self.texter = Texter()

        self.spotter = Spotter()

    def update_musicleague(self):
        print('Updating database')

        # get information from home page
        leagues_df = self.database.get_leagues()
        league_titles = leagues_df['league']
        league_urls = leagues_df['url']

        # store home page information
        for league_title, league_url in zip(league_titles, league_urls):
            print(f'Investigating {league_title}...')
            
            # get information from league page
            self.update_league(league_title, league_url) 

            self.update_creators(league_title)

    def update_league(self, league_title, league_url):
        # get the zip file from each league
        html_zip = self.scraper.get_zip_file(self.main_url,league_url)
        results = self.stripper.unzip_results(html_zip)

        players, rounds, songs, votes = results

        rounds.loc[:, 'status'] = rounds.apply(lambda x: self.database.get_round_status(league_title, x['round_id']),
                                     axis=1)

        # update song_ids
        song_ids = self.database.get_song_ids(league_title, songs)

        songs = songs.merge(song_ids, on='song_id').drop(columns=['song_id']).rename(columns={'new_song_id': 'song_id'})
        votes = votes.merge(song_ids, on='song_id').drop(columns=['song_id']).rename(columns={'new_song_id': 'song_id'})

        songs_df = songs.merge(rounds[['round', 'status']], on='round')
        votes_df = votes.merge(songs_df[['song_id', 'status']], on='song_id')

        # store data
        self.database.store_songs(songs_df, league_title)
        self.database.store_votes(votes_df, league_title)
        
        # close rounds with all votes
        open_rounds = (songs.groupby('round').count()['song_id'] > 0).reset_index().rename(columns={'song_id': 'new_status'})
        closed_rounds = (votes.merge(songs, on='song_id')[['round', 'vote']].groupby('round').sum()['vote'] > 0).reset_index().rename(columns={'vote': 'new_status'})

        rounds.loc[rounds.merge(open_rounds, on='round')['new_status'], 'status'] = 'open'
        rounds.loc[rounds.merge(closed_rounds, on='round')['new_status'], 'status'] = 'closed'

        self.database.store_rounds(rounds, league_title)
        self.database.store_players(players, league_title=league_title)

    def update_creators(self, league_title):
        rounds_df = self.database.get_uncreated_rounds(league_title)

        if len(rounds_df):
            members_df = self.database.get_members(league_title)
            player_names = members_df['player']
            league_creator = self.database.get_league_creator(league_title)
        
            rounds_df[['creator', 'capture']] = rounds_df.apply(lambda x: \
                                             self.find_creator(x['description'], player_names, league_creator),
                                                                axis=1, result_type='expand')

            self.database.store_rounds(rounds_df, league_title)

    def find_creator(self, description, player_names, league_creator):
        creator = None
        captured = None
        
        if description:
        
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
            if not creator:
                creator = league_creator
        
        return creator, captured

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
