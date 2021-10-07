from extract import Stripper, Scraper
from audio import Spotter, FMer
from results import Songs, Votes, Rounds, Leagues, Players

class Updater:
    def __init__(self, database):
        self.database = database
        self.main_url = database.main_url

        self.stripper = Stripper(self.main_url)
        self.scraper = Scraper(self.stripper)

    def update_musicleague(self):
        print('Updating database')
        leagues = Leagues()

        # get information from home page
        html_text = self.scraper.get_html_text(self.main_url)
        results = self.stripper.extract_results(html_text, page_type='home')

        league_titles, league_urls, league_creators, league_dates = results

        leagues_df = leagues.sub_leagues(league_titles, url=league_urls, date=league_dates, creator=league_creators)
        self.database.store_leagues(leagues_df)

        # store home page information
        for league_title, league_url in zip(league_titles, league_urls):
            print(f'Investigating {league_title}...')
            
            # get information from league page
            results = self.update_league(league_title, league_url)
            round_titles, round_urls = results

            for round_title, round_url in zip(round_titles, round_urls):

                # get information from round page
                results = self.update_round(league_title, round_title, round_url)

    def update_league(self, league_title, league_url):
        rounds = Rounds()
        players = Players()

        html_text = self.scraper.get_html_text(league_url)
        results = self.stripper.extract_results(html_text, page_type='league')

        _, round_titles, \
            player_names, player_urls, \
            round_urls, round_dates, round_creators, round_playlists = results # _ = league_title

        if len(round_titles):

            round_creators = [self.database.get_player_match(league_title=league_title, partial_name=round_creator) \
                for round_creator in round_creators]

            league_creator = self.database.get_league_creator(league_title)
            rounds_df = rounds.sub_rounds(round_titles, league_creator=league_creator,
                                          url=round_urls, date=round_dates, creator=round_creators, playlist_url=round_playlists)
            self.database.store_rounds(rounds_df, league_title)

        if(len(player_names)):
            players_df = players.sub_players(player_names, username=player_urls)
            self.database.store_players(players_df, league_title=league_title)
        
        return round_titles, round_urls

    def update_round(self, league_title, round_title, round_url):
        songs = Songs()
        votes = Votes()
        players = Players()

        # check round status
        round_status = self.database.get_round_status(league_title, round_title)

        # extract results from URL if new or open
        if round_status in ['missing', 'new', 'open']:
            round_available = (round_url is not None)
        else: # round_status == closed
            round_available = True

        if round_available:
            print(f'Loading round {round_title}')

            if round_status in ['missing', 'new', 'open']:
                # round needs to be updated
                print(f'\t...updating round {round_title}')

                html_text = self.scraper.get_html_text(round_url)
                results = self.stripper.extract_results(html_text, 'round')
                _, _, submitters, \
                    song_ids, player_names, vote_counts, vote_totals, track_urls, \
                    users = results # _, _ = league_title, round_title, artists, titles

                if users:
                    # update any users that may have dropped
                    players_df = players.sub_players(users['player_names'], username=users['username'])
                    self.database.store_players(players_df, league_title=league_title)

                if not(len(track_urls)):
                    playlist_url = self.database.get_round_playlist(league_title, round_title)
                    if playlist_url:
                        self.spotter.connect_to_spotify()
                        track_urls = self.spotter.get_playlist_uris(playlist_url, external_url=True)
                
                next_song_ids = self.database.get_song_ids(league_title, round_title, track_urls) # -> consider replacing existing song_ids?
                
                # construct details with updates for new and open
                songs_df = songs.sub_round(round_title, track_urls, submitters, next_song_ids)
                votes_df = votes.sub_round(song_ids, player_names, vote_counts, vote_totals, next_song_ids)

                # check if round can be closed
                if len(player_names):
                    new_status = 'closed'
                elif len(track_urls):
                    new_status = 'open'
                else:
                    new_status = 'new'

                if new_status == 'closed':
                    # remove placeholder votes
                    self.database.drop_votes(league_title, round_title)


                # store updates
                self.database.store_songs(songs_df, league_title)
                self.database.store_votes(votes_df, league_title)
                self.database.store_round(league_title, round_title, new_status, round_url) # -> consider replacing with store_rounds

            else:
               # round is closed and doesn't need to be updated
               print(f'\t...round {round_title} is closed, no need to update')

        else:
            # the URL is missing
            print(f'Round {round_title} not found.')
            self.database.store_round(league_title, round_title, 'new')

class Musician:
    def __init__(self, database):
        self.database = database

        self.spotter = Spotter()
        self.fmer = FMer()

    def update_spotify(self):
        self.spotter.update_database(self.database)

    def update_lastfm(self):
        self.fmer.update_database(self.database)