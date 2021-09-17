from pandas import DataFrame

from stripper import Stripper, Scraper, Simulator
from spotify import Spotter
from results import Songs, Votes, Rounds, Leagues, Players

class Updater:
    def __init__(self, database, structure, credentials, settings):
        self.database = database
        self.structure = structure
        self.credentials = credentials
        self.main_url = structure['web']['main_url']

        self.local = settings['local']
        self.spotter = Spotter(credentials['spotify'])

        self.simulator = Simulator(main_url=self.main_url, credentials=credentials['musicleague'],
                                   chrome_directory=structure['web']['chrome_driver'], chrome_profile=structure['web']['chrome_profile'],
                                   silent=settings['silent'])

        self.stripper = Stripper(main_url = self.main_url if self.local else '')
        self.scraper = Scraper(self.simulator, self.stripper)

    def turn_off(self):
        if not self.local:
            self.simulator.turn_off()

    def get_right_url(self, url=None, league_title=None, round_title=None):
        if (league_title == None) and (round_title == None):
            # main url
            if self.local:
                directory = self.structure['local']['home']
                right_url = self.scraper.get_right_url(directory)
            else:
                right_url = self.structure['web']['main_url']
            
        elif round_title == None:
            # league url
            if self.local:
                directory = self.structure['local']['league']
                right_url = self.scraper.get_right_url(directory, league_title=league_title)
            else:
                right_url = url

        else:
            # round url
            if self.local:
                directory = self.structure['local']['round']
                right_url = self.scraper.get_right_url(directory, league_title=league_title, round_title=round_title)
            else:
                right_url = url

        return right_url

    def update_database(self):
        print('Updating database')
        leagues = Leagues()

        # get information from home page
        main_url = self.get_right_url()

        html_text = self.scraper.get_html_text(main_url)
        results = self.stripper.extract_results(html_text, page_type='home')

        league_titles, league_urls, league_creators, league_dates = results

        leagues_df = leagues.sub_leagues(league_titles, url=league_urls, date=league_dates, creator=league_creators)
        self.database.store_leagues(leagues_df)

        # store home page information
        for league_title, league_url in zip(league_titles, league_urls):
            
            # get information from league page
            results = self.update_league(league_title, league_url)
            round_titles, round_urls = results

            for round_title, round_url in zip(round_titles, round_urls):

                # get information from round page
                results = self.update_round(league_title, round_title, round_url)

    def update_league(self, league_title, league_url):
        rounds = Rounds()
        players = Players()

        league_url = self.get_right_url(url=league_url, league_title=league_title)

        html_text = self.scraper.get_html_text(league_url)
        results = self.stripper.extract_results(html_text, page_type='league')

        _, round_titles, \
            player_names, player_urls, player_imgs, \
            round_urls, round_dates, round_creators = results # _ = league_title

        if len(round_titles):
            round_creators = [self.database.get_player_match(league_title, round_creator) for round_creator in round_creators]
            league_creator = self.database.get_league_creator(league_title)
            rounds_df = rounds.sub_rounds(round_titles, league_creator=league_creator,
                                          url=round_urls, date=round_dates, creator=round_creators)
            self.database.store_rounds(rounds_df, league_title)

        if(len(player_names)):
            players_df = players.sub_players(player_names, url=player_urls, src=player_imgs)
            self.database.store_players(players_df, league_title)
        
        return round_titles, round_urls

    def update_round(self, league_title, round_title, round_url):
        songs = Songs()
        votes = Votes()

        # check round status
        round_status = self.database.get_round_status(league_title, round_title)

        # extract results from URL if new or open
        if round_status in ['missing', 'new', 'open']:
            round_url = self.get_right_url(url=round_url, league_title=league_title, round_title=round_title)
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
                _, _, artists, titles, submitters, \
                    song_ids, player_names, vote_counts, track_urls = results # _, _ = league_title, round_title
                
                next_song_ids = self.database.get_song_ids(league_title, artists, titles) # -> consider replacing existing song_ids?

                # construct details with updates for new and open
                songs_df = songs.sub_round(round_title, artists, titles, submitters, next_song_ids)
                votes_df = votes.sub_round(song_ids, player_names, vote_counts, next_song_ids)
                self.database.store_songs(songs_df, league_title)
                self.database.store_votes(votes_df, league_title)

                # check if round can be closed
                if len(player_names):
                    new_status = 'closed'
                elif len(artists):
                    new_status = 'open'
                else:
                    new_status = 'new'
                self.database.store_round(league_title, round_title, new_status, round_url) # -> consider replacing with store_rounds

            else:
               # round is closed and doesn't need to be updated
               print(f'\t...round {round_title} is closed, no need to update')

        else:
            # the URL is missing
            print(f'Round {round_title} not found.')
            self.database.store_round(league_title, round_title, 'new')