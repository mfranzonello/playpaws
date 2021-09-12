from itertools import combinations, permutations
from os import getlogin

from pandas import read_excel, DataFrame, concat, set_option

from results import Songs, Votes, Rounds, Leagues
from comparisons import Pulse, Players, Rankings
from plotting import Plotter
from stripper import HTMLStripper, HTMLScraper, Selena
from data import Database
from secret import credentials

# inputs
directory = f'C:/Users/{getlogin()}/OneDrive/Projects/Play Paws'
html_folder = 'html'

main_url = 'https://musicleague.app'

home_directory = f'{directory}/{html_folder}/home'
leagues_directory = f'{directory}/{html_folder}/leagues'
rounds_directory = f'{directory}/{html_folder}/rounds'

db_name = 'playpaws'
chrome_directory = f'C:/Users/{getlogin()}/OneDrive/Projects/Play Paws/Play Paws/Play Paws/'

def set_options():
    set_option('display.max_columns', None)
    set_option('display.max_rows', None)

def main(local=True):
    set_options()
    database = Database(db_name)
    plotter = Plotter()
    selena = Selena(local, chrome_directory, main_url, credentials) # what if local only?

    weights = database.get_weights()

    if selena.local:
        url = HTMLScraper.get_urls(home_directory)[0]
    else:
        url = main_url
    
    #selena.login()
    html_text = HTMLScraper.get_html_text(url, selena=selena)

    league_titles = []
    league_urls = []

    while (len(league_titles) == 0):
        selena.login()
        league_titles, league_urls = HTMLStripper.extract_results(html_text, 'home')
        print(f'titles: {league_titles}')
        print(f'urls: {league_urls}')

        input()

    leagues = Leagues(league_titles)
    database.store_leagues(leagues.get_leagues())

    for league_title, url in zip(league_titles, league_urls):
        league_analysis(league_title, url, database, plotter, weights, selena)

    selena.turn_off()

    # plot results for all leagues
    plotter.plot_results()


def get_weights(database):
    weights = database.get_weights()
    return weights

def league_analysis(league_title, league_url, database, plotter, weights, selena):
    # see if URL already exists
    url = database.get_url('league', leagues_directory, league_title)
    # see if URL is in the directory
    if url is None:
        if selena.local:
            url = HTMLScraper.get_right_url(leagues_directory, league_title)
        else:
            url = league_url
        database.store_league_url(league_title, url)
    # skip if no URL found
    if url is None:
        print(f'Skipping league {league_title}, not found')
    
    else:
        print(f'Setting up league {league_title}')
        html_text = HTMLScraper.get_html_text(url, selena=selena)
        _, round_titles, round_urls, player_names = HTMLStripper.extract_results(html_text, 'league') # don't need league title
        # get all players
        players = Players(player_names)

        update_database(league_title, round_titles, round_urls, database, selena)

        songs, votes, rounds = get_songs_and_votes(league_title, round_titles, database)

        rankings = crunch_rounds(songs, votes, rounds, players, weights)

        pulse = get_pulse(songs, votes, players)
    
        summarize(plotter, songs, rounds, pulse, players, rankings, league_title)

def update_database(league_title, round_titles, round_urls, database, selena):
    print('Updating database')
    db_songs = database.get_songs(league_title)
    db_votes = database.get_votes(league_title)

    for round_title, url in zip(round_titles, round_urls):
        round_status = database.get_round_status(league_title, round_title)

        # extract results from URL if new or open
        if round_status in ['missing', 'new', 'open']:
            db_urls = database.get_urls('round', rounds_directory)
            if selena.local:
                url = HTMLStripper.get_right_url(rounds_directory, league_title, round_title=round_title, not_urls=db_urls)

            round_available = (url is not None)
        else: # closed
            round_available = True

        if round_available:
            print(f'Loading round {round_title}')

            if round_status in ['missing', 'new', 'open']:
                html_text = HTMLScraper.get_html_text(url, selena)
                results = HTMLStripper.extract_results(html_text, 'round')
                _, _, artists, titles, submitters, \
                    song_ids, player_names, vote_counts = results # don't need league or round title

                # get song_ids for updates
                if round_status in ['missing', 'new']:
                    next_song_ids = database.get_next_song_ids(len(artists), league_title)
                elif round_status == 'open':
                    next_song_ids = DataFrame(zip(artists, titles), columns=['artist', 'title']).merge(db_songs, on=['artist', 'title'])['song_id']

                # construct details with updates for new and open
                songs_df = Songs.sub_round(round_title, artists, titles, submitters, next_song_ids)
                votes_df = Votes.sub_round(song_ids, player_names, vote_counts, next_song_ids)
                database.store_songs(songs_df, league_title)
                database.store_votes(votes_df, league_title)

                # check if round can be closed
                if len(player_names):
                    new_status = 'closed'
                elif len(artists):
                    new_status = 'open'
                else:
                    new_status = 'new'
                database.store_round(league_title, round_title, new_status, url)        
        else:
            # the URL is missing
            print(f'Round {round_title} not found.')
            database.store_round(league_title, round_title, 'new')

def get_songs_and_votes(league_title, round_titles, database):
    print('Loading from database')
    rounds = Rounds()
    songs = Songs()
    votes = Votes()

    db_rounds = database.get_rounds(league_title)
    db_songs = database.get_songs(league_title)
    db_votes = database.get_votes(league_title)

    rounds.add_rounds_db(db_rounds)
    rounds.sort_titles(round_titles)

    for round_title in round_titles:
        songs.add_round_db(db_songs.query(f'round == "{round_title}"'))
        round_song_ids = songs.get_songs_ids(round_title)
        votes.add_round_db(db_votes.query(f'song_id in {round_song_ids}'))
            
    return songs, votes, rounds

def crunch_rounds(songs, votes, rounds, players, weights):
    print('Crunching rounds')
    # count how many submitted per round
    votes.name_rounds(songs)
    rounds.count_players(votes)

    # total points per song
    songs.calculate_points(votes, rounds, players, weights)

    # list winners of each round
    rankings = Rankings(songs, votes, weights)

    return rankings

def get_pulse(songs, votes, players):
    # get group pulse
    print('Getting pulse')
    
    pulse = Pulse(players)
    print('...likes')
    pulse.calculate_likers(songs, votes)
    print('...similarity')
    pulse.calculate_similarity(songs, votes)
    print('...wins')
    pulse.calculate_wins(songs, votes)

    # calculate distances
    print('Getting coordinates')

    print('...coordinates')
    players.update_coordinates(pulse)
    print('...likes')
    players.who_likes_whom(pulse)
    print('...dfc')
    players.get_dfc(songs, votes)
    print('...battle')
    players.battle(pulse)

    return pulse

def summarize(plotter, songs, rounds, pulse, players, rankings, league_title):
    # display results
    print(f'{league_title} Results\n')
    for df in [songs, rounds, pulse, players, rankings]:
        print(df)

    plotter.add_league(league_title, players, rankings)
    
main(local=False)
