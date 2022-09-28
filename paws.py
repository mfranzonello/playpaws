from common.data import Database
from preparing.update import Updater, Musician
from crunching.analyze import Analyzer
from display.streaming import Printer

def update_web_data(database):
    # extract data from MusicLeague
    updater = Updater(database)
    updater.update_musicleague()

def update_api_data(database):
    # enhance data with Spotify and LastFM
    musician = Musician(database)
    musician.update_spotify()
    musician.update_lastfm()

def analyze_data(database):
    # analyze MusicLeague data
    analyzer = Analyzer(database)
    analyzer.analyze_all()
    analyzer.place_all()

def output_playlists(database):
    # update Spotify playlists
    musician = Musician(database)
    musician.output_playlists()

def main():
    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = Database()
    
    # update data in database from MusicLeague webpage
    update_web_data(database)

    # analyze data from rounds
    analyze_data(database)

    # update data in database from Spotify and LastFM APIs
    update_api_data(database)

    # output Spotify playlists
    output_playlists(database)

if __name__ == '__main__':
    main()