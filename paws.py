from data import Database
from update import Updater, Musician
from analyze import Analyzer
from streaming import Printer

def update_web_data(database):
    # scrape data from MusicLeague, Spotify and LastFM
    updater = Updater(database)
    updater.update_musicleague()

def update_api_data(database):
    musician = Musician(database)
    musician.update_spotify()
    musician.update_lastfm()

def analyze_data(database):
    # analyze MusicLeague data
    analyzer = Analyzer(database)
    analyzer.analyze_all()

def main():
    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = Database('https://musicleague.app')
    
    # update data in database from MusicLeague webpage
    #update_web_data(database)

    # analyze data from rounds
    #analyze_data(database)

    # update data in database from Spotify and LastFM APIs
    update_api_data(database)

main()