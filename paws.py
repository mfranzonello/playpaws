from data import Database
from update import Updater
from plotting import Printer
 
def update_data(database):
    # scrape data from MusicLeague, Spotify and LastFM
    updater = Updater(database)
    updater.update_database()
    updater.update_spotify()
    updater.update_lastfm()

def main():
    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = Database('https://musicleague.app')
    
    # update data in database from web
    update_data(database)

main()