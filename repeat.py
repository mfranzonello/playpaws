from common.data import Database
from preparing.update import Musician
from display.streaming import Printer

def update_playlists(database):
    musician = Musician(database)
    musician.update_playlists()
    
def main():
    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = Database('https://musicleague.app')
    
    # update Spotify playlists
    update_playlists(database)

if __name__ == '__main__':
    main()