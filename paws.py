''' Updates from MusicLeague and analyzes data '''

from common.data import Database
from preparing.messenger import Mailer
from preparing.update import Updater, Musician
from crunching.analyze import Analyzer
from display.streaming import Printer

def check_for_updates(mailer):
    ''' look at gmail for notifications '''
    update_needed = mailer.check_mail()
    return update_needed

def update_web_data(database):
    ''' extract data from MusicLeague '''
    updater = Updater(database)
    updater.update_musicleague()

def update_api_data(database):
    ''' enhance data with Spotify and LastFM '''
    musician = Musician(database)
    musician.update_spotify()
    musician.update_lastfm()
    musician.update_wiki()

def analyze_data(database):
    ''' analyze MusicLeague data '''
    analyzer = Analyzer(database)
    analyzer.place_all()

def output_playlists(database):
    ''' update Spotify playlists '''
    musician = Musician(database)
    musician.output_playlists()

def close_out(mailer):
    ''' don't need to check mail again ''' 
    mailer.mark_as_read()

def main():
    printer = Printer()
    printer.clear_screen()

    # check messages
    mailer = Mailer()
    update_needed = check_for_updates(mailer)
    if update_needed:

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

        # mark as complete
        close_out(mailer)
        

if __name__ == '__main__':
    main()