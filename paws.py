''' Updates from MusicLeague and analyzes data '''

import display.printing
from common.data import Database
from preparing.messenger import Mailer
from preparing.update import Updater, Musician
from crunching.analyze import Analyzer

def check_for_updates(mailer):
    ''' look at gmail for notifications '''
    league_ids = mailer.check_mail()
    return league_ids

def update_web_data(database, league_ids=None):
    ''' extract data from MusicLeague '''
    updater = Updater(database)
    updater.update_musicleague(league_ids=league_ids)

def update_api_data(database):
    ''' enhance data with Spotify and LastFM '''
    musician = Musician(database)
    musician.update_spotify()
    musician.update_lastfm()
    musician.update_wiki()

def place_data(database):
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
    # prepare database
    database = Database()
    
    # check messages
    mailer = Mailer(database)
    league_ids = check_for_updates(mailer)

    # update database and playlists with new data
    if league_ids:
        # update data in database from MusicLeague webpage
        update_web_data(database, league_ids=league_ids)

        # update data in database from Spotify and LastFM APIs
        update_api_data(database)

        # output Spotify playlists
        output_playlists(database)

        # mark as complete
        close_out(mailer)

    # place data from rounds
    place_data(database)
        
if __name__ == '__main__':
    main()