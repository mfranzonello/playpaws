''' Updates from MusicLeague and analyzes data '''

import display.printing
from common.data import Database
from preparing.messenger import GMailer
from preparing.update import Updater, Musician
from crunching.analyze import Analyzer

def check_for_updates(gmailer):
    ''' look at gmail for notifications '''
    league_ids = gmailer.check_mail()
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

def close_out(gmailer):
    ''' don't need to check mail again ''' 
    gmailer.mark_as_read()

def main():
    # prepare database
    database = Database()
    
    # check messages
    gmailer = GMailer(database)
    league_ids = check_for_updates(gmailer)

    # update database and playlists with new data
    if league_ids:
        # update data in database from MusicLeague webpage
        update_web_data(database, league_ids=league_ids)

        # update data in database from Spotify and LastFM APIs
        update_api_data(database)

        # output Spotify playlists
        output_playlists(database)

        # mark as complete
        close_out(gmailer)

    # place data from rounds
    place_data(database)
        
if __name__ == '__main__':
    main()