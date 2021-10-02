from datetime import datetime
import re
from os import getenv
from base64 import b64encode

import requests
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth #SpotifyClientCredentials, 
from pylast import LastFMNetwork
from pandas import DataFrame

from streaming import streamer

class Spotter:
    audio_features = ['danceability',
                      'energy',
                      'key',
                      'loudness',
                      'mode',
                      'speechiness',
                      'acousticness',
                      'instrumentalness',
                      'liveness',
                      'valence',
                      'tempo']

    def __init__(self, database=None): #credentials
        #self.credentials = credentials
        self.database = database

        self.sp = None

    def connect_to_spotify(self):
        streamer.print('Connecting to Spotify API...')
        #client_credentials_manager = SpotifyClientCredentials(client_id=getenv('SPOTIFY_CLIENT_ID'),
        #                                                      client_secret=genev('SPOTIFY_CLIENT_SECRET'))
        auth_manager = SpotifyOAuth(client_id=getenv('SPOTIFY_CLIENT_ID'),
                                    client_secret=getenv('SPOTIFY_CLIENT_SECRET'),
                                    redirect_uri=getenv('SPOTIFY_REDIRECT_URL'),
                                    scope='playlist-modify-public ugc-image-upload user-read-private user-read-email')

        self.sp = Spotify(auth_manager=auth_manager)#client_credentials_manager=client_credentials_manager)

    def get_track_elements(self, uri):
        results = self.sp.track(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'artist_uri': [artist['uri'] for artist in results['artists']],
                    'album_uri': results['album']['uri'],
                    'explicit': results['explicit'],
                    'popularity': results['popularity'],
                    }
        return elements

    def get_artist_elements(self, uri):
        results = self.sp.artist(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'genres': results['genres'],
                    'popularity': results['popularity'],
                    'followers': results['followers']['total'],
                    'src': results['images'][0]['url'],
                    }
        return elements

    def get_album_elements(self, uri):
        results = self.sp.album(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'genres': results['genres'],
                    'popularity': results['popularity'],
                    'release_date': self.get_date(results['release_date'], results['release_date_precision']),
                    'src': results['images'][0]['url'],
                    }
        return elements

    def get_date(self, string, precision):
        if precision == 'year':
            date = datetime.strptime(string, '%Y')
        else: # precision == 'day':
            date = datetime.strptime(string, '%Y-%m-%d')
            
        return date

    def search_for_track(self, artist, title):
        results = self.sp.search(q=f'artist: {artist} track: {title}', type='track')
        first_result = results['tracks']['items'][0]

        uris = {'track': first_result['uri'],
                'artists': [artist['uri'] for artist in first_result['artists']],
                'album': first_result['album']['uri'],
                }
        return uris

    def get_user_elements(self, user):
        results = self.sp.user(user)

        elements = {'uri': results['uri'],
                    'followers': results['followers']['total'],
                    'src': results['images'][0]['url'],
                    }
        return elements

    def get_audio_features(self, uri):
        results = self.sp.audio_features(uri)

        features = {key: results[0][key] for key in self.audio_features}
        features['duration'] = results[0]['duration_ms'] / 1000 / 60

        return features

    def update_database(self, database):
        self.database = database

        self.connect_to_spotify()

        self.update_db_players()
        self.update_db_tracks()
        self.update_db_artists()
        self.update_db_albums()
        self.update_db_genres()

        self.update_playlists()
           
    def append_updates(self, df, updates_list, key='uri', updates_only=False):
        to_add = [u for u in updates_list if u not in df[key].values]
        if updates_only:
            df_appended = DataFrame(columns=df.columns)
            df_appended[key] = to_add
        else:
            df_appended = df.append(DataFrame(to_add, columns=[key]), ignore_index=True)

        return df_appended

    def get_updates(self, df, func, key='uri'):
        df_update = df.copy()#[df.isnull().any(1)].copy() # what can be used besides copy?

        df_elements = [func(uri) for uri in df_update[key]]
        df_to_update = DataFrame(df_elements, index=df_update.index)
        df_update.loc[:, df_to_update.columns] = df_to_update

        return df_update

    def update_db_players(self):
        streamer.print('\t...updating user information')
        players_db = self.database.get_players_update_sp()

        if len(players_db):
            players_update = self.get_updates(players_db, self.get_user_elements, key='username')
            self.database.store_players(players_update)    

    def update_db_tracks(self):
        streamer.print('\t...updating track information')
        # get audio features information
        tracks_db = self.database.get_tracks_update_sp()
        
        if len(tracks_db):
            tracks_update_1 = self.get_updates(tracks_db, self.get_track_elements, key='url')
            tracks_update_2 = self.get_updates(tracks_db, self.get_audio_features, key='url')
            tracks_update = tracks_update_1.merge(tracks_update_2, on='url')
            self.database.store_tracks(tracks_update)

    def update_db_artists(self):
        streamer.print('\t...updating artist information')
        artists_db = self.database.get_artists_update_sp()
        
        if len(artists_db):
            artists_update = self.get_updates(artists_db, self.get_artist_elements)
            self.database.store_artists(artists_update)

    def update_db_albums(self):
        streamer.print('\t...updating album information')
        albums_db = self.database.get_albums_update_sp()

        if len(albums_db):
            albums_update = self.get_updates(albums_db, self.get_album_elements)
            self.database.store_albums(albums_update)  

    def update_db_genres(self):
        streamer.print('\t...updating genre information')
        genres_db = self.database.get_genres_update_sp()

        if len(genres_db):
            genres_update = genres_db
            self.database.store_genres(genres_update)

    def update_playlists(self):
        streamer.print('\t...updating playlists')

        self.update_complete_playlists()
        #self.update_best_playlists()

    def update_complete_playlists(self):
        rounds_db, playlists_db = self.database.get_playlists()

        for i in rounds_db.index:
            sublist_uri = rounds_db['url'][i]

            league_title = rounds_db['league'][i]

            if league_title not in playlists_db['league'].values:
                playlist_uri = self.create_playlist(league_title)
                playlists_db.loc[len(playlists_db), ['league', 'uri']] = league_title, playlist_uri

            else:
                playlist_uri = playlists_db[playlists_db['league'] == league_title]['uri'].iloc[0]

            self.update_playlist(playlist_uri, sublist_uri=sublist_uri)

        for i in playlists_db.index:
            playlists_db['src'][i] = self.check_playlist_image(playlists_db['league'][i],
                                                               playlists_db['src'][i])
            
        self.database.store_playlists(playlists_db)

    def update_best_playlists(self):
        rounds_db, playlists_db = self.database.get_playlists(theme='best')

        league_titles = rounds_db['league'].unique()

        for league_title in league_titles:
        
            if league_title not in playlists_db['league'].values:
                playlist_uri = self.create_playlist(f'{league_title} - Best Of')
                playlists_db.loc[len(playlists_db), ['league', 'uri']] = league_title, playlist_uri

            else:
                playlist_uri = playlists_db[playlists_db['league'] == league_title]['uri'].iloc[0]

            track_uris = rounds_db[rounds_db['league'] == league_title]['uri'].to_list()

            self.update_playlist(playlist_uri, track_uris=track_uris)

        for i in playlists_db.index:
            playlists_db['src'][i] = self.check_playlist_image(playlists_db['league'][i],
                                                               playlists_db['src'][i])

        self.database.store_playlists(playlists_db, theme='best')

    def check_playlist_image(self, league_title, src):
        image_src = self.database.get_cover(league_title)
        if (image_src is not None) and (image_src != src):
            self.update_playlist_image(image_src)
            
            new_src = image_src

        else:
            new_src = src

        return new_src

    def get_playlist_uris(self, playlist_uri):
        finished = False
        uris = []
        while not finished:
            results = self.sp.playlist_tracks(playlist_uri, offset=len(uris))
            uris += [r['track']['uri'] for r in results['items']]
            if results['next'] is None:
                finished = True

        return uris

    def create_playlist(self, name):
        playlist = self.sp.user_playlist_create(getenv('SPOTIFY_USER_ID'), name, public=True, collaborative=False, description='')
        uri = playlist['uri']
        
        return uri

    def update_playlist_image(self, uri, image_src):
        image64 = b64encode(requests.get(image_src).content)
        self.sp.playlist_upload_cover_image(uri, image64)

    def update_playlist(self, playlist_uri, sublist_uri=None, track_uris=None):
        existing_uris = self.get_playlist_uris(playlist_uri)

        if track_uris:
            new_uris = track_uris
        elif sublist_uri:
            new_uris = self.get_playlist_uris(sublist_uri)

        update_uris = [uri for uri in new_uris if uri not in existing_uris]

        if len(update_uris):
            self.sp.playlist_add_items(playlist_uri, update_uris)

class FMer:
    def __init__(self): #, credentials):
        self.fm = None

    def connect_to_lastfm(self):
        streamer.print('Connecting to LastFM API...')
        self.fm = LastFMNetwork(api_key=getenv('LASTFM_API_KEY'), api_secret=getenv('LASTFM_API_SECRET'))

        
    def clean_title(self, title):
        # remove featuring artists
        featuring = ['feat', 'with']
        title = self.remove_parenthetical(title, ['feat', 'with '], position='start') #<- should with only be for []?
        title = self.remove_parenthetical(title, ['remix'], position='end')

        # remove description after dash
        if ' - ' in title:
            title = title[:title.find(' - ')].strip()

        return title

    def remove_parenthetical(self, title, words, position):
        parentheses = [['(', ')'], ['[', ']']]
        capture_s = '(.*?)' if position == 'end' else ''
        capture_e = '(.*?)' if position == 'start' else ''
        pattern = '|'.join(f'(\{s}{capture_s}{w}{capture_e}\{e})' for w in words for s, e in parentheses)
        searched = re.search(pattern, title, flags=re.IGNORECASE)
        if searched:
            title = title.replace(next(s for s in searched.groups() if s), '').strip()

        return title

    def get_track_info(self, artist, title):
        track = self.fm.get_track(artist, title)

        streamer.print(f'{artist} - {title}')

        max_tags = 5
        top_tags = track.get_top_tags()
        elements = {'scrobbles': track.get_playcount(),
                    'listeners': track.get_listener_count(),
                    'top_tags': [tag.item.get_name() for tag in top_tags[:min(max_tags, len(top_tags))]],
                    }
        
        return elements

    def update_database(self, database):
        self.database = database

        self.connect_to_lastfm()


        self.update_db_tracks()

    def update_db_tracks(self):
        streamer.print('\t...updating track information')
        tracks_update_db = self.database.get_tracks_update_fm()

        # strip featured artists and remix call outs from track title
        ## add binary for Remix status?
        if len(tracks_update_db):
            tracks_update_db['title'] = tracks_update_db.apply(lambda x: self.clean_title(x['unclean']), axis=1)

            # get LastFM elements
            df_elements = [self.get_track_info(artist, title) for artist, title in tracks_update_db[['artist', 'title']].values]

            df_to_update = DataFrame(df_elements, index=tracks_update_db.index)
            tracks_update_db.loc[:, df_to_update.columns] = df_to_update
            self.database.store_tracks(tracks_update_db)