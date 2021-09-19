from datetime import datetime

from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from pandas import DataFrame

class Spotter:
    def __init__(self, credentials, database=None):
        self.credentials = credentials
        self.database = database

        self.sp = None
        self.connect_to_spotify()

    def connect_to_spotify(self):
        print('Connecting to Spotify API...')
        self.sp = Spotify(client_credentials_manager=SpotifyClientCredentials(**self.credentials))

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
                    }
        return elements

    def get_album_elements(self, uri):
        results = self.sp.album(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'genres': results['genres'],
                    'popularity': results['popularity'],
                    'release_date': self.get_date(results['release_date'], results['release_date_precision']),
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
                'artists': [artist['uri'] for artist in first_results[artists]],
                'album': first_result['album']['uri'],
                }
        return uris

    def get_all_elements_by_url(self, url):
        track_elements = self.get_track_elements(url)
        artist_elements = [self.get_album_elements(artist['uri']) for artist in track_elements['artists']]
        album_elements = self.get_album_elements(track_elements['album']['uri'])

        return track_elements, artist_elements, album_elements

    def get_all_elements_by_query(self, artist, title):
        uris = self.search_for_track(artist, title)
        track_elements = self.get_track_elements(uris['track'])
        artist_elements = [self.get_album_elements(uris['artist']) for artist in track_elements['artists']]
        album_elements = self.get_album_elements(uris['album'])

        return track_elements, artist_elements, album_elements

    def get_user_elements(self, user):
        results = self.sp.user(user)

        elements = {'uri': results['uri'],
                    'followers': results['followers']['total'],
                    'src': results['images'][0]['url'],
                    }
        return elements

    def update_database(self, database):
        self.database = database

        self.update_db_players()
        self.update_db_songs()
        self.update_db_tracks()
        self.update_db_artists()
        self.update_db_albums()
        self.update_db_genres()

    def update_db_songs(self):
        urls_db = self.database.get_song_urls()
        tracks_db = self.database.get_tracks()

        if len(urls_db):
            urls = [url for url in urls_db['track_url'] if url not in tracks_db['url']]
            tracks_db = tracks_db.append(DataFrame(urls, columns=['url']), ignore_index=True)
            self.database.store_tracks(tracks_db)
            
    def append_updates(self, df, updates_list, key='uri'):
        df_appended = df.append(DataFrame([u for u in updates_list if u not in df[key].values],
                                          columns=[key]), ignore_index=True)

        return df_appended

    def get_updates(self, df, func, key='uri'):
        df_update = df[df.isnull().any(1)].copy() # what can be used besides copy?

        df_elements = [func(uri) for uri in df_update[key]]
        df_to_update = DataFrame(df_elements, index=df_update.index)
        df_update.loc[:, df_to_update.columns] = df_to_update

        return df_update

    def update_db_players(self):
        print('\t...updating Spotify user information')
        players_db = self.database.get_players()

        players_update = self.get_updates(players_db, self.get_user_elements, key='username')
        self.database.store_players(players_update)    

    def update_db_tracks(self):
        print('\t...updating Spotify track information')
        tracks_db = self.database.get_tracks()

        tracks_update = self.get_updates(tracks_db, self.get_track_elements, key='url')
        self.database.store_tracks(tracks_update)

    def update_db_artists(self):
        print('\t...updating Spotify artist information')
        tracks_db = self.database.get_tracks()
        artist_uris = set(tracks_db['artist_uri'].sum())
        
        artists_db = self.database.get_artists()
        artists_db = self.append_updates(artists_db, artist_uris)
        
        artists_update = self.get_updates(artists_db, self.get_artist_elements)
        self.database.store_artists(artists_update)

    def update_db_albums(self):
        print('\t...updating Spotify album information')
        tracks_db = self.database.get_tracks()
        album_uris = tracks_db['album_uri']

        albums_db = self.database.get_albums()
        albums_db = self.append_updates(albums_db, album_uris)

        albums_update = self.get_updates(albums_db, self.get_album_elements)
        self.database.store_albums(albums_update)  

    def update_db_genres(self):
        print('\t...updating Spotify genre information')
        artists_db = self.database.get_artists()
        albums_db = self.database.get_albums()
        genres_db = self.database.get_genres()

        if len(albums_db):
            genre_names = set(artists_db['genres'].sum() + albums_db['genres'].sum())
            genres_db = self.append_updates(genres_db, genre_names, key='name')

            genres_update = genres_db
            self.database.store_genres(genres_update)