from itertools import chain
import re

from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from pandas import DataFrame

class Spotter:
    def __init__(self, credentials, database=None):
        client_credentials_manager = SpotifyClientCredentials(**credentials)
        self.sp = Spotify(client_credentials_manager=client_credentials_manager)
        self.database = database

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
        results = self.ap.album(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'popularity': results['popularity'],
                    'release_date': results['release_date'],
                    }
        return elements

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
        #self.update_db_tracks()
        #self.update_db_artists()
        #self.update_db_albums()
        #self.update_db_genres()

    def update_db_songs(self):
        urls_db = self.database.get_song_urls()
        tracks_db = self.database.get_tracks()

        if len(urls_db):
            uris = [url for url in urls_db['track_url'] if url not in tracks_db['uri']]
            tracks_db = tracks_db.append(DataFrame(uris, columns=['uri']), ignore_index=True)
            self.database.store_tracks(tracks_db)
            
    def get_updates(self, df, func, key='uri'):
        df_update = df[df.isnull().any(1)]

        df_elements = [func(uri) for uri in df_update[key]]
        df_to_update = DataFrame(df_elements, index=df_update.index)
        #df_columns = df_elements[0].keys() #list(set().union(*(d.keys() for d in df_elements)))  
        df_update[df_to_update.columns] = df_to_update

        return df_update

    def update_db_tracks(self):
        tracks_db = self.database.get_tracks()

        tracks_update = self.get_updates(tracks_db, self.get_track_elements)
        self.database.store_tracks(tracks_update)

    def update_db_artists(self):
        tracks_db = self.database.get_tracks()
        artist_uris = list(set(chain(*tracks_db['artist_uri'])))

        artists_db = self.database.get_artists()
        artist_uris_update = [uri for uri in artist_uris if uri not in artists_db['uri']]

        artists_db = artists_db.append(DataFrame(artist_uris_update,
                                                 columns=['uri']), ignore_index=True)

        artists_update = self.get_updates(artists_db, self.spotter.get_artists_elements)
        self.database.store_artists(artists_update)

    def update_db_albums(self):
        tracks_db = self.database.get_tracks()
        album_uris = tracks_db['album_uri']

        albums_db = self.database.get_albums()
        album_uris_update = [uri for uri in album_uris if uri not in albums_db['uri']]

        albums_db = albums_db.append(DataFrame(album_uris_update,
                                               columns=['uri']), ignore_index=True)

        albums_update = self.get_updates(albums_db, self.spotter.get_album_elements)
        self.database.store_albums(albums_update)

    def update_db_players(self):
        players_db = self.database.get_players()

        players_update = self.get_updates(players_db, self.get_user_elements, key='username')
        self.database.store_players(players_update)       

    def update_db_genres(self):
        albums_db = self.database.get_albums()
        genres_db = self.database.get_genres()

        if len(albums_db):
            genres = list(set(chain(*albums_db['genre'])))
            genres = [genre for genre in genres_db['name'] if genre not in genres]
            genres_update = genres_db.append(DataFrame(genres, columns=['name']), ignore_index=True)
            self.database.store_genres(genres_update)