from stopipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

client_id = ''
client_secret = ''

class Spotter:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

        client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
        self.sp = Spotify(client_credentials_manager=client_credentials_manager)

    def get_track_elements(self, uri):
        results = self.sp.track(uri)

        elements = {'name': results['name'],
                    'artists': [artist['name'] for artist in results['artists']],
                    'album': results['album']['name'],
                    'explicit': results['explicit'],
                    'popularity': results['popularity'],
                    'uri': results['uri'],
                    }
        return elements

    def get_artist_elements(self, uri):
        results = self.sp.artist(uri)

        elements = {'name': results['name'],
                    'genres': results['genres'],
                    'popularity': results['popularity'],
                    'followers': results['followers']['total'],
                    'uri': results['uri'],
                    }
        return elements

    def get_album_elements(self, uri):
        results = self.ap.album(uri)

        elements = {'name': results['name'],
                    'popularity': results['popularity'],
                    'release_date': results['release_date'],
                    'uri': results['uri'],
                    }
        elements = {}
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
        arist_elements = [self.get_album_elements(artist['uri']) for artist in track_elements['artists']]
        album_elements = self.get_album_elements(track_elements['album']['uri'])

        return track_elements, artist_elements, album_elements

    def get_all_elements_by_query(self, artist, title):
        uris = self.search_for_track(artist, title)
        track_elements = self.get_track_elements(uris['track'])
        arist_elements = [self.get_album_elements(uris['artist']) for artist in track_elements['artists']]
        album_elements = self.get_album_elements(uris['album'])

        return track_elements, artist_elements, album_elements


