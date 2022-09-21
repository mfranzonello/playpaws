from datetime import datetime, timedelta
from math import ceil
import time

import requests
import six
from base64 import b64encode
from spotipy import Spotify, SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from pylast import LastFMNetwork, NetworkError
from pandas import DataFrame, isnull

from common.secret import get_secret
from common.words import Texter
from display.media import Gallery, Byter
from display.storage import Boxer, Googler
from display.streaming import Streamable

class Spotter(Streamable):
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

    def __init__(self, streamer=None):
        super().__init__()
        self.sp = None

        self.texter = Texter()
        self.byter = Byter()
        self.boxer = Boxer()
        self.googler = Googler()
        self.add_streamer(streamer)

    def connect_to_spotify(self, auth=False):
        self.streamer.print('Connecting to Spotify API...')

        client_credentials_manager = None
        auth_manager = None
        cache_handler = None

        if auth:
            data = {'grant_type': 'refresh_token',
                    'refresh_token': get_secret('SPOTIFY_REFRESH_TOKEN'),
                    }
            auth_header = b64encode(six.text_type(get_secret('SPOTIFY_CLIENT_ID')
                                                  + ':'
                                                  + get_secret('SPOTIFY_CLIENT_SECRET')).encode('ascii'))
            headers = {'Authorization': f'Basic {auth_header.decode("ascii")}'}
            response = requests.post('https://accounts.spotify.com/api/token', data=data, headers=headers)
            if response.ok:
                token_info = response.json()
                token_info['expires_at'] = int(time.time()) + token_info['expires_in']
                token_info['refresh_token'] = get_secret('SPOTIFY_REFRESH_TOKEN')

                access_token = token_info['access_token']
                cache_handler = MemoryCacheHandler(token_info)
            auth_manager = SpotifyOAuth(client_id=get_secret('SPOTIFY_CLIENT_ID'),
                                        client_secret=get_secret('SPOTIFY_CLIENT_SECRET'),
                                        redirect_uri=get_secret('SPOTIFY_REDIRECT_URL'),
                                        #requests_session=s,
                                        cache_handler=cache_handler,
                                        open_browser=False,
                                        scope='playlist-modify-public ugc-image-upload user-read-private user-read-email')
        else:
            client_credentials_manager = SpotifyClientCredentials(client_id=get_secret('SPOTIFY_CLIENT_ID'),
                                                                  client_secret=get_secret('SPOTIFY_CLIENT_SECRET'))
            
        self.sp = Spotify(client_credentials_manager=client_credentials_manager,
                          auth_manager=auth_manager)

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
                    'src': self.get_image_src(results['images'], name=results['name']),
                    }
        return elements

    def get_album_elements(self, uri):
        results = self.sp.album(uri)

        elements = {'uri': results['uri'],
                    'name': results['name'],
                    'genres': results['genres'],
                    'popularity': results['popularity'],
                    'release_date': self.get_date(results['release_date'], results['release_date_precision']),
                    'src': self.get_image_src(results['images'], name=results['name']),
                    }
        return elements

    def get_date(self, string, precision):
        pattern = {'year': '%Y',
                   'month': '%Y-%m',
                   'day': '%Y-%m-%d'}[precision]
        
        date = datetime.strptime(string, pattern)
            
        return date

    def get_image_src(self, result_images, name=None):
        if len(result_images):
            # get src from Spotify
            src = result_images[0]['url']

        elif name:
            # get first result from Google
            src = self.googler.get_image_src(name)

        else:
            # return null
            src = None

        return src

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
                    'src': self.get_image_src(results['images']),
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

    def output_playlists(self, database):
        self.database = database
        
        self.connect_to_spotify(auth=True)
        
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
        self.streamer.print('\t...updating user information')
        players_db = self.database.get_players_update_sp()

        if len(players_db):
            players_update = self.get_updates(players_db, self.get_user_elements, key='username')
            players_update['flagged'] = players_update['src'].apply(self.flag_src)
            self.database.store_players(players_update)    

    def flag_src(self, src, fb=7):
        if src is None:
            # no image detected, look again tomorrow
            flag = datetime.today().date() + timedelta(1)
        elif ('fbcdn.net' in src) or ('fbsbx.com' in src):
            # Facebook image detected, check again in a week in case it expired
            flag = datetime.today().date() + timedelta(fb)
        else:
            # presumably a Spotify CDN (i.scdn.co) image that doesn't expire from
            flag = None
        return flag

    def update_db_tracks(self):
        self.streamer.print('\t...updating track information')
        # get audio features information
        tracks_db = self.database.get_tracks_update_sp()
        
        if len(tracks_db):
            tracks_update_1 = self.get_updates(tracks_db, self.get_track_elements, key='url')
            tracks_update_2 = self.get_updates(tracks_db, self.get_audio_features, key='url')
            tracks_update = tracks_update_1.merge(tracks_update_2, on='url')
            self.database.store_tracks(tracks_update)

    def update_db_artists(self):
        self.streamer.print('\t...updating artist information')
        artists_db = self.database.get_artists_update_sp()
        
        if len(artists_db):
            ## note that some artists don't have images on Spotify -- should these be left blank permanently?
            artists_update = self.get_updates(artists_db, self.get_artist_elements)
            self.database.store_artists(artists_update)

    def update_db_albums(self):
        self.streamer.print('\t...updating album information')
        albums_db = self.database.get_albums_update_sp()

        if len(albums_db):
            albums_update = self.get_updates(albums_db, self.get_album_elements)
            self.database.store_albums(albums_update)  

    def update_db_genres(self):
        self.streamer.print('\t...updating genre information')
        genres_db = self.database.get_genres_update_sp()

        if len(genres_db):
            genres_update = genres_db.rename(columns={'genre': 'name'})
            self.database.store_genres(genres_update)

    def update_playlists(self):
        self.streamer.print('\t...updating playlists')

        self.gallery = Gallery(self.database, crop=True)
        
        self.update_complete_playlists()
        self.update_best_playlists()
        self.update_favorite_playlists()

        self.update_playlist_covers()

    def update_complete_playlists(self):
        rounds_db, playlists_db = self.database.get_theme_playlists(theme='complete')

        for i in rounds_db.index:
            sublist_uri = rounds_db['url'][i]

            league_id = rounds_db['league_id'][i]
            round_id = rounds_db['round_id'][i]

            db_query = playlists_db.query('league_id == @league_id') 
            if len(db_query):
                position = db_query.index[0]
                playlist_uri = db_query['uri'].iloc[0]

            else:
                position = len(playlists_db)
                league_name = self.database.get_league_name(league_id)
                playlist_uri = self.create_playlist(f'{league_name} - Complete')

                playlists_db.loc[position, ['league_id', 'uri']] = league_id, playlist_uri
                playlists_db.at[position, 'round_ids'] = []

            skip_rounds = playlists_db['round_ids'][position]
            
            if round_id not in skip_rounds:
                # skip over rounds already in playlist
                self.update_playlist(playlist_uri, sublist_uri=sublist_uri)

                add_rounds = [round_id]
                playlists_db.at[position, 'round_ids'] += add_rounds
            
        self.database.store_playlists(playlists_db, theme='complete')

    def update_best_playlists(self, quantile=0.25): ## move best to db.weights
        rounds_db, playlists_db = self.database.get_theme_playlists(theme='best')

        # trim to best songs
        rounds_db = rounds_db.set_index(['league_id', 'round_id'])[\
            rounds_db.set_index(['league_id', 'round_id'])['points'] >= rounds_db.groupby(['league_id', 'round_id'])[['points']].quantile(1-quantile).reindex_like(\
            rounds_db.set_index(['league_id', 'round_id']))['points']].dropna().reset_index().sort_values(['date', 'points'], ascending=[True, False])

        for league_id in rounds_db['league_id'].unique():
        
            db_query = playlists_db.query('league_id == @league_id')
            if len(db_query):
                position = db_query.index[0]
                playlist_uri = db_query['uri'][position]
               
            else:
                position = len(playlists_db)
                league_name = self.database.get_league_name(league_id)
                playlist_uri = self.create_playlist(f'{league_name} - Best Of')

                playlists_db.loc[len(playlists_db), ['league_id', 'uri']] = league_id, playlist_uri
                playlists_db.at[position, 'round_ids'] = []

            # skip over rounds already in playlist
            skip_rounds = playlists_db['round_ids'][position]
            query_results = rounds_db.query('(league_id == @league_id) & (round_id not in @skip_rounds)')
            track_uris = query_results['uri'].to_list()
            
            if len(track_uris):
                self.update_playlist(playlist_uri, track_uris=track_uris)

                add_rounds = [round_id for round_id in query_results['round_id'].unique() if round_id not in skip_rounds]
                playlists_db.at[position, 'round_ids'] += add_rounds

        self.database.store_playlists(playlists_db, theme='best')

    def update_favorite_playlists(self):
        rounds_db, playlists_db = self.database.get_theme_playlists(theme='favorite')

        rounds_db = rounds_db.sort_values(['date', 'vote'], ascending=[True, False])

        for league_id in rounds_db['league_id'].unique():
            for player_id in rounds_db.query('(league_id == @league_id)')['player_id'].unique():
                theme = 'favorite'

                db_query = playlists_db.query('(league_id == @league_id) & (player_id == @player_id)')
                if len(db_query):
                    position = db_query.index[0]
                    playlist_uri = db_query['uri'][position]
                    
                else:
                    position = len(playlists_db)
                    league_title = self.database.get_league_name(league_id)
                    player_name = self.database.get_player_name(player_id)
                    player_name_print = self.texter.get_display_name_full(player_name)
                    playlist_uri = self.create_playlist(f'{league_title} - {player_name_print}\'s Favorites')

                    playlists_db.loc[len(playlists_db), ['league_id', 'uri', 'theme', 'player_id']] = league_id, playlist_uri, theme, player_id
                    playlists_db.at[position, 'round_ids'] = []
                    
                # skip over rounds already in playlist
                skip_rounds = playlists_db['round_ids'][position]
                query_results = rounds_db.query('(league_id == @league_id) & (player_id == @player_id) & (round_id not in @skip_rounds)')
                track_uris = query_results['uri'].to_list()

                if len(track_uris):
                    self.update_playlist(playlist_uri, track_uris=track_uris)

                    add_rounds = [round_id for round_id in query_results['round_id'].unique() if round_id not in skip_rounds]
                    playlists_db.at[position, 'round_ids'] += add_rounds

        self.database.store_playlists(playlists_db, theme='favorite')

    def get_playlist_uris(self, playlist_uri, external_url=False):
        finished = False
        uris = []

        while not finished:
            results = self.sp.playlist_tracks(playlist_uri, offset=len(uris))
            uris += [r['track']['external_urls']['spotify'] if external_url else r['track']['uri'] for r in results['items']]
            if results['next'] is None:
                finished = True

        return uris

    def create_playlist(self, name):
        """create a new playlist"""
        playlist = self.sp.user_playlist_create(get_secret('SPOTIFY_USER_ID'), name, public=True, collaborative=False, description='')
        uri = playlist['uri']
        
        return uri

    def update_playlist_covers(self):
        """update playlist covers"""
        playlists_db = self.database.get_playlists()

        for i in playlists_db.index:
            league_id, src, uri, theme = playlists_db.loc[i, ['league_id', 'src', 'uri', 'theme']]
            playlists_db['src'][i] = self.check_playlist_image(league_id, src, uri, theme)

        self.database.store_playlists(playlists_db)

    def check_playlist_image(self, league_id, src, uri, theme='complete'):
        """check if a playlist has an image already or if the src has changed and update if necessary"""
        cover_src = self.get_playlist_cover(uri)

        mosaic = 'https://mosaic.scdn.co'
        if isnull(src) or isnull(cover_src) or (cover_src[:len(mosaic)] == mosaic):
            # needs an image
            league_title = self.database.get_league_name(league_id)
            image_src = self.boxer.get_cover(league_title)
            if image_src:

                # found image in Dropbox
                if 'favorite' in theme:
                    player_name = theme[theme.find(' - ') + len(' - '):]
                    overlay = self.gallery.get_image(player_name)

                elif theme == 'best':
                    overlay = self.boxer.get_clipart('best')

                else:
                    overlay = None

                self.update_playlist_image(uri, image_src, overlay=overlay)

                new_src = self.get_playlist_cover(uri)

            else:
                # couldn't find an image
                new_src = None

        else:
            # already has an image
            new_src = cover_src    

        return new_src

    def get_playlist_cover(self, uri):
        current_cover = self.sp.playlist_cover_image(uri)
        ## sample error: HTTPSConnectionPool(host='api.spotify.com', port=443): Read timed out. (read timeout=5)
        if len(current_cover):
            cover_src = current_cover[0]['url']
        else:
            cover_src = None

        return cover_src


    def update_playlist_image(self, uri, image_src, overlay=None):
        image_b64 = self.byter.byte_me(image_src, overlay=overlay)
        self.sp.playlist_upload_cover_image(uri, image_b64)

    def update_playlist(self, playlist_uri, sublist_uri=None, track_uris=None):
        existing_uris = self.get_playlist_uris(playlist_uri)

        if track_uris:
            new_uris = track_uris
        elif sublist_uri:
            new_uris = self.get_playlist_uris(sublist_uri)

        update_uris = [uri for uri in new_uris if uri not in existing_uris]

        segment_size = 100
        for update_uris_segment in [update_uris[i*segment_size:min(len(update_uris), (i+1)*segment_size)] \
            for i in range(ceil(len(update_uris)/segment_size))]:

            self.sp.playlist_add_items(playlist_uri, update_uris_segment)

    def reset_playlist(self, playlist_uri):
        track_uris = []
        self.sp.playlist_replace_items(playlist_uri, track_uris)

class FMer(Streamable):
    def __init__(self, streamer=None):
        super().__init__()
        self.fm = None
        self.add_streamer(streamer)

    def connect_to_lastfm(self):
        self.streamer.print('Connecting to LastFM API...')
        self.fm = LastFMNetwork(api_key=get_secret('LASTFM_API_KEY'), api_secret=get_secret('LASTFM_API_SECRET'))

    def clean_title(self, title):
        # remove featuring artists and pull remixes
        self.texter = Texter()

        title, mix1 = self.texter.remove_parenthetical(title, ['Remix', 'Mix'], position='end', case_sensitive=True)
        title, mix2 = self.texter.remove_parenthetical(title, ['Remix', 'Mix'], parentheses=[['- ', '$']], position='end', case_sensitive=True)
        title, _ = self.texter.remove_parenthetical(title, ['feat', 'with ', 'Duet with '], position='start') #<- should with only be for []?
        title, _ = self.texter.remove_parenthetical(title, ['Live '], position='start', parentheses=[['- ', '$']], middle=[' with '])
        title, _ = self.texter.remove_parenthetical(title, ['feat. '], position='start', parentheses=[['', '$']])
        title = self.texter.drop_dash(title)

        mixes = [m for m in filter(None, [mix1, mix2])]
        mix = mixes[0] if len(mixes) else None

        return title, mix
    
    def get_track_info(self, artist, title):
        track = self.fm.get_track(artist, title)

        self.streamer.print(f'\t...{artist} - {title}')

        if len(track.info):
            max_tags = 5
            top_tags = track.get_top_tags()

            elements = {'scrobbles': track.get_playcount(),
                        'listeners': track.get_listener_count(),
                        'top_tags': [tag.item.get_name() for tag in top_tags[:min(max_tags, len(top_tags))]],
                        }
        else: # ignore when Last.fm has no info
            elements = {x: None for x in ['scrobbles', 'listeners', 'top_tags']}
        
        return elements

    def update_database(self, database):
        self.database = database

        self.connect_to_lastfm()

        self.update_db_tracks()

    def update_db_tracks(self):
        self.streamer.print('\t...updating track information')
        tracks_update_db = self.database.get_tracks_update_fm()

        # strip featured artists and remix call outs from track title
        ## add binary for Remix status?
        if len(tracks_update_db):
            tracks_update_db[['title', 'mix']] = tracks_update_db.apply(lambda x: self.clean_title(x['unclean']),
                                                                        axis=1, result_type='expand')

            # get LastFM elements
            # limit how many are updated in one go to keep under rate limites
            segment_size = 50
            for tracks_update_db_segment in [tracks_update_db.loc[i*segment_size:(i+1)*segment_size-1].copy() \
                                            for i in range(ceil(len(tracks_update_db)/segment_size))]:
                try:
                    df_elements = [self.get_track_info(artist, title) for artist, title in tracks_update_db_segment[['artist', 'title']].values]

                    df_to_update = DataFrame(df_elements, index=tracks_update_db_segment.index)
                    tracks_update_db_segment.loc[:, df_to_update.columns] = df_to_update
                    self.database.store_tracks(tracks_update_db_segment)

                except NetworkError:
                    print('Error: disconnected (possibly too many API calls)')
                    break

class Charter(Streamable):
    def __init__(self):
        super().__init__()
        self.token = None
        self.refresh = get_secret('CHARTMETRICS_RERESH_TOKEN')
    def connect_to_chartmetrics(self):
        endpoint = 'https://api.chartmetric.com/api/token'
        data = {'refreshtoken': self.refresh}
        headers = {'Content-Type': 'application/json'}
        
        response = requests.post(endpoint, data=data, headers=headers)
        if response.ok:
            self.token = response.json()['access_token']
            self.refresh = response.json()['refresh_token']

    def auth_header(self):
        return {'Authorization': f'Bearer {self.token}'}

    def get_json(self, endpoint, data):
        response = requests.get(endpoint, data=data, headers=self.auth_header())
        if response.ok:
            json = response.json()

        else:
            json = None
        return json

    def get_monthly_listeners(self, uri):
        endpoint = 'https://api.chartmetric.com/api/artist/anr/by/social-index'
        data = {}

        json = self.get_json(endpoint, data)
        
