''' Creating playlists and getting additonal music info from Spotify and LastFM '''

from datetime import datetime, timedelta
from math import ceil
import time
from base64 import b64encode
from urllib import parse

import requests
import six
from spotipy import Spotify, SpotifyClientCredentials
from spotipy.oauth2 import SpotifyOAuth
from spotipy.cache_handler import MemoryCacheHandler
from pandas import DataFrame, isnull

from common.secret import get_secret
from common.words import Texter
from common.locations import MOSAIC_URL, SPOTIFY_AUTH_URL, SPOTIFY_REDIRECT, LASTFM_URL, WIKI_URL
from common.calling import Caller
from display.media import Gallery, Byter
from display.storage import Boxer, GImager
from display.streaming import Streamable

class Spotter(Streamable, Caller):
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
        self.gimager = GImager()
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

            token_info = self.get_token(f'{SPOTIFY_AUTH_URL}/api/token',
                                        refresh_token=get_secret('SPOTIFY_REFRESH_TOKEN'),
                                        data=data, headers=headers)

            if token_info:
                access_token = token_info['access_token']
                cache_handler = MemoryCacheHandler(token_info)

            auth_manager = SpotifyOAuth(client_id=get_secret('SPOTIFY_CLIENT_ID'),
                                        client_secret=get_secret('SPOTIFY_CLIENT_SECRET'),
                                        redirect_uri=SPOTIFY_REDIRECT,
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
            src = self.gimager.get_image_src(name)

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
            tracks_update_1 = self.get_updates(tracks_db, self.get_track_elements, key='uri')
            tracks_update_2 = self.get_updates(tracks_db, self.get_audio_features, key='uri')
            tracks_update = tracks_update_1.merge(tracks_update_2, on='uri')
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

        ##self.update_playlist_covers()
        print('COVERS UPDATE: SUCCESS!')

    def update_complete_playlists(self):
        theme = 'complete'

        player_id = self.database.get_god_id()
        playlists_db = self.database.get_playlists(theme)
        playtracks_db = self.database.get_theme_playlists(theme=theme)

        for i in playtracks_db.index:
            league_id = playtracks_db['league_id'][i]
            track_uris = playtracks_db['track_uris'][i]

            query = playlists_db.query('league_id == @league_id')
            if len(query):
                playlist_uri = query['uri'].iloc[0]
            else:
                league_title = self.database.get_league_name(league_id)
                playlist_title = f'{league_title} - Complete'
                playlist_uri = self.create_playlist(playlist_title)
                playlists_db.loc[len(playlists_db), ['uri', 'theme', 'league_id', 'player_id']] \
                    = [playlist_uri, theme, league_id, player_id]
                
            self.update_playlist(playlist_uri, track_uris)
            
        self.database.store_playlists(playlists_db, theme=theme)
        print(f'THEME: {theme} SUCCESS!')

    def update_best_playlists(self):
        theme = 'best'

        player_id = self.database.get_god_id()
        playlists_db = self.database.get_playlists()
        playtracks_db = self.database.get_theme_playlists(theme=theme)

        for i in playtracks_db.index:
            league_id = playtracks_db['league_id'][i]
            track_uris = playtracks_db['track_uris'][i]

            query = playlists_db.query('league_id == @league_id')
            if len(query):
                playlist_uri = query['uri'].iloc[0]
            else:
                league_title = self.database.get_league_name(league_id)
                playlist_title = f'{league_title} - Best Of'
                playlist_uri = self.create_playlist(playlist_title)
                playlists_db.loc[len(playlists_db), ['uri', 'theme', 'league_id', 'player_id']] \
                    = [playlist_uri, theme, league_id, player_id]
                
            self.update_playlist(playlist_uri, track_uris)
            
        self.database.store_playlists(playlists_db, theme=theme)
        print(f'THEME: {theme} SUCCESS!')

    def update_favorite_playlists(self):
        theme = 'favorite'

        playlists_db = self.database.get_playlists(theme)
        playtracks_db = self.database.get_theme_playlists(theme=theme)

        for i in playtracks_db.index:
            league_id = playtracks_db['league_id'][i]
            track_uris = playtracks_db['track_uris'][i]
            player_id = playtracks_db['player_id'][i]

            query = playlists_db.query('league_id == @league_id & player_id == @player_id')
            if len(query):
                playlist_uri = query['uri'].iloc[0]
            else:
                league_title = self.database.get_league_name(league_id)
                player_name = self.database.get_player_name(player_id)
                playlist_title = f'{league_title} - {player_name}\' Favorites'
                playlist_uri = self.create_playlist(playlist_title)
                playlists_db.loc[len(playlists_db), ['uri', 'theme', 'league_id', 'player_id']] \
                    = [playlist_uri, theme, league_id, player_id]
                
            self.update_playlist(playlist_uri, track_uris)
            
        self.database.store_playlists(playlists_db, theme=theme)
        print(f'THEME: {theme} SUCCESS!')
        
    def get_playlist_uris(self, playlist_uri, external_url=False):
        finished = False
        uris = []

        while not finished:
            offset = len(uris)
            results = self.sp.playlist_tracks(playlist_uri, offset=offset)
            uris += [r['track']['external_urls']['spotify'] if external_url else r['track']['uri'] for r in results['items']]
            finished = results['next'] is None

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

    def check_playlist_image(self, league_id, src, uri, theme):
        """check if a playlist has an image already or if the src has changed and update if necessary"""
        cover_src = self.get_playlist_cover(uri)

        if isnull(src) or isnull(cover_src) or (cover_src[:len(MOSAIC_URL)] == MOSAIC_URL):
            # needs an image
            league_title = self.database.get_league_name(league_id)
            image_src = self.boxer.get_cover(league_title)
            if image_src:

                # found image in Dropbox
                if theme == 'favorite':
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

    def update_playlist(self, playlist_uri, track_uris):
        existing_uris = self.get_playlist_uris(playlist_uri)

        track_uris = track_uris
        
        update_uris = [uri for uri in track_uris if uri not in existing_uris]

        segment_size = 100
        for update_uris_segment in [update_uris[i*segment_size:min(len(update_uris), (i+1)*segment_size)] \
            for i in range(ceil(len(update_uris)/segment_size))]:

            self.sp.playlist_add_items(playlist_uri, update_uris_segment)

    def reset_playlist(self, playlist_uri):
        track_uris = []
        self.sp.playlist_replace_items(playlist_uri, track_uris)

class FMer(Streamable, Caller):
    def __init__(self, streamer=None):
        super().__init__()
        self.fm = None
        self.add_streamer(streamer)
        self.api_key = get_secret('LASTFM_API_KEY')

    def call_api(self, artist=None, title=None):
        if artist and not title:
            method = 'artist'
        if artist and title:
            method = 'track'
        else:
            method = None

        if method:
            url = (f'{LASTFM_URL}/2.0/?method={method}.getinfo'
                   f'&api_key={self.api_key}&format=json'
                   )
            url += f'&artist={parse.quote(artist)}' if artist else ''
            url += f'&track={parse.quote(title)}' if title else ''
            
            content, jason = self.invoke_api(url, method='get')
            ##response = requests.get(url)
            ##if response.ok:
            ##    content = response.content
            ##    jason = response.json() if len(content) and response.headers.get('Content-Type').startswith('application/json') else None

        else:
            content = None
            jason = None

        return content, jason

    def clean_title(self, title):
        # remove featuring artists and pull remixes
        self.texter = Texter()

        title, mix1 = self.texter.remove_parenthetical(title, ['Remix', 'Mix'], position='end', case_sensitive=True)
        title, mix2 = self.texter.remove_parenthetical(title, ['Remix', 'Mix'], parentheses=[['- ', '$']], position='end', case_sensitive=True)
        title, _ = self.texter.remove_parenthetical(title, ['with ', 'Duet with '], position='start') #<- should with only be for []?
        title, _ = self.texter.remove_parenthetical(title, ['feat'], position='start') # there can be with and feat together
        title, _ = self.texter.remove_parenthetical(title, ['Live '], position='start', parentheses=[['- ', '$']], middle=[' with '])
        title, _ = self.texter.remove_parenthetical(title, ['feat. '], position='start', parentheses=[['', '$']])
        title = self.texter.drop_dash(title)

        mixes = [m for m in filter(None, [mix1, mix2])]
        mix = mixes[0] if len(mixes) else None

        return title, mix
    
    def get_track_info(self, artist, title, max_tags=5):
        self.streamer.print(f'\t...{artist} - {title}')
        _, track = self.call_api(artist, title)
        
        if (not track) or track.get('error'):
            # track not found
            elements = {x: None for x in ['scrobbles', 'listeners', 'top_tags']}

        else:
            # extract elements from track
            elements = {'scrobbles': int(track['track']['playcount']),
                        'listeners': int(track['track']['listeners']),
                        'top_tags': [tag['name'] for tag in track['track']['toptags']['tag'][:max_tags]],
                        }
            
        return elements

    def update_database(self, database):
        self.database = database

        self.update_db_tracks()

    def update_db_tracks(self):
        self.streamer.print('Connecting to LastFM API...')
        self.streamer.print('\t...updating track information')
        tracks_update_db = self.database.get_tracks_update_fm()

        if len(tracks_update_db):
            # get LastFM elements
            # limit how many are updated in one go to keep under rate limites
            segment_size = 50
            for tracks_update_db_segment in [tracks_update_db.loc[i*segment_size:(i+1)*segment_size-1].copy() \
                                            for i in range(ceil(len(tracks_update_db)/segment_size))]:
                df_elements = [self.get_track_info(artist, title) for artist, title in tracks_update_db_segment[['artist', 'title']].values]

                df_to_update = DataFrame(df_elements, index=tracks_update_db_segment.index)
                tracks_update_db_segment.loc[:, df_to_update.columns] = df_to_update
                self.database.store_tracks(tracks_update_db_segment)

class Wikier(Streamable, Caller):
    wiki_page = 'list_of_music_genres_and_styles'

    def __init__(self):
        super().__init__()

    def call_api(self):
        payload = {'action': 'parse',
                   'page': self.wiki_page,
                   'format': 'json',
                   }

        url = f'{WIKI_URL}/w/api.php?' + '&'.join(f'{k}={payload[k]}' for k in payload)

        content, jason = self.invoke_api(url, method='get')

        ##try:
        ##    response = requests.get(url)
        ##    if response.ok:
        ##        r_content = response.content
        ##        r_jason = response.json() if len(r_content) and response.headers.get('Content-Type').startswith('application/json') else None

        ##    else:
        ##        r_jason = None

        ##except TimeoutError:
        ##    r_jason = None

        return jason

    def get_genres(self):
        self.streamer.print('Connecting to Wikimedia API...')
        genres = self.call_api()
        return genres

    def update_database(self, database):#, default='other'):
        self.database = database

        genres_df = database.get_genres()[['name', 'wiki_category']]
        categories_df = genres_df[['wiki_category']].dropna().drop_duplicates().reset_index(drop=True)
    
        genre_categories_df = genres_df.where(
            genres_df['wiki_category'].notnull(),
            genres_df[['name']]\
                .merge(DataFrame(categories_df['wiki_category'].str.split(' and ', expand=True)\
                .melt(ignore_index=False).dropna().drop_duplicates()['value']\
                .apply(lambda x: [genres_df['name'][i] \
                    for i in genres_df[genres_df['name'].str.contains(x, regex=False)].index])\
                .groupby(level=0).sum().to_list()).melt(value_name='name', ignore_index=False)\
                .dropna().reset_index()\
                .merge(categories_df, left_on='index', right_on=categories_df.index)[['name', 'wiki_category']],
                       how='left', on='name')
            ).rename(columns={'wiki_category': 'category'})#.fillna(default)

        return genre_categories_df