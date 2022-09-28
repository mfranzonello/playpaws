from datetime import date
import json

from sqlalchemy import create_engine
from pandas import read_sql, DataFrame, isnull
from pandas.api.types import is_numeric_dtype

from common.secret import get_secret
from display.streaming import Streamable, cache

class Engineer:
    def __init__(self):
        self.db = f'"{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}"'
        self.engine_string = (f'postgresql://{get_secret("BITIO_USERNAME")}'
                              f':{get_secret("BITIO_PASSWORD")}@{get_secret("BITIO_HOST")}'
                              f'/{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}')

    @cache(allow_output_mutation=True, max_entries=10, ttl=10800)
    def connect(self):  
        engine = create_engine(self.engine_string)
        connection = engine.connect()
        return connection

class Database(Streamable):
    tables = {# MusicLeague data
              'Leagues': {'keys': ['league_id'],
                          'values': ['creator_id', 'date', 'league_name', 'extendable']},
              'Players': {'keys': ['player_id'],
                          'values': ['player_name', 'username', 'src', 'uri', 'followers', 'flagged', 'inactive']},
              'Rounds': {'keys': ['league_id', 'round_id'],
                         'values': ['round_name', 'creator_id', 'date', 'playlist_url', 'description', 'capture']},
              'Songs': {'keys': ['league_id', 'song_id'],
                        'values': ['round_id', 'submitter_id', 'track_uri']}, # comment
              'Votes': {'keys': ['league_id', 'player_id', 'song_id'],
                        'values': ['vote']}, # comment

              'Playlists': {'keys': ['league_id', 'theme', 'player_id'],
                            'values': ['uri', 'src', 'round_ids']},

              # Spotify data
              'Tracks': {'keys': ['uri'],
                         'values': ['name', 'title', 'mix', 'artist_uri', 'album_uri', 'explicit', 'popularity',
                                    'duration', 'key', 'mode', 'loudness', 'tempo',
                                    'danceability', 'energy', 'liveness', 'valence', 'speechiness', 'acousticness', 'instrumentalness',
                                    'scrobbles', 'listeners', 'top_tags']},
              'Artists': {'keys': ['uri'],
                          'values': ['name', 'genres', 'popularity', 'followers', 'src']},
              'Albums': {'keys': ['uri'],
                         'values': ['name', 'genres', 'popularity', 'release_date', 'src']},
              'Genres': {'keys': ['name'],
                         'values': ['category']},
              
              # analytics
              'Members': {'keys': ['league_id', 'player_id'],
                          'values': ['x', 'y', 'wins', 'dfc', 'likes_id', 'liked_id']},
              'Pulse': {'keys': ['league_id', 'p1_id', 'p2_id'],
                        'values': ['distance']},
              'Results': {'keys': ['league_id', 'song_id'],
                          'values': ['people', 'votes', 'closed', 'discovery', 'points']},
              'Rankings': {'keys': ['league_id', 'round_id', 'player_id'],
                           'values': ['points', 'score']},
              'Boards': {'keys': ['league_id', 'round_id', 'player_id'],
                         'values': ['place']},
              'Competitions': {'keys': ['league_id', 'competition'],
                               'values': ['start', 'finish']},
              'Analyses': {'keys': ['league_id'],
                           'values': ['date', 'round_ids', 'version']},
              
              # settings
              'Weights': {'keys': ['parameter', 'version'],
                          'values': ['value']},
              }

    god_id = '777'

    def __init__(self, streamer=None):
        super().__init__()
        self.db = f'"{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}"'
        
        self.add_streamer(streamer)
        
        self.keys = {table_name: self.tables[table_name]['keys'] for table_name in self.tables}
        self.values = {table_name: self.tables[table_name]['values'] for table_name in self.tables}
        self.columns = {table_name: self.tables[table_name]['keys'] + self.tables[table_name]['values'] for table_name in self.tables}
 
        self.streamer.print(f'Connecting to database {self.db}...')

        engineer = Engineer()
        self.connection = engineer.connect()
        self.streamer.print(f'\t...success!')
          
    def table_name(self, table_name:str) -> str:
        full_table_name = f'{self.db}."{table_name.lower()}"'

        return full_table_name

    def read_sql(self, sql, **kwargs):
        return read_sql(sql, self.connection, **kwargs)

    def get_table(self, table_name, columns=None, league_id=None, order_by=None, drop_league=False):
        # get values from database

        if order_by and order_by.get('other'):
            other_table = order_by['other']
            m_ = 'm.'
            f_ = 'f.'
            mf_join = ' AND '.join(f'(m.{m} = f.{f})' for m, f in zip(order_by.get('on', order_by.get('left_on')),
                                                                      order_by.get('on', order_by.get('right_on'))))

            joins = f' AS m JOIN {self.table_name(other_table)} AS f ON {mf_join} '

        else:
            m_, f_, joins = ['']*3

        # check if column specific
        if columns is None:
            # return full table
            cols = f'{m_}*'
        else:
            # return only specific rows, like finding keys
            cols = ', '.join(f'{m_}{col}' for col in columns)

        # check if league specific
        ##if league is not None:
        ##    # return only league values based on title
        ##    wheres = f' WHERE {m_}league = {self.needs_quotes(league)}'
        if league_id is not None:
            # return only league values based on ID
            wheres = f' WHERE {m_}league_id = {self.needs_quotes(league_id)}'
        else:
            # return all values
            wheres = ''

        if order_by:
            orders = f' ORDER BY {f_}{order_by["column"]} {order_by["sort"]}'
        else:
            orders = ''

        # write and execute SQL
        sql = f'SELECT {cols} FROM {self.table_name(table_name)}{joins}{wheres}{orders};'
        table = self.read_sql(sql, coerce_float=True)        

        if drop_league:
            table = table.drop(columns='league_id')

        return table

    def quotable(self, item):
        # add SQL appropriate quotes to string variables
        is_quote = not (self.numberable(item) or self.nullable(item))
        return is_quote

    def numberable(self, item):
        # do not add quotes or cast information to numbers
        is_number = (not self.nullable(item)) and is_numeric_dtype(type(item))
        return is_number

    def datable(self, item):
        # add cast information to date values
        is_date = isinstance(item, date)
        return is_date

    def nullable(self, item):
        # change to None for None, nan, etc
        is_null = (not self.jsonable(item)) and (isnull(item) or (item == 'nan'))
        return is_null

    def jsonable(self, item):
        # add cast information to lists and dicts as JSON
        is_json = isinstance(item, (list, dict, set))
        return is_json

    def needs_quotes(self, item) -> str:
        # put quotes around strings to account for special characters
        if self.quotable(item):
            if self.datable(item):
                quoted = self.replace_for_sql(str(item)) + '::date'
            elif self.jsonable(item):
                quoted = self.replace_for_sql(json.dumps(item)) + '::jsonb'
            else:
                quoted = self.replace_for_sql(str(item))
        elif self.nullable(item):
            quoted = 'NULL'
        else:
            quoted = str(item)

        return quoted

    def replace_for_sql(self, text):
        char = "'"
        pct = '%'
        for_sql = char + text.replace(char, char*2).replace(pct, pct*2).replace(pct*4, pct*2) + char
        return for_sql

    def update_rows(self, table_name, df, keys):
        # write SQL for existing rows
        if df.empty or (set(df.columns) == set(keys)):
            sql = ''

        else:
            value_columns = df.drop(columns=keys).columns
            all_columns = keys + value_columns.to_list()
            df_upsert = df.reindex(columns=all_columns) # -> may not need, see insert_rows below

            sets = ', '.join(f'{col} = c.{col}' for col in value_columns)
            values = ', '.join('(' + ', '.join(self.needs_quotes(v) for v in df_upsert.loc[i].values) + ')' for i in df_upsert.index)
            cols = ', '.join(f'{col}' for col in all_columns)
            wheres = ' AND '.join(f'(c.{key} = t.{key})' for key in keys)
                
            sql = f'UPDATE {self.table_name(table_name)} AS t SET {sets} FROM (VALUES {values}) AS c({cols}) WHERE {wheres};'

        return sql

    def insert_rows(self, table_name, df):
        # write SQL for new rows
        if df.empty:
            sql = ''
        else:
            columns = '(' + ', '.join(df.columns) + ')'
            values = ', '.join('(' + ', '.join(self.needs_quotes(value) for value in df.loc[i]) + ')' for i in df.index)
            sql = f'INSERT INTO {self.table_name(table_name)} {columns} VALUES {values};'

        return sql

    def find_existing(self, df_store, df_existing, keys):
        # split dataframe between old and new rows
        df_merge = df_store.merge(df_existing, on=keys, how='left', indicator=True)

        df_updates = df_store.iloc[df_merge[df_merge['_merge'] == 'both'].index]
        df_inserts = df_store.iloc[df_merge[df_merge['_merge'] == 'left_only'].index]
       
        return df_updates, df_inserts

    def get_keys(self, table_name):
        keys = self.keys[table_name]

        return keys

    def get_values(self, table_name, match=None):
        values = self.values[table_name]

        if len(match):
            values = [v for v in values if v in match]

        return values

    def execute_sql(self, sql):
        if len(sql):
            self.connection.execute(sql)

    def upsert_table(self, table_name, df):
        # update existing rows and insert new rows
        if len(df):
            # there are values to store
            keys = self.get_keys(table_name)

            # only store columns that have values, so as to not overwrite with NA
            # retain key columns that have NA values, such as Votes table
            value_columns = self.get_values(table_name, match=df.columns)
            df_store = df.drop(columns=df[value_columns].columns[df[value_columns].isna().all()])
            
            # get current league if not upserting Leagues or table that doesn't have league as a key
            if ('league_id' in self.get_keys(table_name)) and (len(df_store['league_id'].unique()) == 1): 
                league_id = df_store['league_id'].iloc[0]
            else:
                league_id = None
            
            # get existing ids in database
            df_existing = self.get_table(table_name, columns=keys, league_id=league_id)

            # split dataframe into existing updates and new inserts
            df_updates, df_inserts = self.find_existing(df_store, df_existing, keys)

            # write SQL for updates and inserts
            sql_updates = self.update_rows(table_name, df_updates, keys)
            sql_inserts = self.insert_rows(table_name, df_inserts)         

            # execute SQL
            self.execute_sql(sql_updates)
            self.execute_sql(sql_inserts)

    def get_player_match(self, league_id=None, partial_name=None, url=None):
        # find closest name
        if url or (league_id and partial_name):
            # something to search for
            if url:
                # search by URL
                wheres = f'url = {self.needs_quotes(url)}'
            else:
                # search by name and league
                like_name = f'{partial_name}%%'
                wheres = f'(league_id = {self.needs_quotes(league_id)}) AND (player_id LIKE {self.needs_quotes(like_name)})'

            sql = f'SELECT player_id FROM {self.table_name("Members")} WHERE {wheres}'

            names_df = self.read_sql(sql)
            if len(names_df):
                # potential matches
                if partial_name in names_df['player_id'].values:
                    # the name is an exact match
                    matched_name = partial_name
                else:
                    # return the first name match
                    matched_name = names_df['player_id'].iloc[0]
            else:
                # no name found
                matched_name = None
        else:
            matched_name = None

        return matched_name ### also return player_id

    def get_song_ids(self, league_id:str, songs:DataFrame) -> DataFrame:
        # first check for which songs already exists
        ids_df = self.get_table('Songs', league_id=league_id, drop_league=True).rename(columns={'song_id': 'new_song_id'})
        merge_cols = ['track_uri', 'round_id'] # 'track_url', 'round'
        id_cols = ['song_id', 'new_song_id']
        songs_df = songs.merge(ids_df, on=merge_cols, how='left')[merge_cols + id_cols]
        
        # then fill in songs that are new
        n_retrieve = songs_df['new_song_id'].isna().sum()
        if n_retrieve:
            new_ids = self.get_new_song_ids(league_id, n_retrieve)
            songs_df.loc[songs_df['new_song_id'].isna(), 'new_song_id'] = new_ids
            
        song_ids = songs_df[id_cols]

        return song_ids

    def get_new_song_ids(self, league_id, n_retrieve):
        # get next available song_ids
        existing_song_ids = self.get_table('Songs', columns=['song_id'], league_id=league_id)['song_id'] 
        max_song_id = 0 if existing_song_ids.empty else existing_song_ids.max()
        next_song_ids = [song_id for song_id in range(1, max_song_id + n_retrieve + 1) \
            if song_id not in existing_song_ids.values][0:n_retrieve]
        return next_song_ids

    def store_columns(self, table_name):
        columns = self.columns[table_name]
        return columns

    def get_leagues(self):
        # get league IDs
        leagues_df = self.get_table('Leagues', order_by={'column': 'date', 'sort': 'ASC'})
        return leagues_df

    def store_leagues(self, leagues_df):
        # store league names
        df = leagues_df.reindex(columns=self.store_columns('Leagues'))
        self.upsert_table('Leagues', df)

    def get_league_creator(self, league_id):
        # get name of league creator
        sql = f'SELECT creator_id FROM {self.table_name("Leagues")} WHERE league_id = {self.needs_quotes(league_id)}' 
        creators_df = self.read_sql(sql)
        if len(creators_df):
            creator_id = creators_df['creator_id'].iloc[0]
        else:
            creator_id = None
        return creator_id 

    def get_player_leagues(self, player_id):
        sql = (f'SELECT league_id FROM {self.table_name("Members")} '
               f'WHERE player_id = {self.needs_quotes(player_id)};'
               )
               
        league_ids = self.read_sql(sql)['league_id'].values

        return league_ids

    def check_data(self, league_id, round_id=None):
        # see if there is any data for the league or round
        wheres = f'league_id = {self.needs_quotes(league_id)}'
        if round_id is not None:
            wheres = f'({wheres}) AND (round_id = {self.needs_quotes(round_id)})'
            count = 'round_id'
            table_name = 'Rounds'
        else:
            count = 'league_id'
            table_name = 'Leagues'

        sql = f'SELECT COUNT({count}) FROM {self.table_name(table_name)} WHERE {wheres}'

        count_df = self.read_sql(sql)
        check = count_df['count'].gt(0).all()

        return check

    def get_rounds(self, league_id):
        rounds_df = self.get_table('Rounds', league_id=league_id, order_by={'column': 'date', 'sort': 'ASC'}, drop_league=True)
        return rounds_df


    def get_uncreated_rounds(self, league_id):
        sql = (f'SELECT round_id, description, creator_id FROM {self.table_name("Rounds")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) AND (creator_id IS NULL);') 

        rounds_df = self.read_sql(sql)

        return rounds_df 

    def store_rounds(self, rounds_df, league_id):
        df = rounds_df.reindex(columns=self.store_columns('Rounds'))
        df['league_id'] = league_id

        self.upsert_table('Rounds', df)

    def store_songs(self, songs_df, league_id):
        df = songs_df.reindex(columns=self.store_columns('Songs'))
        df['league_id'] = league_id
        self.upsert_table('Songs', df)

    def get_songs(self, league_id):
        songs_df = self.get_table('Songs', league_id=league_id, drop_league=True)
        return songs_df

    ##def get_song_urls(self):
    ##    # get just the URLS for all songs
    ##    songs_df = self.get_table('Songs', columns=['track_url'])
    ##    return songs_df

    def store_votes(self, votes_df, league_id):
        df = votes_df.reindex(columns=self.store_columns('Votes'))
        df['league_id'] = league_id
        self.upsert_table('Votes', df)

    def get_votes(self, league_id):
        votes_df = self.get_table('Votes', league_id=league_id, drop_league=True)
        return votes_df
        
    def store_members(self, members_df, league_id):
        df = members_df.reindex(columns=self.store_columns('Members'))
        df['league_id'] = league_id
        self.upsert_table('Members', df)

    def get_results(self, league_id):
        songs_df = self.get_table('Results', league_id=league_id, drop_league=True)
        return songs_df

    def store_results(self, results_df, league_id):
        df = results_df.reindex(columns=self.store_columns('Results'))
        df['league_id'] = league_id
        self.upsert_table('Results', df)

    def get_members(self, league_id):
        members_df = self.get_table('Members', league_id=league_id, drop_league=True)
        return members_df

    def store_pulse(self, pulse_df, league_id):
        df = pulse_df.reindex(columns=self.store_columns('Pulse'))
        df['league_id'] = league_id
        self.upsert_table('Pulse', df)

    def get_pulse(self, league_id):
        pulse_df = self.get_table('Pulse', league_id=league_id, drop_league=True)
        return pulse_df

    def store_players(self, players_df, league_id=None):
        df = players_df.reindex(columns=self.store_columns('Players'))
        self.upsert_table('Players', df)

        if league_id:
            self.store_members(df, league_id)

    def get_players(self):
        players_df = self.get_table('Players')
        return players_df

    def get_player_names(self):
        player_names = self.get_players()[['player_id', 'player_name']].sort_values(by='player_name', key=lambda s: s.str.lower())
        
        return player_names

    def get_name(self, id, table):
        sql = f'SELECT {table}_name FROM {self.table_name(table.title() + "s")} WHERE {table}_id = {self.needs_quotes(id)}'
        name = self.read_sql(sql)[f'{table}_name'].squeeze()

        return name

    def get_player_name(self, player_id):  
        return self.get_name(player_id, 'player')

    def get_round_name(self, round_id):  
        return self.get_name(round_id, 'round')

    def get_league_name(self, league_id):  
        return self.get_name(league_id, 'league')

    def get_player_ids(self, league_id=None):
        if league_id:
            members_df = self.get_members(league_id)
            player_ids = members_df['player_id'].to_list()
        else:
            sql = (f'SELECT player_id FROM {self.table_name("Players")} '
                   f'WHERE player_ID != {self.needs_quotes(self.god_id)} '
                   f'ORDER BY player_name;'
                   )

            player_ids = self.read_sql(sql)['player_id'].to_list()

            ##player_ids = self.get_table('Players', order_by='player_name')['player_id'].to_list()

        return player_ids

    def get_god_id(self):
        return self.god_id

    def get_inactive_players(self):
        sql = (f'SELECT player_id FROM {self.table_name("Players")} '
               f'WHERE inactive = {self.needs_quotes("True")};'
               )

        player_ids = self.read_sql(sql)['player_id'].to_list()

        return player_ids

    def get_extendable_leagues(self):
        sql = (f'SELECT league_id FROM {self.table_name("Leagues")} '
               f'WHERE extendable = {self.needs_quotes("True")};'
               )

        league_ids = self.read_sql(sql)['league_id'].to_list()

        return league_ids

    def get_weights(self, version):
        sql = (f'SELECT DISTINCT ON(parameter) version, parameter, value '
               f'FROM {self.table_name("Weights")} WHERE version >= FLOOR({version}::real) '
               f'ORDER BY parameter, version DESC;'
               )

        weights = self.read_sql(sql, index_col='parameter')['value']
        weights = weights.apply(self.clean_up_weight)
        return weights

    def clean_up_weight(self, value):
        if value == 'True':
            clean_value = True
        elif value == 'False':
            clean_value = False
        else:
            try:
                clean_value = float(value)
            except:
                clean_value = value
        return clean_value


    # Spotify functions
    def store_spotify(self, df, table_name):
        df = df.reindex(columns=self.store_columns(table_name))
        self.upsert_table(table_name, df)

    def store_tracks(self, tracks_df):
        self.store_spotify(tracks_df, 'Tracks')
        
    def store_artists(self, artists_df):
        self.store_spotify(artists_df, 'Artists')

    def store_albums(self, albums_df):
        self.store_spotify(albums_df, 'Albums')

    def store_genres(self, genres_df):
        self.store_spotify(genres_df, 'Genres')

    def get_spotify(self, table_name):
        df = self.get_table(table_name)
        return df

    def get_tracks(self):
        df = self.get_spotify('Tracks')
        return df

    def get_artists(self):
        df = self.get_spotify('Artists')
        return df

    def get_albums(self):
        df = self.get_spotify('Albums')
        return df

    def get_genres(self):
        df = self.get_spotify('Genres')
        return df

    def get_round_playlist(self, league_id, round_id):
        sql = (f'SELECT playlist_url AS url FROM {self.table_name("Rounds")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) AND (round_id = {self.needs_quotes(round_id)}) '
               f'AND (playlist_url IS NOT NULL);')

        playlist_df = self.read_sql(sql)
        if len(playlist_df):
            playlist_url = playlist_df['url'].iloc[0]
        else:
            playlist_url = None

        return playlist_url

    def get_playlists(self, league_id=None):
        playlists_df = self.get_table('Playlists', league_id=league_id)
        return playlists_df

    def get_track_count(self, league_id):
        sql = (f'SELECT COUNT(DISTINCT track_uri) FROM {self.table_name("Songs")} ' #track_url
               f'WHERE league_id = {self.needs_quotes(league_id)};'
               )

        count = self.read_sql(sql)['count'].iloc[0]

        return count

    def get_track_durations(self, league_id):
        sql = (f'SELECT SUM(t.duration) AS duration FROM '
               f'(SELECT DISTINCT track_uri FROM {self.table_name("Songs")} ' #track_url
               f'WHERE league_id = {self.needs_quotes(league_id)}) as s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri;' #track_url
               )

        duration = self.read_sql(sql)['duration'].iloc[0]

        return duration

    def get_theme_playlists(self, theme):
        # get playlists or track URIs to pull songs from
        if theme == 'complete':
            # all songs
            sql = (f'SELECT league_id, round_id, playlist_url AS url FROM {self.table_name("Rounds")} '
                   f'WHERE playlist_url IS NOT NULL '
                   f'ORDER BY date;'
                   )

            wheres = f'theme = {self.needs_quotes(theme)}'
            
        elif theme == 'best':
            # based on performance
            sql = (f'SELECT s.league_id, s.round_id, t.uri, r.points, d.date FROM {self.table_name("Results")} as r ' 
                   f'LEFT JOIN {self.table_name("Songs")} as s ON (r.league_id = s.league_id) AND (r.song_id = s.song_id) '
                   f'LEFT JOIN {self.table_name("Tracks")} as t ON s.track_uri = t.uri '
                   f'LEFT JOIN {self.table_name("Rounds")} as d ON (s.league_id = d.league_id) AND (s.round_id = d.round_id) '
                   f';'
                   )

            wheres = f'theme = {self.needs_quotes(theme)}'

        elif theme == 'favorite':
            # player favorite
            sql = (f'SELECT * FROM (SELECT s.league_id, s.round_id, t.uri, v.player_id, v.vote, d.date '
                   f'FROM {self.table_name("Votes")} as v '
                   f'LEFT JOIN {self.table_name("Songs")} as s ON (v.league_id = s.league_id) AND (v.song_id = s.song_id) '
                   f'LEFT JOIN {self.table_name("Tracks")} as t ON s.track_uri = t.uri ' #track_url, url
                   f'LEFT JOIN {self.table_name("Rounds")} AS d ON (s.league_id = d.league_id) AND (s.round_id = d.round_id) '
                   f'UNION SELECT s.league_id, s.round_id, t.uri, s.submitter_id, -1 as vote,  d.date ' 
                   f'FROM {self.table_name("Songs")} as s '
                   f'LEFT JOIN {self.table_name("Tracks")} as t ON s.track_uri = t.uri ' #track_url, url
                   f'LEFT JOIN {self.table_name("Rounds")} AS d ON (s.league_id = d.league_id) AND (s.round_id = d.round_id)) AS q '
                   f'WHERE q.player_id IS NOT NULL;'
                   )

            wheres = f'theme LIKE {self.needs_quotes(theme+"%%")}'

        rounds_df = self.read_sql(sql)

        # get comprehensive playlists
        selects = ', player_id' if theme == 'favorite' else ''

        sql = (f'SELECT league_id, uri, src, round_ids{selects} FROM {self.table_name("Playlists")} '
               f'WHERE {wheres};'
               )

        playlists_df = self.read_sql(sql)
     
        return rounds_df, playlists_df

    def store_playlists(self, playlists_df, theme=None):
        df = playlists_df.reindex(columns=self.store_columns('Playlists'))
        if theme:
            df['theme'] = theme
        if theme in ['complete', 'best']:
            df['player_id'] = self.god_id

        self.upsert_table('Playlists', df)

    def flag_player_image(self, player_id):
        sql = (f'UPDATE {self.table_name("Players")} '
               f'SET flagged = {self.needs_quotes(date.today())} '
               f'WHERE player_id = {self.needs_quotes(player_id)};'
               )

        self.execute_sql(sql)

    def get_players_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['src', 'uri', 'followers'])
        sql = (f'SELECT player_id, username FROM {self.table_name("Players")} '
               f'WHERE ({wheres} OR (flagged <= {self.needs_quotes(date.today())})) '
               f'AND (username IS NOT NULL);')

        players_df = self.read_sql(sql)

        return players_df

    def get_tracks_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['name', 'artist_uri', 'album_uri', # 'uri'
                                                        'explicit', 'popularity', 'duration',
                                                        'key', 'mode', 'loudness', 'tempo',
                                                        'danceability', 'energy', 'liveness', 'valence',
                                                        'speechiness', 'acousticness', 'instrumentalness'])
        sql = (f'SELECT DISTINCT track_uri AS url FROM {self.table_name("Songs")} ' # track_url
               f'WHERE track_uri NOT IN ' # track_url
               f'(SELECT uri FROM {self.table_name("Tracks")}) '
               f'UNION SELECT uri FROM {self.table_name("Tracks")} WHERE {wheres};'
               )

        tracks_df = self.read_sql(sql)

        return tracks_df
    
    def get_artists_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['src', 'name', 'popularity',
                                                        'genres', 'followers'])
        sql = (f'SELECT t.a_uri as uri FROM '
               f'(SELECT DISTINCT jsonb_array_elements(artist_uri)->>0 AS a_uri '
               f'FROM {self.table_name("Tracks")}) AS t '
               f'WHERE t.a_uri NOT IN (SELECT uri FROM {self.table_name("Artists")}) '
               f'UNION SELECT uri FROM {self.table_name("Artists")} WHERE {wheres};'
               )

        artists_df = self.read_sql(sql)

        return artists_df

    def get_albums_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['src', 'name', 'popularity',
                                                        'release_date', 'genres'])
        sql = (f'SELECT DISTINCT album_uri AS uri FROM {self.table_name("Tracks")} '
               f'WHERE (album_uri NOT IN (SELECT uri FROM {self.table_name("Albums")})) '
               f'UNION SELECT uri FROM {self.table_name("Albums")} WHERE {wheres};'
               )

        albums_df = self.read_sql(sql)

        return albums_df

    def get_genres_update_sp(self):
        sql = (f'SELECT u.genre FROM (SELECT DISTINCT '
               f'jsonb_array_elements(genres)->>0 AS genre '
               f'FROM {self.table_name("Artists")} '
               f'UNION SELECT jsonb_array_elements(genres)->>0 AS genre '
               f'FROM {self.table_name("Albums")}) AS u '
               f'WHERE u.genre NOT IN (SELECT name FROM {self.table_name("Genres")});'
               )

        genres_df = self.read_sql(sql)

        return genres_df


    # LastFM functions
    def get_tracks_update_fm(self):
        wheres = ' OR '.join(f'(t.{v} IS NULL)' for v in ['title', 'scrobbles', 'listeners',
                                                          'top_tags'])
        sql = (f'SELECT t.uri, t.name AS unclean, t.title, t.mix, a.name AS artist, ' #t.url
               f't.scrobbles, t.listeners, t.top_tags '
               f'FROM {self.table_name("Tracks")} as t '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON (t.artist_uri->>0) = a.uri WHERE {wheres};'
               )

        tracks_df = self.read_sql(sql)

        return tracks_df

    # analytics functions
    def store_analysis(self, league_id, version, round_ids=None, optimized=None): 
        today = date.today()
        d = {'league_id': [league_id],
             'date': [today],
             'version': [version]}

        d['round_ids'] = [round_ids]

        if optimized is not None:
            d['optimized'] = [optimized]

        analyses_df = DataFrame(d)
        
        self.upsert_table('Analyses', analyses_df)

    def get_analyses(self):
        analyses_df = self.get_table('Analyses', order_by={'other': 'Leagues',
                                                           'on': ['league_id'],
                                                           'column': 'date',
                                                           'sort': 'ASC'})
        return analyses_df
    
    def get_analyzed(self, league_id, round_ids, version):
        sql = (f'SELECT COUNT(*) FROM {self.table_name("Analyses")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (round_ids = {self.needs_quotes(round_ids)}) '
               f'AND (version = {version}::real);'
               )

        analyzed = self.read_sql(sql)['count'].iloc[0] > 0

        return analyzed

    def get_optimized(self, league_id):
        sql = (f'SELECT COUNT(*) FROM {self.table_name("Analyses")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (optimized = TRUE);'
               )

        optimized = self.read_sql(sql)['count'].iloc[0] > 0

        return optimized

    def store_rankings(self, rankings_df, league_id):
        df = rankings_df.reset_index().reindex(columns=self.store_columns('Rankings'))
        df['league_id'] = league_id
        self.upsert_table('Rankings', df)

    def get_rankings(self, league_id):
        # get rankings sorted by round date
        reindexer = self.get_round_order(league_id)
        rankings_df = self.get_table('Rankings', league_id=league_id, drop_league=True)\
            .set_index(['round_id', 'player_id']).sort_values(['round_id', 'points'], ascending=[True, False]).reindex(reindexer, level=0)

        return rankings_df

    def get_round_order(self, league_id):
        reindexer = self.get_table('Rounds', columns=['round_id'], league_id=league_id,
                                   order_by={'column': 'date', 'sort': 'ASC'})['round_id']

        return reindexer

    def store_boards(self, boards_df, league_id):
        df = boards_df.reset_index().melt(id_vars='player_id',
                                          value_vars=boards_df.columns,
                                          var_name='round_id',
                                          value_name='place').dropna(subset=['place']).reindex(columns=self.store_columns('Boards'))
        df['league_id'] = league_id

        self.upsert_table('Boards', df)

    def get_boards(self, league_id):
        reindexer = self.get_round_order(league_id)
        boards_df = self.get_table('Boards', league_id=league_id, order_by={'other': 'Rounds', 
                                                                            'on': ['league_id', 'round_id'],
                                                                            'column': 'date',
                                                                            'sort': 'ASC'},
                                   drop_league=True)\
            .pivot(index='player_id', columns='round_id', values='place')
        
        reindexer = [r for r in reindexer if r in boards_df.columns]
        boards_df = boards_df.reindex(columns=reindexer)
        
        return boards_df

    # things that don't require analysis
    def get_dirtiness(self, league_id, vote=False):
        if vote:
            gb = 'player_id'
            sql = (f'SELECT v.player_id, '
                   f'AVG(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS dirtiness '
                   f'FROM {self.table_name("Votes")} AS v '
                   f'LEFT JOIN {self.table_name("Songs")} AS s '
                   f'ON (v.song_id = s.song_id) AND (v.league_id = s.league_id) '
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri '
                   f'WHERE s.league_id = {self.needs_quotes(league_id)} '
                   f'GROUP BY v.player_id;'
                   )
        else:
            gb = 'submitter_id'
            sql = (f'SELECT s.submitter_id, ' 
                   f'count(CASE WHEN t.explicit THEN 1 END) / '
                   f'count(CASE WHEN NOT t.explicit THEN 1 END)::real AS dirtiness '
                   f'FROM {self.table_name("Songs")} AS s '
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri '
                   f'WHERE s.league_id = {self.needs_quotes(league_id)} '
                   f'GROUP BY s.submitter_id;'
                   )

        dirtiness = self.read_sql(sql).set_index(gb)['dirtiness']

        return dirtiness

    def get_audio_features(self, league_id, json=False, methods=None):
        values = ['duration', 'danceability', 'energy', 'key', 'loudness', 'mode',
                  'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']

        if not methods:
            methods = ['MIN', 'AVG', 'MAX']

        if json:
            jsons = ', '.join('json_build_object(' +
                              ', '.join(f'{self.needs_quotes(method)}, {method}(t.{k})' for method in methods) +
                              f') AS {k}' for k in values)

        else:
            jsons = ', '.join(f'{method}(t.{k}) AS {method}_{k}' for method in methods for k in values)

            sql = (f'SELECT s.round_id, {jsons} '
                   f'FROM {self.table_name("Songs")} AS s '
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url
                   f'LEFT JOIN {self.table_name("Rounds")} AS r ON s.round_id = r.round_id '
                   f'WHERE s.league_id = {self.needs_quotes(league_id)} '
                   f'GROUP BY s.round_id, r.date ORDER BY r.date;'
                   )

        features_df = self.read_sql(sql)

        return features_df

    def get_discoveries(self, league_id, base=1000):
        sql = (f'SELECT s.round_id, s.song_id, '
               f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url, url
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY s.round_id, s.song_id;'
               )

        discoveries_df = self.read_sql(sql)

        return discoveries_df

    def get_discovery_scores(self, league_id, base=1000):
        sql = (f'SELECT s.submitter_id, '
               f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery, '
               f'AVG(t.popularity::real/100) AS popularity '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY s.submitter_id;'
               )

        discoveries_df = self.read_sql(sql).set_index('submitter_id')

        return discoveries_df

    def get_genres_and_tags(self, league_id, player_id=None):
        if player_id:
            wheres = f' AND s.submitter_id = {self.needs_quotes(player_id)}'
        else:
            wheres = ''

        sql = (f'SELECT a.genres, t.top_tags AS tags '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_uri = t.uri ' #track_url
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league_id = {self.needs_quotes(league_id)}{wheres};'
               )

        genres_df = self.read_sql(sql)
        
        if player_id:
            genres_df = set(genres_df.sum().sum())

        return genres_df

    def get_exclusive_genres(self, league_id):
        sql = (f'SELECT q.tag FROM '
               f'(SELECT jsonb_array_elements(a.genres) as tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_uri = t.uri ' #track_url
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'UNION '
               f'SELECT jsonb_array_elements(t.top_tags) AS tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_uri = t.uri ' #track_url
               f'WHERE s.league_id = {self.needs_quotes(league_id)}) AS q '
               f'WHERE q.tag NOT IN '
               f'(SELECT jsonb_array_elements(a.genres) as tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_uri = t.uri ' #track_url
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league_id != {self.needs_quotes(league_id)} '
               f'UNION '
               f'SELECT jsonb_array_elements(t.top_tags) AS tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_uri = t.uri ' #track_url
               f'WHERE s.league_id != {self.needs_quotes(league_id)});'
               )

        exclusives = self.read_sql(sql)['tag']

        return exclusives

    def get_song_results(self, league_id):
        sql = (f'SELECT s.round_id, s.song_id, s.submitter_id, '
               f't.title, ttt.artist, b.release_date, b.src, r.closed, r.points '
               f'FROM {self.table_name("Results")} AS r '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON (s.league_id = r.league_id) AND (s.song_id = r.song_id) '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_uri = t.uri ' 
               f'LEFT JOIN {self.table_name("Albums")} AS b '
               f'ON t.album_uri = b.uri '
               f'LEFT JOIN '
               f'(SELECT tt.uri, json_agg(a.name) AS artist FROM ' 
               f'(SELECT jsonb_array_elements(artist_uri) AS a_uri, uri ' 
               f'FROM {self.table_name("Tracks")}) AS tt '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON tt.a_uri ? a.uri '
               f'GROUP BY tt.uri) AS ttt ON t.uri = ttt.uri ' 
               f'LEFT JOIN {self.table_name("Leagues")} AS l '
               f'ON s.league_id = l.league_id '
               f'LEFT JOIN {self.table_name("Rounds")} AS d '
               f'ON (s.league_id = d.league_id) AND (s.round_id = d.round_id) '
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'ORDER BY l.date ASC, d.date ASC, r.points DESC;'
               )

        results_df = self.read_sql(sql).drop_duplicates(subset='song_id')

        return results_df

    def get_round_descriptions(self, league_id):
        descriptions_df = self.get_table('Rounds', columns=['round_id', 'description'],
                                         league_id=league_id, order_by={'column': 'date',
                                                                        'sort': 'ASC'})

        return descriptions_df

    def get_creators_and_winners(self, league_id):
        sql = (f'SELECT r.round_id, r.creator_id, jsonb_agg(b.player_id) AS winner '
               f'FROM {self.table_name("Rounds")} as r '
               f'LEFT JOIN {self.table_name("Boards")} as b '
               f'ON (r.league_id = b.league_id) AND (r.round_id = b.round_id) '
               f'WHERE (r.league_id = {self.needs_quotes(league_id)}) '
               f'AND ((b.place < 2) OR (b.place IS NULL)) ' ## can this be MIN without GROUP BY?
               f'GROUP BY r.round_id, r.creator_id, r.date '
               f'ORDER BY r.date;'
               )
        
        creators_winners_df = self.read_sql(sql)

        return creators_winners_df

    def get_all_artists(self, league_id):
        sql = (f'SELECT s.league_id, s.song_id, json_agg(DISTINCT a.name) as arist '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")}AS t ON s.track_uri = t.uri ' #track_url
               f'LEFT JOIN {self.table_name("Artists")} AS a ON t.artist_uri ? a.uri '
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY s.song_id, s.league;'
               )

        all_artists_df = self.read_sql(sql)

        return all_artists_df

    def get_all_info(self):
        sql = (f'SELECT q.league_id, x.song_id, q.round_id, x.artist, q.title, q.submitter_id FROM '
               f'(SELECT s.league_id, s.song_id, json_agg(DISTINCT a.name) as artist '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url
               f'LEFT JOIN {self.table_name("Artists")} AS a ON t.artist_uri ? a.uri '
               f'GROUP BY s.song_id, s.league_id) x '
               f'LEFT JOIN {self.table_name("Songs")} AS q '
               f'ON (x.song_id = q.song_id) AND (x.league_id = q.league_id);'
               )

        all_info_df = self.read_sql(sql)

        return all_info_df

    def get_player_pulse(self, league_id, player_id):
        sql = (f'SELECT p.player_id, p.likes_id, p.liked_id, j.closest_id '
               f'FROM (SELECT player_id, likes_id, liked_id  '
               f'FROM {self.table_name("Members")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (player_id = {self.needs_quotes(player_id)})) as p '

               f'CROSS JOIN '

               f'(SELECT p2_id AS closest_id FROM {self.table_name("Pulse")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (p1_id = {self.needs_quotes(player_id)}) '
               f'AND (distance IN (SELECT MIN(distance) '
               f'FROM {self.table_name("Pulse")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (p1_id = {self.needs_quotes(player_id)}) '
               f'GROUP BY league_id, p1_id)) LIMIT 1) AS j '

               ##f'(SELECT q.player AS closest FROM ( '
               ##f'SELECT player, abs(dfc - '
               ##f'(SELECT dfc FROM {self.table_name("Members")} '
               ##f'WHERE (league = {self.needs_quotes(league_title)}) '
               ##f'AND (player = {self.needs_quotes(player_name)}))) AS distance '
               ##f'FROM {self.table_name("Members")} '
               ##f'WHERE (league = {self.needs_quotes(league_title)}) '
               ##f'AND (player != {self.needs_quotes(player_name)}) '
               ##f'ORDER BY distance LIMIT 1) AS q) AS j'
               
               f';'
               )

        player_pulse_df = self.read_sql(sql).squeeze(0)

        return player_pulse_df

    def get_player_wins(self, league_id, player_id):
        sql = (f'SELECT round_id FROM {self.table_name("Boards")} '
               f'WHERE (player_id = {self.needs_quotes(player_id)}) '
               f'AND (league_id = {self.needs_quotes(league_id)}) AND (place = 1);'
               )

        player_wins_df = self.read_sql(sql)

        return player_wins_df

    def get_competition_wins(self, league_id, player_id):
        competition_titles = self.get_competitions(league_id)['competition'].unique()
        competition_wins = [competition for competition in competition_titles \
            if self.get_badge(league_id, player_id, competition_title=competition) == 1]

        if not len(competition_wins):
            competition_wins = None

        return competition_wins

    def get_awards(self, league_id, player_id, base=1000):
        ## Note that Discoverer, Dirtiest, etc should be based on MAX/MIN and not LIMIT 1
        sql = (f'SELECT (p.popular = 1) AS popular, (q.discoverer = 1) AS discoverer, '
               f'(r.dirtiest = 1) as dirtiest, (z.generous > 0.5) AS generous, (n.clean = 0) AS clean, '
               f'j.win_rate, k.play_rate '
               
               f'FROM '
               f'(SELECT v.popular FROM (SELECT s.submitter_id, RANK() OVER '
               f'(ORDER BY AVG(t.popularity::real/100) DESC) AS popular '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url
               f'WHERE s.league_id = {self.needs_quotes(league_id)} ' 
               f'GROUP BY s.submitter_id) AS v '
               f'WHERE v.submitter_id = {self.needs_quotes(player_id)}) as p '

               f'CROSS JOIN'
               f'(SELECT u.discoverer FROM (SELECT s.submitter_id, RANK() OVER '
               f'(ORDER BY AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) DESC) AS discoverer '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url
               f'WHERE s.league_id = {self.needs_quotes(league_id)} ' 
               f'GROUP BY s.submitter_id) AS u '
               f'WHERE u.submitter_id = {self.needs_quotes(player_id)}) as q '

               f'CROSS JOIN '
               f'(SELECT w.dirtiest FROM (SELECT s.submitter_id, RANK() OVER '
               f'(ORDER BY AVG(CASE WHEN t.explicit THEN 1 ELSE 0 END) DESC) AS dirtiest '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY s.submitter_id) AS w '
               f'WHERE w.submitter_id = {self.needs_quotes(player_id)}) AS r '

               f'CROSS JOIN '
               f'(SELECT w.clean FROM (SELECT s.submitter_id, '
               f'SUM(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS clean '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_uri = t.uri ' #track_url
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY s.submitter_id) AS w '
               f'WHERE w.submitter_id = {self.needs_quotes(player_id)}) AS n '

               f'CROSS JOIN '
               f'(SELECT q.wins / p.total AS win_rate FROM '
               f'(SELECT COUNT(round_id)::real AS wins FROM {self.table_name("Boards")} '
               f'WHERE (player_id = {self.needs_quotes(player_id)}) '
               f'AND (league_id = {self.needs_quotes(league_id)}) AND (place = 1)) AS q '
               f'CROSS JOIN '
               f'(SELECT COUNT(round_id)::real AS total FROM {self.table_name("Boards")} '
               f'WHERE (player_id = {self.needs_quotes(player_id)}) '
               f'AND (league_id = {self.needs_quotes(league_id)})) AS p) AS j '

               f'CROSS JOIN '
               f'(SELECT q.plays / p.total AS play_rate FROM '
               f'(SELECT COUNT(round_id)::real AS plays FROM {self.table_name("Boards")} '
               f'WHERE (player_id = {self.needs_quotes(player_id)}) '
               f'AND (league_id = {self.needs_quotes(league_id)}) AND (place > 0)) AS q '
               f'CROSS JOIN '
               f'(SELECT COUNT(round_id)::real AS total FROM {self.table_name("Rounds")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f') '
               f'AS p) AS k '

               f'CROSS JOIN '
               f'(SELECT p.player_id, AVG(CASE WHEN p.votes/q.songs::real > r.avg_generosity THEN 1 ELSE 0 END) AS generous '
               f'FROM (SELECT v.player_id, count(v.player_id) AS votes, s.round_id, s.league_id '
               f'FROM {self.table_name("Votes")} AS v '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON (s.league_id = v.league_id) AND (s.song_id = v.song_id) '
               f'WHERE v.player_id IS NOT NULL '
               f'GROUP BY s.league_id, s.round_id, v.player_id) AS p '
               f'LEFT JOIN '
               f'(SELECT count(s.song_id) AS songs, s.round_id FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Rounds")} AS d '
               f'ON s.round_id = d.round_id '
               f'GROUP BY s.league_id, s.round_id) AS q ON p.round_id = q.round_id ' 
               f'LEFT JOIN (SELECT p.round_id, AVG(p.votes/q.songs::real) AS avg_generosity '
               f'FROM (SELECT v.player_id, COUNT(v.player_id) AS votes, s.round_id FROM {self.table_name("Votes")} AS v '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON (s.league_id = v.league_id) AND (s.song_id = v.song_id) '
               f'WHERE v.player_id IS NOT NULL '
               f'GROUP BY s.league_id, s.round_id, v.player_id) AS p '
               f'LEFT JOIN (SELECT count(s.song_id) AS songs, s.round_id FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Rounds")} AS d '
               f'ON s.round_id = d.round_id '
               f'GROUP BY s.league_id, s.round_id) AS q '
               f'ON p.round_id = q.round_id GROUP BY p.round_id) AS r '
               f'ON p.round_id = r.round_id '
               f'WHERE p.player_id = {self.needs_quotes(player_id)} AND p.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY p.player_id) AS z '

               f';'
               )

        awards_df = self.read_sql(sql).squeeze(0)

        return awards_df

    def get_badge(self, league_id, player_id, competition=None, competition_title=None):
        if (not competition) and (not competition_title):
            sql = (f'SELECT q.badge FROM '
                   f'(SELECT player_id, RANK() OVER (ORDER BY wins DESC) AS badge '
                   f'FROM {self.table_name("Members")} '
                   f'WHERE league_id = {self.needs_quotes(league_id)}) AS q '
                   f'WHERE q.player_id = {self.needs_quotes(player_id)};'
                   )
        else:
            if not competition_title:
                competition_title = self.get_current_competition(league_id)
            
            if competition_title:
                sql = (f'SELECT q.badge FROM (SELECT r.player_id, RANK() '
                       f'OVER(ORDER BY SUM(r.points) DESC) AS badge '
                       f'FROM {self.table_name("Rounds")} AS d '
                       f'RIGHT JOIN {self.table_name("Competitions")} AS c '
                       f'ON d.league_id = c.league_id AND d.date >= c.start '
                       f'AND d.date <= (CASE WHEN c.finish IS NOT NULL THEN c.finish ELSE '
                       f'(SELECT MAX(date) FROM {self.table_name("Rounds")} '
                       f'WHERE c.league_id = {self.needs_quotes(league_id)} '
                       f') '
                       f'END) '
                       f'RIGHT JOIN {self.table_name("Rankings")} AS r '
                       f'ON d.league_id = r.league_id AND d.round_id = r.round_id '
                       f'WHERE c.league_id = {self.needs_quotes(league_id)} '
                       f'AND c.competition = {self.needs_quotes(competition_title)} '
                       f'GROUP BY r.player_id) AS q WHERE q.player_id = {self.needs_quotes(player_id)};'
                       )
            else:
                sql = None

        if sql:
            badge = self.read_sql(sql).squeeze()
        else:
            badge = None

        return badge

    def get_competitions(self, league_id):
        sql = (f'SELECT c.competition, d.round_id '
               f'FROM {self.table_name("Rounds")} AS d '
               f'RIGHT JOIN {self.table_name("Competitions")} AS c '
               f'ON d.league_id = c.league_id '
               f'AND d.date >= c.start AND d.date <= (CASE WHEN c.finish IS NOT NULL '
               f'THEN c.finish ELSE (SELECT MAX(date) FROM {self.table_name("Rounds")} '
               f'WHERE c.league_id = {self.needs_quotes(league_id)} '
               f') '
               f'END) '
               f'WHERE c.league_id = {self.needs_quotes(league_id)} '
               f'ORDER BY d.date;'
               )

        competitions_df = self.read_sql(sql)

        return competitions_df

    def get_current_competition(self, league_id):
        sql = (f'SELECT competition FROM {self.table_name("Competitions")} '
               f'WHERE (start <= CURRENT_DATE) AND (finish >= CURRENT_DATE OR finish IS NULL) '
               f'AND (league_id = {self.needs_quotes(league_id)})'
               f'LIMIT 1;' )

        competition_title = self.read_sql(sql)
        
        if len(competition_title):
            competition_title = competition_title.squeeze()
        else:
            competition_title = None

        return competition_title

    def get_competition_results(self, league_id, competition_title=None):
        if competition_title is None:
            # get current competition
            competition_title = self.get_current_competition(league_id)

        if competition_title:
            sql = (f'SELECT r.player_id, RANK() '
                   f'OVER(ORDER BY SUM(r.points) DESC) AS place '
                   f'FROM {self.table_name("Rounds")} AS d '
                   f'RIGHT JOIN {self.table_name("Competitions")} AS c '
                   f'ON d.league_id = c.league_id AND d.date >= c.start '
                   f'AND d.date <= (CASE WHEN c.finish IS NOT NULL THEN c.finish ELSE '
                   f'(SELECT MAX(date) FROM {self.table_name("Rounds")} '
                   f'WHERE c.league_id = {self.needs_quotes(league_id)} '
                   f') END) '
                   f'RIGHT JOIN {self.table_name("Rankings")} AS r '
                   f'ON d.league_id = r.league_id AND d.round_id = r.round_id '
                   f'WHERE c.league_id = {self.needs_quotes(league_id)} '
                   f'AND c.competition = {self.needs_quotes(competition_title)} '
                   f'GROUP BY r.player_id;'
                   )
               
            results_df = self.read_sql(sql)

        else:
            results_df = None

        return results_df

    def get_hoarding(self, league_id):
        sql = (f'SELECT q.round_id, q.player_id, q.votes/p.total::real*n.player_ids/(n.player_ids-1) AS pct '
               f'FROM (SELECT s.round_id, v.player_id, COUNT(v.song_id) AS votes '
               f'FROM {self.table_name("Votes")} AS v '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON v.song_id = s.song_id AND v.league_id = s.league_id '
               f'WHERE v.league_id = {self.needs_quotes(league_id)} AND v.player_id IS NOT NULL '
               f'GROUP BY s.round_id, v.player_id) AS q '
               f'LEFT JOIN (SELECT round_id, COUNT(song_id) AS total '
               f'FROM {self.table_name("Songs")} WHERE league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY round_id) AS p ON q.round_id = p.round_id '
               f'LEFT JOIN (SELECT round_id, COUNT(DISTINCT submitter_id) AS player_ids '
               f'FROM {self.table_name("Songs")} WHERE league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY round_id) AS n ON q.round_id = n.round_id;'
               )

        hoarding_df = self.read_sql(sql).pivot(index='player_id', columns='round_id', values='pct')

        reindexer = [c for c in self.get_round_order(league_id) if c in hoarding_df.columns]
        hoarding_df = hoarding_df.reindex(columns=reindexer)
                
        return hoarding_df