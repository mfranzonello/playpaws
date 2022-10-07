''' Database structure and functions '''

from datetime import date
import json

from sqlalchemy import create_engine
from pandas import read_sql, DataFrame, Series, isnull
from pandas.api.types import is_numeric_dtype

from common.secret import get_secret
from display.streaming import Streamable, cache

class Engineer:
    def __init__(self):
        self.engine_string = (f'postgresql://{get_secret("BITIO_USERNAME")}'
                              f':{get_secret("BITIO_PASSWORD")}@{get_secret("BITIO_HOST")}'
                              f'/{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}')

    @cache(allow_output_mutation=True, max_entries=10, ttl=10800)
    def connect(self):  
        engine = create_engine(self.engine_string)
        connection = engine.connect()
        return connection

class Database(Streamable):
    god_id = '777'

    def __init__(self, streamer=None):
        super().__init__()
        self.db = f'{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}'
        
        self.add_streamer(streamer)
        
        self.streamer.print(f'Connecting to database {self.db}...')

        engineer = Engineer()
        self.connection = engineer.connect()
        self.streamer.print(f'\t...success!')

        self.schema = self.get_schema()
          
    def table_name(self, table_name:str) -> str:
        full_table_name = f'"{self.db}"."{table_name.lower()}"'

        return full_table_name

    def read_sql(self, sql, **kwargs):
        return read_sql(sql, self.connection, **kwargs)

    def get_schema(self):
        sql = (f'SELECT sc.table_name, sc.column_name, kc.constraint_name, '
               f'(CASE WHEN tc.constraint_type IN ({self.needs_quotes("PRIMARY KEY")}, '
               f'{self.needs_quotes("FOREIGN KEY")}) THEN tc.constraint_type '
               f'WHEN tc.constraint_type IS NULL THEN {self.needs_quotes("VALUE")} '
               f'ELSE NULL END) AS column_type '
               f'FROM information_schema.columns AS sc '
               f'LEFT JOIN information_schema.key_column_usage AS kc '
               f'ON sc.table_name = kc.table_name '
               f'AND sc.column_name = kc.column_name '
               f'AND sc.table_schema = kc.table_schema '
               f'LEFT JOIN information_schema.table_constraints AS tc '
               f'ON sc.table_name = tc.table_name '
               f'AND sc.table_schema = tc.table_schema '
               f'AND kc.constraint_name = tc.constraint_name '
               f'WHERE sc.table_schema = {self.needs_quotes(self.db)} '
               f'ORDER BY '
               f'sc.table_name, '
               f'tc.constraint_type DESC NULLS LAST; '
               )

        schema = self.read_sql(sql)

        return schema

    def get_keys(self, table_name):
        q = '(column_type == "PRIMARY KEY") & (table_name == @table_name.lower())'
        keys = self.schema.query(q)['column_name'].to_list()

        return keys

    def get_values(self, table_name, match_cols=None):
        q = '(column_type != "PRIMARY KEY") & (table_name == @table_name.lower())'
        if len(match_cols):
            q += ' & (column_name in @match_cols)'
        values = self.schema.drop_duplicates(['table_name', 'column_name'])\
            .query(q)['column_name'].to_list()

        return values

    def get_columns(self, table_name):
        q = 'table_name == @table_name.lower()'
        columns = self.schema.drop_duplicates(['table_name', 'column_name'])\
            .query(q)['column_name'].to_list()
        return columns

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
            value_columns = self.get_values(table_name, match_cols=df.columns)
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

            sql = f'SELECT player_id FROM {self.table_name("members")} WHERE {wheres}'

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

    def get_leagues(self):
        # get league IDs
        leagues_df = self.get_table('Leagues', order_by={'column': 'created_date', 'sort': 'ASC'})
        return leagues_df

    def store_leagues(self, leagues_df):
        # store league names
        df = leagues_df.reindex(columns=self.get_columns('Leagues'))
        self.upsert_table('Leagues', df)

    def get_league_creator(self, league_id):
        # get name of league creator
        sql = f'SELECT creator_id FROM {self.table_name("leagues")} WHERE league_id = {self.needs_quotes(league_id)}' 
        creators_df = self.read_sql(sql)
        if len(creators_df):
            creator_id = creators_df['creator_id'].iloc[0]
        else:
            creator_id = None
        return creator_id 

    def get_player_leagues(self, player_id):
        sql = (f'SELECT league_id FROM {self.table_name("members")} '
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
        rounds_df = self.get_table('Rounds', league_id=league_id, order_by={'column': 'created_date', 'sort': 'ASC'}, drop_league=True)
        return rounds_df


    def get_uncreated_rounds(self, league_id):
        sql = (f'SELECT round_id, description, creator_id FROM {self.table_name("rounds")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) AND (creator_id IS NULL);') 

        rounds_df = self.read_sql(sql)

        return rounds_df 

    def store_rounds(self, rounds_df, league_id):
        df = rounds_df.reindex(columns=self.get_columns('Rounds'))
        df['league_id'] = league_id

        self.upsert_table('Rounds', df)

    def store_songs(self, songs_df, league_id):
        df = songs_df.reindex(columns=self.get_columns('Songs'))
        df['league_id'] = league_id
        self.upsert_table('Songs', df)

    def get_songs(self, league_id):
        songs_df = self.get_table('Songs', league_id=league_id, drop_league=True)
        return songs_df

    def store_votes(self, votes_df, league_id):
        df = votes_df.reindex(columns=self.get_columns('Votes'))
        df['league_id'] = league_id
        self.upsert_table('Votes', df)

    def get_votes(self, league_id):
        votes_df = self.get_table('Votes', league_id=league_id, drop_league=True)
        return votes_df
        
    def store_members(self, members_df, league_id):
        df = members_df.reindex(columns=self.get_columns('Members'))
        df['league_id'] = league_id
        self.upsert_table('Members', df)

    def get_results(self, league_id):
        songs_df = self.get_table('Results', league_id=league_id, drop_league=True)
        return songs_df

    def store_results(self, results_df, league_id):
        df = results_df.reindex(columns=self.get_columns('Results'))
        df['league_id'] = league_id
        self.upsert_table('Results', df)

    def get_members(self, league_id):
        members_df = self.get_table('Members', league_id=league_id, drop_league=True)
        return members_df

    def store_pulse(self, pulse_df, league_id):
        df = pulse_df.reindex(columns=self.get_columns('Pulse'))
        df['league_id'] = league_id
        self.upsert_table('Pulse', df)

    def get_pulse(self, league_id):
        pulse_df = self.get_table('Pulse', league_id=league_id, drop_league=True)
        return pulse_df

    def store_players(self, players_df, league_id=None):
        df = players_df.reindex(columns=self.get_columns('Players'))
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
        sql = f'SELECT {table}_name FROM {self.table_name(table + "s")} WHERE {table}_id = {self.needs_quotes(id)}'
        name = self.read_sql(sql)[f'{table}_name'].squeeze()

        return name

    def get_player_name(self, player_id):  
        return self.get_name(player_id, 'player')

    def get_round_name(self, round_id):  
        return self.get_name(round_id, 'round')

    def get_league_name(self, league_id):  
        return self.get_name(league_id, 'league')

    def get_competition_name(self, competition_id):
        return self.get_name(competition_id, 'competition')

    def get_player_ids(self, league_id=None):
        if league_id:
            members_df = self.get_members(league_id)
            player_ids = members_df['player_id'].to_list()
        else:
            sql = (f'SELECT player_id FROM {self.table_name("players")} '
                   f'WHERE player_ID != {self.needs_quotes(self.god_id)} '
                   f'ORDER BY player_name;'
                   )

            player_ids = self.read_sql(sql)['player_id'].to_list()

        return player_ids

    def get_god_id(self):
        return self.god_id

    def get_inactive_players(self):
        sql = (f'SELECT player_id FROM {self.table_name("players")} '
               f'WHERE inactive = {self.needs_quotes(True)};'
               )

        player_ids = self.read_sql(sql)['player_id'].to_list()

        return player_ids

    def get_extendable_leagues(self):
        sql = (f'SELECT league_id FROM {self.table_name("leagues")} '
               f'WHERE extendable = {self.needs_quotes(True)};'
               )

        league_ids = self.read_sql(sql)['league_id'].to_list()

        return league_ids

    def get_weights(self, version):
        sql = (f'SELECT DISTINCT ON(parameter) version, parameter, weight_value '
               f'FROM {self.table_name("weights")} WHERE version >= FLOOR({version}::real) '
               f'ORDER BY parameter, version DESC;'
               )

        weights = self.read_sql(sql, index_col='parameter')['weight_value']
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
        df = df.reindex(columns=self.get_columns(table_name))
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
        sql = (f'SELECT playlist_url AS url FROM {self.table_name("rounds")} '
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
        sql = (f'SELECT COUNT(DISTINCT track_uri) FROM {self.table_name("songs")} '
               f'WHERE league_id = {self.needs_quotes(league_id)};'
               )

        count = self.read_sql(sql)['count'].iloc[0]

        return count

    def get_track_durations(self, league_id):
        sql = (f'SELECT SUM(t.duration) AS duration FROM '
               f'(SELECT DISTINCT track_uri FROM {self.table_name("songs")} '
               f'WHERE league_id = {self.needs_quotes(league_id)}) as s '
               f'LEFT JOIN {self.table_name("tracks")} AS t ON s.track_uri = t.uri;'
               )

        duration = self.read_sql(sql)['duration'].iloc[0]

        return duration

    def get_theme_playlists(self, theme):
        # get playlists or track URIs to pull songs from
        if theme == 'complete':
            # all songs
            sql = (f'SELECT league_id, round_id, playlist_url AS url FROM {self.table_name("rounds")} '
                   f'WHERE playlist_url IS NOT NULL '
                   f'ORDER BY created_date;'
                   )

            wheres = f'theme = {self.needs_quotes(theme)}'
            
        elif theme == 'best':
            # based on performance
            sql = (f'SELECT s.league_id, s.round_id, t.uri, r.points, d.created_date FROM {self.table_name("results")} as r ' 
                   f'LEFT JOIN {self.table_name("songs")} as s ON (r.league_id = s.league_id) AND (r.song_id = s.song_id) '
                   f'LEFT JOIN {self.table_name("tracks")} as t ON s.track_uri = t.uri '
                   f'LEFT JOIN {self.table_name("rounds")} as d ON (s.league_id = d.league_id) AND (s.round_id = d.round_id) '
                   f';'
                   )

            wheres = f'theme = {self.needs_quotes(theme)}'

        elif theme == 'favorite':
            # player favorite
            sql = (f'SELECT * FROM (SELECT s.league_id, s.round_id, t.uri, v.player_id, v.vote, d.created_date '
                   f'FROM {self.table_name("votes")} as v '
                   f'LEFT JOIN {self.table_name("songs")} as s ON (v.league_id = s.league_id) AND (v.song_id = s.song_id) '
                   f'LEFT JOIN {self.table_name("tracks")} as t ON s.track_uri = t.uri '
                   f'LEFT JOIN {self.table_name("rounds")} AS d ON (s.league_id = d.league_id) AND (s.round_id = d.round_id) '
                   f'UNION SELECT s.league_id, s.round_id, t.uri, s.submitter_id, -1 as vote,  d.created_date ' 
                   f'FROM {self.table_name("songs")} as s '
                   f'LEFT JOIN {self.table_name("tracks")} as t ON s.track_uri = t.uri '
                   f'LEFT JOIN {self.table_name("rounds")} AS d ON (s.league_id = d.league_id) AND (s.round_id = d.round_id)) AS q '
                   f'WHERE q.player_id IS NOT NULL;'
                   )

            wheres = f'theme LIKE {self.needs_quotes(theme+"%%")}'

        rounds_df = self.read_sql(sql)

        # get comprehensive playlists
        selects = ', player_id' if theme == 'favorite' else ''

        sql = (f'SELECT league_id, uri, src, round_ids{selects} FROM {self.table_name("playlists")} '
               f'WHERE {wheres};'
               )

        playlists_df = self.read_sql(sql)
     
        return rounds_df, playlists_df

    def store_playlists(self, playlists_df, theme=None):
        df = playlists_df.reindex(columns=self.get_columns('Playlists'))
        if theme:
            df['theme'] = theme
        if theme in ['complete', 'best']:
            df['player_id'] = self.god_id

        self.upsert_table('Playlists', df)

    def flag_player_image(self, player_id):
        sql = (f'UPDATE {self.table_name("players")} '
               f'SET flagged = {self.needs_quotes(date.today())} '
               f'WHERE player_id = {self.needs_quotes(player_id)};'
               )

        self.execute_sql(sql)

    def get_players_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['src', 'uri', 'followers'])
        sql = (f'SELECT player_id, username FROM {self.table_name("players")} '
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
        sql = (f'SELECT DISTINCT track_uri AS url FROM {self.table_name("songs")} '
               f'WHERE track_uri NOT IN ' 
               f'(SELECT uri FROM {self.table_name("tracks")}) '
               f'UNION SELECT uri FROM {self.table_name("tracks")} WHERE {wheres};'
               )

        tracks_df = self.read_sql(sql)

        return tracks_df
    
    def get_artists_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['src', 'name', 'popularity',
                                                        'genres', 'followers'])
        sql = (f'SELECT t.a_uri as uri FROM '
               f'(SELECT DISTINCT jsonb_array_elements(artist_uri)->>0 AS a_uri '
               f'FROM {self.table_name("tracks")}) AS t '
               f'WHERE t.a_uri NOT IN (SELECT uri FROM {self.table_name("artists")}) '
               f'UNION SELECT uri FROM {self.table_name("artists")} WHERE {wheres};'
               )

        artists_df = self.read_sql(sql)

        return artists_df

    def get_albums_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['src', 'name', 'popularity',
                                                        'release_date', 'genres'])
        sql = (f'SELECT DISTINCT album_uri AS uri FROM {self.table_name("tracks")} '
               f'WHERE (album_uri NOT IN (SELECT uri FROM {self.table_name("albums")})) '
               f'UNION SELECT uri FROM {self.table_name("albums")} WHERE {wheres};'
               )

        albums_df = self.read_sql(sql)

        return albums_df

    def get_genres_update_sp(self):
        sql = (f'SELECT u.genre FROM (SELECT DISTINCT '
               f'jsonb_array_elements(genres)->>0 AS genre '
               f'FROM {self.table_name("artists")} '
               f'UNION SELECT jsonb_array_elements(genres)->>0 AS genre '
               f'FROM {self.table_name("albums")}) AS u '
               f'WHERE u.genre NOT IN (SELECT name FROM {self.table_name("genres")});'
               )

        genres_df = self.read_sql(sql)

        return genres_df


    # LastFM functions
    def get_tracks_update_titles(self):
        sql = (f'SELECT uri, name AS unclean ' 
               f'FROM {self.table_name("tracks")} '
               f'WHERE title IS NULL;'
               )

        tracks_df = self.read_sql(sql)

        return tracks_df

    def get_tracks_update_fm(self):
        wheres = ' OR '.join(f'(t.{v} IS NULL)' for v in ['scrobbles', 'listeners',
                                                          'top_tags'])
        sql = (f'SELECT t.uri, t.title, a.name AS artist, '
               f't.scrobbles, t.listeners, t.top_tags '
               f'FROM {self.table_name("tracks")} as t '
               f'LEFT JOIN {self.table_name("artists")} AS a '
               f'ON (t.artist_uri->>0) = a.uri WHERE {wheres};'
               )

        tracks_df = self.read_sql(sql)

        return tracks_df

    # analytics functions
    def store_analysis(self, league_id, version, round_ids=None, optimized=None): 
        today = date.today()
        d = {'league_id': [league_id],
             'created_date': [today],
             'version': [version]}

        d['round_ids'] = [round_ids]

        if optimized is not None:
            d['optimized'] = [optimized]

        analyses_df = DataFrame(d)
        
        self.upsert_table('Analyses', analyses_df)

    def get_analyses(self):
        analyses_df = self.get_table('Analyses', order_by={'other': 'Leagues',
                                                           'on': ['league_id'],
                                                           'column': 'created_date',
                                                           'sort': 'ASC'})
        return analyses_df
    
    def get_analyzed(self, league_id, round_ids, version):
        sql = (f'SELECT COUNT(*) FROM {self.table_name("analyses")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (round_ids = {self.needs_quotes(round_ids)}) '
               f'AND (version = {version}::real);'
               )

        analyzed = self.read_sql(sql)['count'].iloc[0] > 0

        return analyzed

    def get_optimized(self, league_id):
        sql = (f'SELECT COUNT(*) FROM {self.table_name("analyses")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (optimized = TRUE);'
               )

        optimized = self.read_sql(sql)['count'].iloc[0] > 0

        return optimized

    def store_rankings(self, rankings_df, league_id):
        df = rankings_df.reset_index().reindex(columns=self.get_columns('Rankings'))
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
                                   order_by={'column': 'created_date', 'sort': 'ASC'})['round_id']

        return reindexer

    def store_boards(self, boards_df, league_id):
        df = boards_df.reset_index().melt(id_vars='player_id',
                                          value_vars=boards_df.columns,
                                          var_name='round_id',
                                          value_name='place').dropna(subset=['place']).reindex(columns=self.get_columns('Boards'))
        df['league_id'] = league_id

        self.upsert_table('Boards', df)

    def get_boards(self, league_id):
        reindexer = self.get_round_order(league_id)
        boards_df = self.get_table('Boards', league_id=league_id, order_by={'other': 'Rounds', 
                                                                            'on': ['league_id', 'round_id'],
                                                                            'column': 'created_date',
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
                   f'FROM {self.table_name("votes")} AS v '
                   f'LEFT JOIN {self.table_name("songs")} AS s '
                   f'ON (v.song_id = s.song_id) AND (v.league_id = s.league_id) '
                   f'LEFT JOIN {self.table_name("tracks")} AS t ON s.track_uri = t.uri '
                   f'WHERE s.league_id = {self.needs_quotes(league_id)} '
                   f'GROUP BY v.player_id;'
                   )
        else:
            gb = 'submitter_id'
            sql = (f'SELECT s.submitter_id, ' 
                   f'count(CASE WHEN t.explicit THEN 1 END) / '
                   f'count(CASE WHEN NOT t.explicit THEN 1 END)::real AS dirtiness '
                   f'FROM {self.table_name("songs")} AS s '
                   f'LEFT JOIN {self.table_name("tracks")} AS t ON s.track_uri = t.uri '
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
                   f'FROM {self.table_name("songs")} AS s '
                   f'LEFT JOIN {self.table_name("tracks")} AS t ON s.track_uri = t.uri ' 
                   f'LEFT JOIN {self.table_name("rounds")} AS r ON s.round_id = r.round_id '
                   f'WHERE s.league_id = {self.needs_quotes(league_id)} '
                   f'GROUP BY s.round_id, r.created_date ORDER BY r.created_date;'
                   )

        features_df = self.read_sql(sql)

        return features_df

    def get_discoveries(self, league_id, base=1000):
        sql = (f'SELECT s.round_id, s.song_id, '
               f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery '
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t ON s.track_uri = t.uri '
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY s.round_id, s.song_id;'
               )

        discoveries_df = self.read_sql(sql)

        return discoveries_df

    def get_discovery_scores(self, league_id, base=1000):
        sql = (f'SELECT s.submitter_id, '
               f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery, '
               f'AVG(t.popularity::real/100) AS popularity '
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t ON s.track_uri = t.uri '
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
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t '
               f'ON s.track_uri = t.uri ' 
               f'LEFT JOIN {self.table_name("artists")} AS a '
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
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t '
               f'ON s.track_uri = t.uri ' 
               f'LEFT JOIN {self.table_name("artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'UNION '
               f'SELECT jsonb_array_elements(t.top_tags) AS tag '
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t '
               f'ON s.track_uri = t.uri ' 
               f'WHERE s.league_id = {self.needs_quotes(league_id)}) AS q '
               f'WHERE q.tag NOT IN '
               f'(SELECT jsonb_array_elements(a.genres) as tag '
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t '
               f'ON s.track_uri = t.uri ' 
               f'LEFT JOIN {self.table_name("artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league_id != {self.needs_quotes(league_id)} '
               f'UNION '
               f'SELECT jsonb_array_elements(t.top_tags) AS tag '
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t '
               f'ON s.track_uri = t.uri ' 
               f'WHERE s.league_id != {self.needs_quotes(league_id)});'
               )

        exclusives = self.read_sql(sql)['tag']

        return exclusives

    def get_song_results(self, league_id):
        sql = (# convert artist URIs to artist names
               f'WITH expanded AS ('
               f'SELECT t.uri AS track_uri, t.name AS track, '
               f'value as artist_uri, ordinality '
               f'FROM {self.table_name("tracks")} AS t, '
               f'jsonb_array_elements_text(t.artist_uri) WITH ORDINALITY), '

               # group artist names
               f'combined AS ('
               f'SELECT r.league_id, r.round_id, expanded.track_uri, '
               f'jsonb_agg(a.name ORDER BY expanded.ordinality) AS artist '
               f'FROM {self.table_name("rounds")} AS r '
               f'LEFT JOIN {self.table_name("songs")} AS s '
               f'ON r.league_id = s.league_id AND r.round_id = s.round_id '
               f'LEFT JOIN expanded ON s.track_uri = expanded.track_uri '
               f'LEFT JOIN {self.table_name("artists")} AS a '
               f'ON expanded.artist_uri = a.uri '
               f'GROUP BY r.league_id, r.round_id, expanded.track_uri) '

               # combine with other tables
               f'SELECT c.round_id, s.song_id, s.submitter_id, '
               f'c.artist, t.title, b.release_date, b.src, r.closed, r.points '
               f'FROM combined AS c '
               f'LEFT JOIN {self.table_name("songs")} AS s '
               f'ON c.league_id = s.league_id AND c.round_id = s.round_id '
               f'AND c.track_uri = s.track_uri '
               f'LEFT JOIN {self.table_name("tracks")} AS t ON c.track_uri = t.uri '
               f'LEFT JOIN {self.table_name("albums")} AS b ON t.album_uri = b.uri '
               f'LEFT JOIN {self.table_name("results")} AS r '
               f'ON r.league_id = s.league_id AND r.song_id = s.song_id '
               f'LEFT JOIN {self.table_name("leagues")} AS l '
               f'ON r.league_id = l.league_id '
               f'LEFT JOIN {self.table_name("rounds")} AS d '
               f'ON r.league_id = d.league_id AND c.round_id = d.round_id '
               f'WHERE l.league_id = {self.needs_quotes(league_id)} '
               f'ORDER BY l.created_date ASC, d.created_date ASC, r.points DESC;'
               )

        results_df = self.read_sql(sql)

        return results_df

    def get_round_descriptions(self, league_id):
        descriptions_df = self.get_table('Rounds', columns=['round_id', 'description'],
                                         league_id=league_id, order_by={'column': 'created_date',
                                                                        'sort': 'ASC'})

        return descriptions_df

    def get_creators_and_winners(self, league_id):
        sql = (f'SELECT r.round_id, r.creator_id, jsonb_agg(b.player_id) AS winner '
               f'FROM {self.table_name("rounds")} as r '
               f'LEFT JOIN {self.table_name("boards")} as b '
               f'ON (r.league_id = b.league_id) AND (r.round_id = b.round_id) '
               f'WHERE (r.league_id = {self.needs_quotes(league_id)}) '
               f'AND ((b.place < 2) OR (b.place IS NULL)) ' ## can this be MIN without GROUP BY?
               f'GROUP BY r.round_id, r.creator_id, r.created_date '
               f'ORDER BY r.created_date;'
               )
        
        creators_winners_df = self.read_sql(sql)

        return creators_winners_df

    def get_all_artists(self, league_id):
        sql = (f'SELECT s.league_id, s.song_id, json_agg(DISTINCT a.name) as arist '
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")}AS t ON s.track_uri = t.uri ' 
               f'LEFT JOIN {self.table_name("artists")} AS a ON t.artist_uri ? a.uri '
               f'WHERE s.league_id = {self.needs_quotes(league_id)} '
               f'GROUP BY s.song_id, s.league;'
               )

        all_artists_df = self.read_sql(sql)

        return all_artists_df

    def get_all_info(self):
        sql = (f'SELECT q.league_id, x.song_id, q.round_id, x.artist, q.title, q.submitter_id FROM '
               f'(SELECT s.league_id, s.song_id, json_agg(DISTINCT a.name) as artist '
               f'FROM {self.table_name("songs")} AS s '
               f'LEFT JOIN {self.table_name("tracks")} AS t ON s.track_uri = t.uri ' 
               f'LEFT JOIN {self.table_name("artists")} AS a ON t.artist_uri ? a.uri '
               f'GROUP BY s.song_id, s.league_id) x '
               f'LEFT JOIN {self.table_name("songs")} AS q '
               f'ON (x.song_id = q.song_id) AND (x.league_id = q.league_id);'
               )

        all_info_df = self.read_sql(sql)

        return all_info_df

    def get_player_pulse(self, league_id, player_id):
        sql = (f'SELECT p.player_id, p.likes_id, p.liked_id, j.closest_id '
               f'FROM (SELECT player_id, likes_id, liked_id  '
               f'FROM {self.table_name("members")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (player_id = {self.needs_quotes(player_id)})) as p '

               f'CROSS JOIN '

               f'(SELECT p2_id AS closest_id FROM {self.table_name("pulse")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (p1_id = {self.needs_quotes(player_id)}) '
               f'AND (distance IN (SELECT MIN(distance) '
               f'FROM {self.table_name("pulse")} '
               f'WHERE (league_id = {self.needs_quotes(league_id)}) '
               f'AND (p1_id = {self.needs_quotes(player_id)}) '
               f'GROUP BY league_id, p1_id)) LIMIT 1) AS j '

               ##f'(SELECT q.player AS closest FROM ( '
               ##f'SELECT player, abs(dfc - '
               ##f'(SELECT dfc FROM {self.table_name("members")} '
               ##f'WHERE (league = {self.needs_quotes(league_title)}) '
               ##f'AND (player = {self.needs_quotes(player_name)}))) AS distance '
               ##f'FROM {self.table_name("members")} '
               ##f'WHERE (league = {self.needs_quotes(league_title)}) '
               ##f'AND (player != {self.needs_quotes(player_name)}) '
               ##f'ORDER BY distance LIMIT 1) AS q) AS j'
               
               f';'
               )

        player_pulse_df = self.read_sql(sql).squeeze(0)

        return player_pulse_df

    def add_on(self, add_type, alias, alias2=None, value=None,
               comma=False, conjunction=None):
        if not add_type:
            add_on = ''

        else:
            sp = ' ' if (comma or conjunction) else ''
            period = '.' if alias else ''
            add_on = (f'{conjunction if conjunction else ""}{"," if comma else ""}'
                      f'{sp}{alias}{period}{add_type}')
            if (value or alias2):
                add_on += f' = '
                if value:
                    add_on += f'{self.needs_quotes(value)}'
                else:
                    add_on += f'{alias2}{period}{add_type}'

            add_on += ' '
            
        return add_on

    def add_on1(self, add_type, alias, alias2):
        return self.add_on(add_type, alias, alias2=alias2, conjunction='AND')

    def add_on2(self, add_type, alias):
        return self.add_on(add_type, alias, comma=True)

    def add_on3(self, add_type, alias, value):
        return self.add_on(add_type, alias, value=value, comma=True)

    def get_round_awards(self, league_id, round_id=None, base=1000,
                         categories=None):
        add_type = 'round_id' if round_id else None
        
        sqls = {# all players
                'rd': {'cols': [x for x in ['league_id',
                                            add_type if add_type else None,
                                            'player_id'] if x],
                       'sql':
                       (f'rd AS ('
                       f'SELECT r.league_id, r.round_id, m.player_id '
                       f'FROM {self.table_name("rounds")} AS r '
                       f'JOIN {self.table_name("members")} AS m '
                       f'ON r.league_id = m.league_id)'
                       )},
            
                # commenting
                'ch': {'cols': ['comments', 'chatty'],
                       'sql':
                       (f'ch AS ('
                       f'SELECT c.league_id{self.add_on2(add_type, "c")}, c.player_id, '
                       f'count(c.comment) AS comments, '
                       f'RANK() OVER (PARTITION BY c.league_id{self.add_on2(add_type, "c")} '
                       f'ORDER BY count(c.comment) DESC) AS chatty '
                       f'FROM ('
                       f'SELECT league_id{self.add_on2(add_type, "")}, submitter_id AS player_id, comment '
                       f'FROM {self.table_name("songs")} '
                       f'UNION ALL '
                       f'SELECT v.league_id{self.add_on2(add_type, "s")}, v.player_id, v.comment '
                       f'FROM {self.table_name("votes")} AS v '
                       f'LEFT JOIN {self.table_name("songs")} AS s '
                       f'ON s.league_id = v.league_id AND s.song_id = v.song_id) AS c '
                       f'GROUP BY c.league_id{self.add_on2(add_type, "c")}, c.player_id)'
                       )},

                # mainstream
                'po': {'cols': ['popularity', 'popular'],
                       'sql':
                       (f'po AS (SELECT s.league_id{self.add_on2(add_type, "s")}, s.submitter_id AS player_id, '
                       f'AVG(t.popularity::real/100) AS popularity, '
                       f'RANK() OVER (PARTITION BY s.league_id{self.add_on2(add_type, "s")} '
                       f'ORDER BY AVG(t.popularity::real/100) DESC) AS popular '
                       f'FROM {self.table_name("songs")} AS s '
                       f'LEFT JOIN {self.table_name("tracks")} AS t '
                       f'ON s.track_uri = t.uri '
                       f'GROUP BY s.league_id{self.add_on2(add_type, "s")}, s.submitter_id)'
                       )},

                # undiscovered
                'di': {'cols': ['discovery', 'discoverer'],
                       'sql':
                       (f'di AS ('
                       f'SELECT s.league_id{self.add_on2(add_type, "s")}, s.submitter_id AS player_id, '
                       f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery, '
                       f'RANK() OVER (PARTITION BY s.league_id{self.add_on2(add_type, "s")} '
                       f'ORDER BY AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) DESC) AS discoverer '
                       f'FROM {self.table_name("songs")} AS s '
                       f'LEFT JOIN {self.table_name("tracks")} AS t '
                       f'ON s.track_uri = t.uri '
                       f'GROUP BY s.league_id{self.add_on2(add_type, "s")}, s.submitter_id)'
                       )},

                # explicitness
                'dr': {'cols': ['dirtiness', 'dirtiest'],
                       'sql':
                       (f'dr AS ('
                       f'SELECT s.league_id{self.add_on2(add_type, "s")}, s.submitter_id AS player_id, '
                       f'AVG(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS dirtiness, '
                       f'RANK() OVER (PARTITION BY s.league_id{self.add_on2(add_type, "s")} '
                       f'ORDER BY AVG(CASE WHEN t.explicit THEN 1 ELSE 0 END) DESC) AS dirtiest '
                       f'FROM {self.table_name("songs")} AS s '
                       f'LEFT JOIN {self.table_name("tracks")} AS t '
                       f'ON s.track_uri = t.uri '
                       f'GROUP BY s.league_id{self.add_on2(add_type, "s")}, s.submitter_id)'
                       )},

               # hoarding
               f'sv{"" if add_type else "l"}': {'cols': ['generosity', 'generous'],
                      'sql':
                      (f'vo AS ('
                      f'SELECT v.league_id, s.round_id, v.player_id, '
                      f'SUM(CASE WHEN v.vote > 0 THEN 1 ELSE 0 END) AS votes '
                      f'FROM {self.table_name("votes")} AS v '
                      f'JOIN {self.table_name("songs")} AS s '
                      f'ON v.league_id = s.league_id AND v.song_id = s.song_id '
                      f'GROUP BY v.league_id, s.round_id, v.player_id), '

                      f'so AS ('
                      f'SELECT s.league_id, s.round_id, '
                      f'COUNT(s.song_id) AS songs, COUNT(DISTINCT s.submitter_id) AS players '
                      f'FROM {self.table_name("songs")} AS s '
                      f'GROUP BY s.league_id, s.round_id), '
               
                      f'sv AS ('
                      f'SELECT vo.league_id, vo.round_id, vo.player_id, '
                      f'vo.votes, so.songs, so.players, '
                      f'vo.votes/(so.songs*(so.players-1)/(so.players))::real AS generosity, '
                      f'RANK() OVER(PARTITION BY vo.league_id, vo.round_id '
                      f'ORDER BY vo.votes/(so.songs*(so.players-1)/(so.players))::real DESC) AS generous '
                      f'FROM vo JOIN so ON vo.league_id = so.league_id AND vo.round_id = so.round_id), '

                      f'svl AS ('
                      f'SELECT sv.league_id, sv.player_id, '
                      f'SUM(sv.players * sv.generosity)/SUM(sv.players)::real AS generosity, '
                      f'RANK() OVER(PARTITION BY sv.league_id '
                      f'ORDER BY SUM(sv.players * sv.generosity)/SUM(sv.players)::real DESC) AS generous '
                      f'FROM sv GROUP BY sv.league_id, sv.player_id)'
                      )},

               # speed
               f'svdr{"" if add_type else "l"}': {'cols': ['submit_speed', 'submit_fastest',
                                                          'vote_speed', 'vote_fastest'],
                                                  'sql':
                     (f'sd AS ('
                      f'SELECT league_id, round_id, submitter_id AS player_id, '
                      f'to_timestamp(AVG(extract(epoch FROM created_date)))::date AS submit_date '
                      f'FROM {self.table_name("songs")} '
                      f'GROUP BY league_id, round_id, submitter_id), '
               
                      f'vd AS ('
                      f'SELECT v.league_id, s.round_id, v.player_id, '
                      f'to_timestamp(AVG(extract(epoch FROM v.created_date)))::date AS vote_date '
                      f'FROM {self.table_name("votes")} AS v '
                      f'JOIN {self.table_name("songs")} AS s '
                      f'ON v.league_id = s.league_id AND v.song_id = s.song_id '
                      f'GROUP BY v.league_id, s.round_id, v.player_id), '

                      f'svd AS ('
                      f'SELECT sd.league_id, sd.round_id, sd.player_id, '
                      f'sd.submit_date, vd.vote_date '
                      f'FROM sd JOIN vd '
                      f'ON sd.league_id = vd.league_id AND sd.round_id = vd.round_id '
                      f'AND sd.player_id = vd.player_id), '

                      f'svdr AS ('
                      f'SELECT r.league_id, r.round_id, svd.player_id, '
                      f'r.created_date, svd.submit_date, svd.vote_date, '
                      f'svd.submit_date - r.created_date AS submit_speed, '
                      f'svd.vote_date - r.created_date AS vote_speed, '
                      f'RANK() OVER (PARTITION BY r.league_id, r.round_id '
                      f'ORDER BY svd.submit_date - r.created_date ASC) AS submit_fastest, '
                      f'RANK() OVER (PARTITION BY r.league_id, r.round_id '
                      f'ORDER BY svd.vote_date - r.created_date ASC) AS vote_fastest '
                      f'FROM {self.table_name("rounds")} AS r '
                      f'RIGHT JOIN svd '
                      f'ON r.league_id = svd.league_id AND r.round_id = svd.round_id), '
               
                      f'svdrl AS ('
                      f'SELECT svdr.league_id, svdr.player_id, '
                      f'AVG(svdr.submit_speed) AS submit_speed, '
                      f'AVG(svdr.vote_speed) AS vote_speed, '
                      f'RANK() OVER (PARTITION BY svdr.league_id '
                      f'ORDER BY AVG(svdr.submit_speed)) AS submit_fastest, '
                      f'RANK() OVER (PARTITION BY svdr.league_id '
                      f'ORDER BY AVG(svdr.vote_speed)) AS vote_fastest '
                      f'FROM svdr GROUP BY svdr.league_id, svdr.player_id)'
                      )},
                }

        if categories:
            cats = ['rd'] + categories
        else:
            cats = list(sqls.keys())

        sql_withs = ', '.join(sqls[s]['sql'] for s in cats)
        sql_joins = ' '.join(f'JOIN {a} ON rd.league_id = {a}.league_id {self.add_on1(add_type, "rd", a)}'
                             f'AND rd.player_id = {a}.player_id' for a in cats[1:])
        sql_selects = ', '.join(f'{a}.{c}' for a in cats for c in sqls[a]['cols'])
        sql_where = '{self.add_on3(add_type, "rd", self.needs_quotes(round_id))}' \
            if isinstance(round_id, str) else ''

        sql = (# get results
               f'WITH {sql_withs} '
               f'SELECT {sql_selects} '
               f'FROM rd {sql_joins} '
               f'WHERE rd.league_id = {self.needs_quotes(league_id)} '
               f'{sql_where}'
               f'ORDER BY rd.league_id{self.add_on2(add_type, "rd")}, rd.player_id;'
               )

        round_awards = self.read_sql(sql)

        if not add_type:
            round_awards.drop_duplicates(inplace=True)
       
        return round_awards

    def get_league_awards(self, league_id, base=1000):
        round_awards = self.get_round_awards(league_id, base=base)

        sql = (# win
               f'WITH wipl AS ('
               f'SELECT league_id, player_id, '
               f'SUM(CASE WHEN place = 1 THEN 1 ELSE 0 END) AS wins, '
               f'COUNT(round_id)::real AS plays '
               f'FROM {self.table_name("boards")} '
               f'GROUP BY league_id, player_id), '

               # plays
               f'ro AS ('
               f'SELECT league_id, COUNT(round_id) AS total_rounds '
               f'FROM {self.table_name("rounds")} '
               f'GROUP BY league_id) '

               # win and play rates
               f'SELECT wipl.league_id, wipl.player_id, '
               f'wipl.wins, wipl.plays, ro.total_rounds, '
               f'(wipl.wins/wipl.plays)::real AS win_rate, '
               f'(wipl.plays/ro.total_rounds)::real AS play_rate '
               f'FROM wipl LEFT JOIN ro ON wipl.league_id = ro.league_id;')

        league_awards = self.read_sql(sql).merge(round_awards, on=['league_id', 'player_id']).drop_duplicates()

        return league_awards

    def get_awards(self, league_id, player_id=None, round_id=None, base=1000):
        god_mode = player_id == self.get_god_id()
             
        if round_id:
            awards_df = self.get_round_awards(league_id, round_id, base=base)
        else:
            awards_df = self.get_league_awards(league_id, base=base)

        award_labels = {'chatty': ['chatty', True], 'quiet': ['chatty', False],
                        'popular': ['popular', True], 'discoverer': ['discoverer', True],
                        'dirtiest': ['dirtiest', True], 'clean': ['dirtiest', 0],
                        'generous': ['generosity', 0.66], 'stingy': ['generous', False],
                        'fast_submit': ['submit_fastest', True], 'slow_submit': ['submit_fastest', False],
                        'fast_vote': ['vote_fastest', True], 'slow_vote': ['vote_fastest', False],
                        }
        if (not round_id) and (not god_mode):
            award_labels.update({'win_rate': ['win_rate', None],
                                 'play_rate': ['play_rate', None],
                                 })

        awards_s = Series(index=award_labels.keys())

        for label in award_labels:
            col, pos = award_labels[label]
            if col in awards_df.columns:
                # highest value
                if pos is True:
                    value = awards_df[awards_df[col] == awards_df[col].min()]['player_id'].to_list()
                # lowest value
                elif pos is False:
                    value = awards_df[awards_df[col] == awards_df[col].max()]['player_id'].to_list()
                # exact match
                elif isinstance(pos, int):
                    value = awards_df[awards_df[col] == pos]['player_id'].to_list()
                # threshold
                elif isinstance(pos, float):
                    value = awards_df[awards_df[col].ge(pos)]['player_id'].to_list()
                # stat
                elif pos is None:
                    value = awards_df[awards_df['player_id'] == player_id][col].iloc[0]

                # is player
                if (not god_mode) and isinstance(pos, (bool, int, float)):
                    value = player_id in value
                    
                awards_s.loc[label] = value

        return awards_s

    def get_league_placement(self, league_id):
        sql = (f'SELECT player_id, '
               f'RANK() OVER (PARTITION BY league_id ORDER BY wins DESC) AS place '
               f'FROM {self.table_name("members")} '
               f'WHERE league_id = {self.needs_quotes(league_id)};'
               )

        places_df = self.read_sql(sql)

        return places_df

    def get_round_placement(self, league_id, player_id=None):
        wheres = f'WHERE b.player_id = {self.needs_quotes(player_id)} AND b.place = 1 ' if player_id else ''

        sql = (f'SELECT b.round_id, b.player_id, b.place '
               f'FROM {self.table_name("boards")} AS b '
               f'JOIN {self.table_name("rounds")} AS r ON b.round_id = r.round_id '
               f'{wheres}'
               f'ORDER BY r.created_date')

        places_df = self.read_sql(sql)

        return places_df

    def get_competition_placement(self, league_id, competition_id=None, player_id=None):
        if competition_id:
            # look at specific competition
            wheres1 = f'AND c.competition_id = {self.needs_quotes(competition_id)} '
        else:
            # look at all finished competition
            wheres1 = f'AND c.finished = TRUE '

        # find competitions won by a player
        wheres2 = f'WHERE player_id = {self.needs_quotes(player_id)} AND place = 1 ' if player_id else ''
        
        sql = (f'WITH cr AS ('
               f'SELECT r.league_id, r.player_id, c.competition_id, '
               f'RANK() OVER(PARTITION BY r.league_id, c.competition_id '
               f'ORDER BY SUM(r.points) DESC) AS place '
               f'FROM {self.table_name("rankings")} AS r '
               f'JOIN {self.table_name("competitions")} AS c '
               f'ON c.round_ids ? r.round_id '
               f'WHERE c.league_id = {self.needs_quotes(league_id)} '
               f'{wheres1}'
               f'GROUP BY r.league_id, c.competition_id, r.player_id) '
               f'SELECT * FROM cr '
               f'{wheres2}'
               f'ORDER BY competition_id;'
               )

        places_df = self.read_sql(sql)

        return places_df

    def get_badge(self, league_id, player_id, competition=None, competition_id=None):
        if (not competition) and (not competition_id):
            places_df = self.get_league_placement(league_id)

        else:
            if not competition_id:
                competition_id = self.get_current_competition(league_id)
            
            if competition_id:
                places_df = self.get_competition_placement(league_id, competition_id=competition_id)
            else:
                places_df = None

        if (places_df is not None) and len(places_df.query('player_id == @player_id')):
            badge = places_df.query('player_id == @player_id')['place'].squeeze()
            n_players = len(places_df)
        else:
            badge = None
            n_players = 0

        return badge, n_players

    def get_competitions(self, league_id):
        sql = (f'SELECT c.competition_id, c.competition_name, d.round_id '
               f'FROM {self.table_name("rounds")} AS d '
               f'RIGHT JOIN {self.table_name("competitions")} AS c '
               f'ON d.league_id = c.league_id '
               f'AND c.round_ids ? d.round_id '
               f'WHERE c.league_id = {self.needs_quotes(league_id)} '
               f'ORDER BY d.created_date;'
               )

        competitions_df = self.read_sql(sql)

        return competitions_df

    def get_current_competition(self, league_id):
        sql = (f'SELECT competition_id, competition_name FROM {self.table_name("competitions")} '
               f'WHERE finished = FALSE OR finished IS NULL '
               f'AND (league_id = {self.needs_quotes(league_id)})'
               f'LIMIT 1;' )

        competitions_df = self.read_sql(sql)
        
        if len(competitions_df):
            competition_id = competitions_df['competition_id'].squeeze()
        else:
            competition_id = None

        return competition_id

    def get_competition_results(self, league_id, competition_id=None):
        if competition_id is None:
            # get current competition
            competition_id = self.get_current_competition(league_id)

        if competition_id:
            results_df = self.get_competition_placement(league_id, competition_id=competition_id)

        else:
            results_df = None

        return results_df

    def get_round_wins(self, league_id, player_id):
        round_wins = self.get_round_placement(league_id, player_id=player_id)['round_id'].to_list()

        return round_wins

    def get_competition_wins(self, league_id, player_id):
        competition_wins = self.get_competition_placement(league_id, player_id=player_id)['competition_id'].to_list()
        
        return competition_wins

    def get_hoarding(self, league_id):
        awards_round_df = self.get_round_awards(league_id, round_id=True)
        hoarding_df = awards_round_df.pivot(index='player_id', columns='round_id', values='generosity')

        awards_league_df = self.get_round_awards(league_id)
        most_generous = awards_league_df[awards_league_df['generous']==awards_league_df['generous'].min()]['player_id'].to_list()
        least_generous = awards_league_df[awards_league_df['generous']==awards_league_df['generous'].max()]['player_id'].to_list()
        
        reindexer = [c for c in self.get_round_order(league_id) if c in hoarding_df.columns]
        hoarding_df = hoarding_df.reindex(columns=reindexer)
                        
        return hoarding_df, most_generous, least_generous

    def get_emojis(self):
        emojis_df = self.get_table('emojis')
        emoji = {t1: e for e, t1, _ in emojis_df.dropna(subset=['single']).values}
        emojis = {e: t2 for e, _, t2 in emojis_df.dropna(subset=['multiple']).values}
        return emoji, emojis