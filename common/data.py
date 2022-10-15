''' Database structure and functions '''

import json
from datetime import date

import requests
from sqlalchemy import create_engine
from pandas import DataFrame, Series, isnull, read_sql
from pandas import read_sql, DataFrame, Series, isnull
from pandas.api.types import is_numeric_dtype

from common.secret import get_secret
from common.locations import BITIO_URL, BITIO_HOST
from display.streaming import Streamable, cache

class Engineer:
    def __init__(self):
        self.engine_string = (f'postgresql://{get_secret("BITIO_USERNAME")}'
                              f':{get_secret("BITIO_PASSWORD")}@{BITIO_HOST}'
                              f'/{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}')

    @cache(allow_output_mutation=True, max_entries=10, ttl=10800)
    def connect(self):  
        engine = create_engine(self.engine_string)
        connection = engine.connect()
        return connection

class Database(Streamable):
    god_id = '777'
    dtypes = {'text': 'str',
            'float4': 'float',
            'float8': 'float',
            'int4': 'int',
            'int8': 'int',
            'date': 'datetime64',
            'bool': 'bool',
            }

    def __init__(self, connection_type='alchemy', streamer=None):
        super().__init__()
        self.db = f'{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}'
        
        self.add_streamer(streamer)
        
        self.streamer.print(f'Connecting to database {self.db}...')
        self.connection_type = connection_type

        if self.connection_type == 'alchemy':
            engineer = Engineer()
            self.connection = engineer.connect()

        elif self.connection_type == 'api':
            self.url = BITIO_URL
            self.headers = {'Authorization': f'Bearer {get_secret("BITIO_PASSWORD")}',
                            'Accept': 'application/json',
                            'Content-Type': 'application/json'}

        self.streamer.print(f'\t...success!')
        
             
        self.table_schema = None
        self.view_schema = None
        self.name_schema = None
        self.schema_loaded = False
        self.load_schema()

        self.materialized = False

    def call_api(self, sql):
        ''' connect to bit.io API '''
        url = f'{self.url}/v2beta/query'
        data = json.dumps({'query_string': sql, 'database_name': self.db})

        response = requests.post(url, headers=self.headers, data=data)

        if response.ok:
            jason = response.json()

        else:
            jason = None

        return jason

    def convert_json(self, jason):
        ''' convert response contents to dataframe '''
        if jason:
            df = DataFrame(jason['data'], columns=jason['metadata'])
            
            for m, d in zip(jason['metadata'].keys(), jason['metadata'].values()):
                if m in ['jsonb', 'json']:
                    df[m] = df[m].apply(json.loads)

                else:
                    df[m] = df[m].astype(self.dtypes.get(d, 'object'))

        else:
            df = None

        return df

    # housekeeping functions
    def read_sql(self, sql, **kwargs):
        ''' execute SQL and return dataframe '''
        if self.connection_type == 'alchemy':
            df = read_sql(sql, self.connection, **kwargs)

        elif self.connection_type == 'api':
            jason = self.call_api(sql)
            df = self.convert_json(jason)

        return df

    def execute_sql(self, sql):
        ''' execute SQL and return nothing '''
        if len(sql):
            if self.connection_type == 'alchemy':
                self.connection.execute(sql)
            elif self.connection_type == 'api':
                self.read_sql(sql)

    def table_name(self, table_name:str) -> str:
        material = '_m'
        if self.schema_loaded:
            relationship = self.name_schema.query('table_name.str.lower() == @table_name.lower()')['relationship'].iloc[0].lower()

            if relationship == 'view' and len(self.name_schema.query('table_name.str.lower() == @table_name.lower() + "_m"')['relationship']):
                name = table_name + '_m'
            else:
                name = table_name

            if name[:-len(material)] == material:
                self.materialize()
        else:
            name = table_name

        full_table_name = f'"{self.db}"."{name.lower()}"'

        return full_table_name

    def load_schema(self):
        self.table_schema = self.get_table('_schema_tables')
        self.view_schema = self.get_table('_schema_views')
        self.name_schema = self.get_table('_schema_names')
        self.schema_loaded = all(s is not None for s in [self.table_schema, self.view_schema, self.name_schema])

    def materialize(self, table_name=None):
        if table_name:
            sql = (f'REFRESH MATERIALIZED VIEW {self.table_name(table_name)};')
            self.execute_sql(sql)

        else:
            if not self.materialized:
                material_views = self.name_schema.query('relationship.str.lower() == "materialized"')['name'].to_list()
                for m_view in material_views:
                    ## add a way to check dependencies
                    sql = (f'REFRESH MATERIALIZED VIEW {self.table_name(m_view)};')
                    self.execute_sql(sql)

                self.materialized = True

    def get_keys(self, table_name):
        q = '(column_type == "PRIMARY KEY") & (table_name == @table_name.lower())'
        keys = self.table_schema.query(q)['column_name'].to_list()

        return keys

    def get_values(self, table_name, match_cols=None):
        q = '(column_type != "PRIMARY KEY") & (table_name == @table_name.lower())'
        if len(match_cols):
            q += ' & (column_name in @match_cols)'
        values = self.table_schema.drop_duplicates(['table_name', 'column_name'])\
            .query(q)['column_name'].to_list()

        return values

    def get_columns(self, table_name):
        q = 'table_name == @table_name.lower()'
        columns = self.table_schema.drop_duplicates(['table_name', 'column_name'])\
            .query(q)['column_name'].to_list()
        return columns

    def get_table(self, table_name, league_id=None, columns=None, order_by=None, drop_league=False, **kwargs):
        ''' get values from database '''
        # assume WHERE, ORDER BY and columns
        if league_id:
            kwargs.update({'league_id': league_id})
        wheres = ' WHERE ' + ' AND '.join(f'{kw} {"= " + self.needs_quotes(kwargs[kw]) if kwargs[kw] is not None else "IS NULL"}' for kw in kwargs) if len(kwargs) else ''
        orders = f' ORDER BY {order_by["column"]} {order_by["sort"]}' if order_by else ''
        cols = ', '.join(columns) if columns else '*'

        # write and execute SQL
        sql = f'SELECT {cols} FROM {self.table_name(table_name)}{wheres}{orders};'
        table = self.read_sql(sql)

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

            sets = ', '.join(f'{col} = c.{col}' for col in value_columns)
            values = ', '.join('(' + ', '.join(self.needs_quotes(df.loc[i, col]) for col in all_columns) + ')' for i in df.index)
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

    def upsert_table(self, table_name, df):
        # update existing rows and insert new rows
        if len(df):
            # there are values to store
            # remove potential index issues
            df_upsert = df.reset_index(drop=True)
            keys = self.get_keys(table_name)

            # only store columns that have values, so as to not overwrite with NA
            # retain key columns that have NA values, such as Votes table
            value_columns = self.get_values(table_name, match_cols=df_upsert.columns)
            df_store = df_upsert.drop(columns=df_upsert[value_columns].columns[df_upsert[value_columns].isna().all()])
            
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

    # query functions
    def get_player_match(self, league_id, partial_name):
        ''' find closest name '''
        # search by name and league
        members_df = self.get_table('members', league_id=league_id)

        matched_names = members_df.query('player_name.str.lower() == @partial_name)')
        if len(matched_names):
            player_id = matched_names['player_id'].iloc[0]

        else:
            found_names = members_df.query('player_name.str.contains(@partial_name, case=False)')
            player_id = found_names['player_id'].iloc[0] if len(found_names) else None

        return player_id

    ## eventual change song_id concept
    def get_song_ids(self, league_id:str, songs:DataFrame) -> DataFrame:
        # first check for which songs already exists
        ids_df = self.get_table('songs', league_id=league_id, drop_league=True).rename(columns={'song_id': 'new_song_id'})
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

    ## eventual change song_id concept
    def get_new_song_ids(self, league_id, n_retrieve):
        # get next available song_ids
        existing_song_ids = self.get_table('Songs', columns=['song_id'], league_id=league_id)['song_id'] 
        max_song_id = 0 if existing_song_ids.empty else existing_song_ids.max()
        next_song_ids = [song_id for song_id in range(1, max_song_id + n_retrieve + 1) \
            if song_id not in existing_song_ids.values][0:n_retrieve]
        return next_song_ids

    def get_leagues(self):
        # get league IDs
        leagues_df = self.get_table('leagues', order_by={'column': 'created_date', 'sort': 'ASC'})
        return leagues_df

    def store_leagues(self, leagues_df):
        # store league names
        df = leagues_df.reindex(columns=self.get_columns('leagues')) ## is this reindexing still necessary?
        self.upsert_table('leagues', df)

    def get_league_creator(self, league_id):
        # get name of league creator
        leagues_df = self.get_table('leagues', league_id=league_id)
        creator_id = leagues_df['creator_id'].iloc[0]

        return creator_id 

    def get_league_ids(self):
        league_ids = self.get_table('leagues')['league_id'].to_list()

        return league_ids

    def get_player_leagues(self, player_id):
        leagues_df = self.get_table('leagues', player_id=player_id)
        league_ids = leagues_df['league_id'].to_list()

        return league_ids

    def check_data(self, league_id, round_id=None): ## might need to change
        # see if there is any data for the league or round
        wheres = f'league_id = {self.needs_quotes(league_id)}'
        if round_id is not None:
            wheres = f'({wheres}) AND (round_id = {self.needs_quotes(round_id)})'
            count = 'round_id'
            table_name = 'rounds'
        else:
            count = 'league_id'
            table_name = 'leagues'

        sql = f'SELECT COUNT({count}) FROM {self.table_name(table_name)} WHERE {wheres}'

        count_df = self.read_sql(sql)
        check = count_df['count'].gt(0).all()

        return check

    def get_rounds(self, league_id):
        rounds_df = self.get_table('rounds', league_id=league_id, order_by={'column': 'created_date', 'sort': 'ASC'}, drop_league=True)
        return rounds_df

    def get_n_rounds(self, league_id): ## what is this used for?
        return len(self.get_rounds(league_id))

    def get_uncreated_rounds(self, league_id):
        uncreated_rounds_df = self.get_table('rounds', league_id=league_id, creator_id=None)
        return uncreated_rounds_df 

    def store_rounds(self, rounds_df, league_id):
        df = rounds_df.reindex(columns=self.get_columns('Rounds'))
        df['league_id'] = league_id

        self.upsert_table('Rounds', df)

    def store_songs(self, songs_df, league_id):
        df = songs_df.reindex(columns=self.get_columns('Songs'))
        df['league_id'] = league_id
        self.upsert_table('Songs', df)

    def get_songs(self, league_id):
        songs_df = self.get_table('songs', league_id=league_id, drop_league=True)
        return songs_df

    def store_votes(self, votes_df, league_id):
        df = votes_df.reindex(columns=self.get_columns('Votes'))
        df['league_id'] = league_id
        self.upsert_table('Votes', df)

    def get_votes(self, league_id):
        votes_df = self.get_table('votes', league_id=league_id, drop_league=True)
        return votes_df
        
    def store_members(self, members_df, league_id):
        df = members_df.reindex(columns=self.get_columns('members'))
        df['league_id'] = league_id
        self.upsert_table('members', df)

    def get_results(self, league_id):
        songs_df = self.get_table('results', league_id=league_id, drop_league=True)
        return songs_df

    def get_members(self, league_id):
        members_df = self.get_table('members', league_id=league_id, drop_league=True)
        return members_df

    def get_distances(self, league_id):
        distances_df = self.get_table('pulse', league_id=league_id)[['league_id', 'player_id', 'opponent_id', 'distance']]

        return distances_df

    def get_pulse(self, league_id):
        pulse_df = self.get_table('pulse', league_id=league_id, drop_league=True)
        return pulse_df

    def store_players(self, players_df, league_id=None):
        df = players_df.reindex(columns=self.get_columns('players'))
        self.upsert_table('players', df)

        if league_id:
            # mark inactives
            active_players = players_df['player_id'].to_list()
            self.store_inactive_players(active_players, league_id=league_id)
            
            # store members
            self.store_members(df, league_id)

    def store_inactive_players(self, active_players, league_id=None, reactivate=False):
        if isinstance(active_players, list):
            sql = (f'WITH actives AS (SELECT value AS player_id '
                   f'FROM jsonb_array_elements_text({self.needs_quotes(active_players)})), '
                   f'inactives AS (SELECT player_id FROM {self.table_name("members")} '
                   f'WHERE player_id NOT IN (SELECT player_id FROM actives) '
                   f'AND league_id = {self.needs_quotes(league_id)}) '

                   f'UPDATE {self.table_name("members")} SET inactive = TRUE '
                   f'WHERE player_id IN (SELECT player_id FROM inactives) '
                   f'AND league_id = {self.needs_quotes(league_id)}; '
                   )
            self.execute_sql(sql)

            sql = (f'WITH actives AS (SELECT value AS player_id '
                   f'FROM jsonb_array_elements_text({self.needs_quotes(active_players)})), '
                   f'reactives AS (SELECT player_id FROM actives '
                   f'WHERE player_id NOT IN (SELECT player_id FROM {self.table_name("members")} '
                   f'WHERE league_id = {self.needs_quotes(league_id)})) '

                   f'UPDATE {self.table_name("members")} SET inactive = NULL '
                   f'WHERE player_id IN (SELECT player_id FROM reactives) '
                   f'AND league_id = {self.needs_quotes(league_id)}; '
                   )
            self.execute_sql(sql)

        elif isinstance(active_players, str):
            wheres = f'AND league_id = {league_id}' if league_id else ''
            sql = (f'UPDATE {self.table_name("members")} SET inactive = {self.needs_quotes(not reactivate)} '
                   f'WHERE player_id = {active_players} {wheres};'
                   )

    def get_inactive_players(self, league_id, missed_limit=2):
        sql = (f'SELECT player_id FROM {self.table_name("misses")} '
               f'WHERE league_id = {self.needs_quotes(league_id)} AND missed >= {self.needs_quotes(missed_limit)};'
               )

        player_ids = self.read_sql(sql)['player_id'].to_list()

        return player_ids

    def get_extendable_leagues(self):
        leagues_df = self.get_table('leagues', extendable=True)
        league_ids = leagues_df['league_id'].to_list()

        return league_ids

    def get_players(self):
        players_df = self.get_table('players')

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
            players_df = self.get_players()
            player_ids = players_df.query('player_id != @self.god_id').sort_values(by='player_name', key=lambda s: s.str.lower())['player_id'].to_list()

        return player_ids

    def get_god_id(self):
        return self.god_id

    # Spotify functions
    def store_spotify(self, df, table_name):
        df = df.reindex(columns=self.get_columns(table_name))
        self.upsert_table(table_name, df)

    def store_tracks(self, tracks_df):
        self.store_spotify(tracks_df, 'tracks')
        
    def store_artists(self, artists_df):
        self.store_spotify(artists_df, 'artists')

    def store_albums(self, albums_df):
        self.store_spotify(albums_df, 'albums')

    def store_genres(self, genres_df):
        self.store_spotify(genres_df, 'genres')

    def get_spotify(self, table_name):
        df = self.get_table(table_name)
        return df

    def get_tracks(self):
        df = self.get_spotify('tracks')
        return df

    def get_artists(self):
        df = self.get_spotify('artists')
        return df

    def get_albums(self):
        df = self.get_spotify('albums')
        return df

    def get_genres(self):
        df = self.get_spotify('genres')
        return df

    def get_round_playlist(self, league_id, round_id):
        rounds_df = self.get_table('rounds', league_id=league_id, round_id=round_id)
        playlist_url = rounds_df['playlist_url'].iloc[0]
        
        return playlist_url

    def get_playlists(self, league_id=None):
        playlists_df = self.get_table('playlists', league_id=league_id)
        return playlists_df

    def get_track_count_and_durations(self, league_id):
        durations_df = self.get_table('durations', league_id=league_id)
        count, duration = durations_df[['count', 'duration']].iloc[0]

        return count, duration

    def get_theme_playlists(self, theme):
        # get playlists or track URIs to pull songs from
        playlists_df = self.get_table(f'playlists_{theme}')
     
        return playlists_df

    def store_playlists(self, playlists_df, theme=None):
        df = playlists_df.reindex(columns=self.get_columns('playlists'))
        if theme:
            df['theme'] = theme
        if theme in ['complete', 'best']:
            df['player_id'] = self.god_id

        self.upsert_table('playlists', df)

    def flag_player_image(self, player_id):
        sql = (f'UPDATE {self.table_name("players")} '
               f'SET flagged = {self.needs_quotes(date.today())} '
               f'WHERE player_id = {self.needs_quotes(player_id)};'
               )

        self.execute_sql(sql)

    def get_players_update_sp(self):
        players_df = self.get_table('updates_spotify_players')

        return players_df

    def get_tracks_update_sp(self):
        tracks_df = self.get_table('updates_spotify_tracks')

        return tracks_df
    
    def get_artists_update_sp(self):
        artists_df = self.get_table('updates_spotify_artists')

        return artists_df

    def get_albums_update_sp(self):
        albums_df = self.get_table('updates_spotify_albums')

        return albums_df

    def get_genres_update_sp(self):
        genres_df = self.get_table('updates_spotify_genres')

        return genres_df


    # LastFM functions
    def get_tracks_update_titles(self):
        tracks_df = self.get_table('updates_lastfm_titles')

        return tracks_df

    def get_tracks_update_fm(self):
        tracks_df = self.get_table('updates_lastfm_tracks')

        return tracks_df

    # analytics functions
    def store_optimizations(self, league_id, round_ids=None, optimized=None): 
        optimized_df = DataFrame([[league_id, round_ids, optimized]],
                                 columns=['league_id', 'round_ids', 'optimized'])       
        self.upsert_table('optimizations', optimized_df)

    ######
    def get_optimized(self, league_id):
        optimized_df = self.get_table('optimizations', league_id=league_id)
        optimized = len(optimized_df) != 0

        return optimized

    def get_rankings(self, league_id): ## better way?
        # get rankings sorted by round date
        reindexer = self.get_round_order(league_id)
        rankings_df = self.get_table('rankings', drop_league=True, league_id=league_id)\
            .set_index(['round_id', 'player_id']).sort_values(['round_id', 'points'], ascending=[True, False]).reindex(reindexer, level=0)

        return rankings_df

    def get_round_order(self, league_id): ## what is this used for?
        reindexer = self.get_table('Rounds', columns=['round_id'], league_id=league_id,
                                   order_by={'column': 'created_date', 'sort': 'ASC'})['round_id']

        return reindexer

    def get_boards(self, league_id): ## better way?
        reindexer = self.get_round_order(league_id)
        boards_df = self.get_table('boards', leauge_id=league_id)\
            .pivot(index='player_id', columns='round_id', values='place')
        
        reindexer = [r for r in reindexer if r in boards_df.columns]
        boards_df = boards_df.reindex(columns=reindexer)
        
        return boards_df

    # things that don't require analysis
    def get_audio_features(self, league_id):
        features_df = self.get_table('audio', league_id=league_id)

        return features_df

    def get_dirtiness(self, league_id): ## <- use awards instead
        ##if vote:
        awards_df = self.get_table('awards_leagues', league_id=league_id)
        dirtiness_df = awards_df.set_index('player_id')['dirtiness']
        
        return dirtiness_df

    def get_discovery_scores(self, league_id): ## <- use awards instead
        awards_df = self.get_table('awards_leagues', league_id=league_id)
        discoveries_df = awards_df.set_index('player_id')['discovery']
        
        return discoveries_df

    def get_genre_counts(self, league_id, round_id=None, player_id=None,
                        tags=False, default='other', remove_default=False):

        
        genres_count = self.read_sql(sqls['genre'])
        categories_count = self.read_sql(sqls['category'])

        return genres_count, categories_count

    def get_player_tags(self, league_id, player_id, round_id=None):
        genres_count, _ = self.get_genre_counts(league_id, round_id=round_id, player_id=player_id,
                                             tags=True)

        tags_df = genres_count['genre'].to_list()
        return tags_df

    def get_genres_and_tags(self, league_id, player_id=None): ## <- use get_genre_count instead
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
        exclusives_df = self.get_table('occurances_exclusives_genres')
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
        results_df = self.get_table('top_songs', league_id=league_id)

        return results_df

    def get_round_descriptions(self, league_id):
        descriptions_df = self.get_table('rounds', league_id=league_id)[['round_id', 'description']]

        return descriptions_df

    def get_creators_and_winners(self, league_id):
        creators_winners_df = self.get_table('creators_and_winners', league_id=league_id)

        return creators_winners_df
  
    def get_relationships(self, league_id, player_id):
        relationships_s = self.get_table('relationships', league_id=league_id, player_id=player_id).squeeze()
        return relationships_s
    
    def get_round_awards(self, league_id, round_id=None):
        if round_id:
            round_awards = self.get_table('awards_rounds', league_id=league_id, round_id=round_id)
        else:
            round_awards = self.get_table('awards_leagues', league_id=league_id)
        
        return round_awards

    def get_league_awards(self, league_id):
        league_awards = self.get_table('awards_leagues', league_id=league_id)

        return league_awards

    def get_awards(self, league_id, player_id=None, round_id=None):
        god_mode = player_id == self.get_god_id()
             
        if round_id:
            awards_df = self.get_round_awards(league_id, round_id)
        else:
            awards_df = self.get_league_awards(league_id)

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
        places_df = self.get_table('battles', league_id=league_id)
        
        return places_df

    def get_round_placement(self, league_id, player_id=None):
        if player_id:
            places_df = self.get_table('boards', league_id=league_id, place_id=player_id, place=1)
        else:
            places_df = self.get_table('boards', league_id=league_id)

        return places_df

    def get_competition_placement(self, league_id, competition_id=None, player_id=None):
        competition_id = self.get_current_competition(league_id)
        if player_id:
            places_df = self.get_table('boards', league_id=league_id, competition_id=competition_id, player_id=player_id)
        else:
            places_df = self.get_table('boards', league_id=league_id, competition_id=competition_id)
        return places_df

    def get_current_competition(self, league_id):
        competitions_df = self.get_table('competitions_status', league_id=league_id, current=True)
        
        if len(competitions_df):
            competition_id = competitions_df['competition_id'].squeeze()
        else:
            competition_id = None

        return competition_id

    def get_badge(self, league_id, player_id, competition=None, competition_id=None):
        if (not competition) and (not competition_id):
            places_df = self.get_league_placement(league_id, player_id=player_id)
            n_players = len(self.get_members(league_id=league_id))

        else:
            if not competition_id:
                competition_id = self.get_current_competition(league_id)
            
            if competition_id:
                places_df = self.get_competition_placement(league_id, competition_id=competition_id, player_id=player_id)
                n_players = len(self.get_)
            else:
                places_df = None

        if (places_df is not None) and len(places_df):
            badge = places_df['place'].iloc[0]
            
        else:
            badge = None

        return badge, n_players

    def get_competitions(self, league_id):
        competitions_df = self.get_table('competitons', league_id=league_id)

        return competitions_df

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

    def update_competitions(self):
        ''' add sequential non-bonus rounds if there is a started, unfinished competition '''
        sql = (# add new rounds
               f'WITH '
               f'cs AS ('
               f'SELECT c.league_id, c.competition_id, MIN(r.created_date) AS start_date '
               f'FROM {self.table_name("competitions")} AS c '
               f'JOIN {self.table_name("rounds")} AS r ON c.round_ids ? r.round_id '
               f'WHERE c.finished IS NOT TRUE '
               f'GROUP BY c.competition_id), '
               
               f'js AS ('
               f'SELECT cs.competition_id, r.league_id, r.round_id '
               f'FROM {self.table_name("rounds")} AS r '
               f'JOIN cs ON r.league_id = cs.league_id '
               f'WHERE r.created_date >= cs.start_date AND r.bonus IS NOT TRUE), '

               f'up AS ('
               f'SELECT js.competition_id AS cid, jsonb_agg(js.round_id) AS round_ids '
               f'FROM js GROUP BY js.competition_id) '
               
               f'UPDATE {self.table_name("competitions")} '
               f'SET round_ids = up.round_ids FROM up '
               f'WHERE competition_id = up.cid; '
               
               # remove bonus rounds
               f'WITH '
               f'bo AS ('
               f'SELECT c.league_id, c.competition_id, '
               f'jsonb_array_elements_text(jsonb_agg(b.round_id)) AS bonus_rounds '
               f'FROM {self.table_name("competitions")} AS c '
               f'JOIN {self.table_name("rounds")} AS b ON c.league_id = b.league_id '
               f'WHERE b.bonus = TRUE GROUP BY c.competition_id, c.league_id), '

               f'new_values AS ('
               f'SELECT c.competition_id AS cid, '
               f'c.round_ids - array_agg(bo.bonus_rounds) AS new_round_ids '
               f'FROM {self.table_name("competitions")} AS c '
               f'JOIN bo ON c.league_id = bo.league_id '
               f'AND c.competition_id = bo.competition_id '
               f'GROUP BY c.competition_id, bo.league_id) '

               f'UPDATE {self.table_name("competitions")} '
               f'SET round_ids = new_values.new_round_ids '
               f'FROM new_values WHERE competition_id = new_values.cid;'
               )

        self.execute_sql(sql)

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