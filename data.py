from datetime import date
import json
from difflib import SequenceMatcher
from os import getenv, getcwd

from sqlalchemy import create_engine
from pandas import read_sql, DataFrame, isnull
from pandas.api.types import is_numeric_dtype
import streamlit as st

from streaming import streamer

print(f"PATH: {getcwd()}")

class Database:
    tables = {# MusicLeague data
              'Leagues': {'keys': ['league'], 'values': ['creator', 'date', 'url']},
              'Players': {'keys': ['player'], 'values': ['username', 'src', 'uri', 'followers']},
              'Rounds': {'keys': ['league', 'round'], 'values': ['creator', 'date', 'status', 'url', 'playlist_url']},
              'Songs': {'keys': ['league', 'song_id'], 'values': ['round', 'artist', 'title', 'submitter', 'track_url']},    
              'Votes': {'keys': ['league', 'player', 'song_id'], 'values': ['vote']},

              'Playlists': {'keys': ['url'], 'values': ['title', 'src']},

              # Spotify data
              'Tracks': {'keys': ['url'], 'values': ['uri', 'name', 'title', 'artist_uri', 'album_uri', 'explicit', 'popularity',
                                                     'duration', 'key', 'mode', 'loudness', 'tempo',
                                                     'danceability', 'energy', 'liveness', 'valence',
                                                     'speechiness', 'acousticness', 'instrumentalness',
                                                     'scrobbles', 'listeners', 'top_tags']},
              'Artists': {'keys': ['uri'], 'values': ['name', 'genres', 'popularity', 'followers', 'src']},
              'Albums': {'keys': ['uri'], 'values': ['name', 'genres', 'popularity', 'release_date', 'src']},
              'Genres': {'keys': ['name'], 'values': ['category']},
              
              # analytics
              'Members': {'keys': ['league', 'player'], 'values': ['x', 'y', 'wins', 'dfc', 'likes', 'liked']},
              'Results': {'keys': ['league', 'song_id'], 'values': ['people', 'closed', 'discovery', 'points']},
              'Rankings': {'keys': ['league', 'round', 'player'], 'values': ['points', 'score']},
              'Boards': {'keys': ['league', 'round', 'player'], 'values': ['place']},
              'Analyses': {'keys': ['league'], 'values': ['date', 'open', 'closed', 'version']},
              
              # settings
              'Weights': {'keys': ['parameter', 'version'], 'values': ['value']},
              'Images': {'keys': ['keyword'], 'values': ['src']}, 
              }
   
    def __init__(self, main_url):
        self.db = f'"{getenv("BITIO_USERNAME")}/{getenv("BITIO_DBNAME")}"'
        engine_string = f'postgresql://{getenv("BITIO_USERNAME")}{getenv("BITIO_ADD_ON")}:{getenv("BITIO_PASSWORD")}@{getenv("BITIO_HOST")}'
        
        self.main_url = main_url

        self.keys = {table_name: self.tables[table_name]['keys'] for table_name in self.tables}
        self.values = {table_name: self.tables[table_name]['values'] for table_name in self.tables}
        self.columns = {table_name: self.tables[table_name]['keys'] + self.tables[table_name]['values'] for table_name in self.tables}
 
        self.connection = self.connect(engine_string)

        streamer.print(f'\t...success!')
        
    @st.cache(allow_output_mutation=True)
    def connect(self, engine_string):
        streamer.print(f'Connecting to database {self.db}...')
        
        engine = create_engine(engine_string)
        connection = engine.connect()
        return connection
        
    def __hash__(self):
        return hash(self.engine_string, self.main_url)

    def table_name(self, table_name:str) -> str:
        full_table_name = f'{self.db}."{table_name.lower()}"'

        return full_table_name

    def get_table(self, table_name, columns=None, league=None, order_by=None):
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
        if league is None:
            # return all values
            wheres = ''
        else:
            # return only league values
            wheres = f' WHERE {m_}league = {self.needs_quotes(league)}'

        if order_by:
            orders = f' ORDER BY {f_}{order_by["column"]} {order_by["sort"]}'
        else:
            orders = ''

        # write and execute SQL
        sql = f'SELECT {cols} FROM {self.table_name(table_name)}{joins}{wheres}{orders};'
        table = read_sql(sql, self.connection, coerce_float=True)        

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
        char = "'"

        if self.quotable(item):
            if self.datable(item):
                quoted = char + str(item) + char + '::date'
            elif self.jsonable(item):
                quoted = char + json.dumps(item).replace(char, char*2) + char + '::jsonb'
            else:
                quoted = char + str(item).replace(char, char*2) + char
        elif self.nullable(item):
            quoted = 'NULL'
        else:
            quoted = str(item)

        return quoted

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
        links = ['(' + ' & '.join(f'({key} ' + ('!=' if isnull(key_value) else '==') + ' ' + (f'{key}' if isnull(key_value) else self.needs_quotes(key_value)) + ')' \
            for key, key_value in zip(keys, df_existing[keys].loc[i])) + ')' \
            for i in df_existing.index]
        
        if len(df_existing):
            # there is already data in the database
            sql_up = ' | '.join(links)
            sql_in = f'~({sql_up})'

            df_updates = df_store.query(sql_up)
            df_inserts = df_store.query(sql_in)
        else:
            # all data is new
            df_updates = df_store.reindex([])
            df_inserts = df_store.reindex()
        
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
        for s in sql.split(';'):
            if len(s):
                self.connection.execute(s.strip())

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
            if (table_name == 'Leagues') or ('league' not in self.keys[table_name]):
                league = None
            else:
                league = df_store['league'].iloc[0]

            # get existing ids in database
            df_existing = self.get_table(table_name, columns=keys, league=league)

            # split dataframe into existing updates and new inserts
            df_updates, df_inserts = self.find_existing(df_store, df_existing, keys)

            # write SQL for updates and inserts
            sql_updates = self.update_rows(table_name, df_updates, keys)
            sql_inserts = self.insert_rows(table_name, df_inserts)

            # write combined SQL
            sql = ' '.join(s for s in [sql_updates, sql_inserts] if len(s))

            # execute SQL
            self.execute_sql(sql)

    def get_player_match(self, league_title=None, partial_name=None, url=None):
        # find closest name
        if url or (league_title and partial_name):
            # something to search for
            if url:
                # search by URL
                wheres = f'url = {self.needs_quotes(url)}'
            else:
                # search by name and league
                like_name = f'{partial_name}%%'
                wheres = f'(league = {self.needs_quotes(league_title)}) AND (player LIKE {self.needs_quotes(like_name)})'

            sql = f'SELECT player FROM {self.table_name("Members")} WHERE {wheres}'

            names_df = read_sql(sql, self.connection)
            if len(names_df):
                # potential matches
                if partial_name in names_df['player'].values:
                    # the name is an exact match
                    matched_name = partial_name
                else:
                    # return the first name match
                    matched_name = names_df['player'].iloc[0]
            else:
                # no name found
                matched_name = None
        else:
            matched_name = None

        return matched_name

    def get_song_ids(self, league_title:str, round_title:str, track_urls:list) -> list:
        # first check for which songs already exists
        ids_df = self.get_table('Songs', league=league_title).drop(columns='league')
        merge_cols = ['track_url', 'round']
        songs_df = DataFrame(data=zip(track_urls, [round_title]*len(track_urls)), columns=merge_cols).merge(ids_df, on=merge_cols, how='left')[merge_cols + ['song_id']]
        
        # then fill in songs that are new
        n_retrieve = songs_df['song_id'].isna().sum()
        if n_retrieve:
            new_ids = self.get_new_song_ids(league_title, n_retrieve)
            songs_df.loc[songs_df['song_id'].isna(), 'song_id'] = new_ids
            
        song_ids = songs_df['song_id'].values

        return song_ids

    def get_new_song_ids(self, league_title, n_retrieve):
        # get next available song_ids
        existing_song_ids = self.get_table('Songs', columns=['song_id'], league=league_title)['song_id']
        max_song_id = 0 if existing_song_ids.empty else existing_song_ids.max()
        next_song_ids = [song_id for song_id in range(1, max_song_id + n_retrieve + 1) \
            if song_id not in existing_song_ids.values][0:n_retrieve]
        return next_song_ids

    def store_columns(self, table_name):
        columns = self.columns[table_name]
        return columns

    def get_leagues(self):
        # get league names
        leagues_df = self.get_table('Leagues', order_by={'column': 'date', 'sort': 'ASC'})
        return leagues_df

    def store_leagues(self, leagues_df):
        # store league names
        df = leagues_df.reindex(columns=self.store_columns('Leagues'))
        self.upsert_table('Leagues', df)

    def get_league_creator(self, league_title):
        # get name of league creator
        sql = f'SELECT creator FROM {self.table_name("Leagues")} WHERE league = {self.needs_quotes(league_title)}'
        creators_df = read_sql(sql, self.connection)
        if len(creators_df):
            creator = creators_df['creator'].iloc[0]
        else:
            creator = None
        return creator

    def check_data(self, league_title, round_title=None):
        # see if there is any data for the league or round
        wheres = f'league = {self.needs_quotes(league_title)}'
        if round_title is not None:
            wheres = f'({wheres}) AND (round = {self.needs_quotes(round_title)})'
            count = 'round'
            table_name = 'Rounds'
        else:
            count = 'league'
            table_name = 'Leagues'

        sql = f'SELECT COUNT({count}) FROM {self.table_name(table_name)} WHERE {wheres}'

        count_df = read_sql(sql, self.connection)
        check = count_df['count'].gt(0).all()

        return check

    def get_rounds(self, league):
        rounds_df = self.get_table('Rounds', league=league, order_by={'column': 'date', 'sort': 'ASC'}).drop(columns='league')
        return rounds_df

    def get_url_status(self, url):
        table_name = 'Rounds'
        sql = f'SELECT league, round FROM {self.table_name(table_name)} WHERE url = {self.needs_quotes(url)}'
        results = read_sql(sql, self.connection)

        if len(results):
            league, round_title = results.iloc[0]
        else:
            league, round_title = [None, None]

        return league, round_title

    def get_round_status(self, league_title, round_title):
        if (league_title is None) and (round_title is None):
            round_status = 'n/a'
        else:
            table_name = 'Rounds'
            sql = f'SELECT * FROM {self.table_name(table_name)} WHERE (league = {self.needs_quotes(league_title)}) AND (round = {self.needs_quotes(round_title)})' 
            status_df = read_sql(sql, self.connection)

            if len(status_df) and (not isnull(status_df['status'].iloc[0])):
                round_status = status_df['status'].iloc[0] # ['new', 'open', 'closed']
            else:
                round_status = 'missing'

        return round_status

    def store_round(self, league_title, round_title, new_status, url=None):
        df = DataFrame([[league_title, round_title, new_status, url]], columns=['league', 'round', 'status', 'url'])
        self.upsert_table('Rounds', df)

    def store_rounds(self, rounds_df, league_title):
        df = rounds_df.reindex(columns=self.store_columns('Rounds'))
        df['league'] = league_title
        if 'status' not in rounds_df.columns:
            df = df.drop(columns='status') # only store status if it is there
        self.upsert_table('Rounds', df)

    def store_songs(self, songs_df, league):
        df = songs_df.reindex(columns=self.store_columns('Songs'))
        df['league'] = league
        self.upsert_table('Songs', df)

    def get_songs(self, league_title):
        songs_df = self.get_table('Songs', league=league_title).drop(columns='league')
        return songs_df

    def get_song_urls(self):
        # get just the URLS for all songs
        songs_df = self.get_table('Songs', columns=['track_url'])
        return songs_df

    def store_votes(self, votes_df, league):
        df = votes_df.reindex(columns=self.store_columns('Votes'))
        df['league'] = league
        self.upsert_table('Votes', df)

    def get_votes(self, league):
        votes_df = self.get_table('Votes', league=league).drop(columns='league')
        return votes_df

    def drop_votes(self, league_title, round_title):
        # remove placeholder votes when a round closes
        sql = (f'DELETE FROM {self.table_name("Votes")} AS v '
               f'WHERE (v.player IS NULL) AND (v.song_id IN '
               f'(SELECT s.song_id FROM {self.table_name("Songs")} AS s '
               f'WHERE (s.league = {self.needs_quotes(league_title)}) '
               f'AND (s.round = {self.needs_quotes(round_title)})));'
               )

        self.execute_sql(sql)
        
    def store_members(self, members_df, league_title):
        df = members_df.reindex(columns=self.store_columns('Members'))
        df['league'] = league_title
        self.upsert_table('Members', df)

    def get_results(self, league_title):
        songs_df = self.get_table('Results', league=league_title).drop(columns='league')
        return songs_df

    def store_results(self, songs_df, league_title):
        df = songs_df.reindex(columns=self.store_columns('Results'))
        df['league'] = league_title
        self.upsert_table('Results', df)

    def get_members(self, league_title):
        members_df = self.get_table('Members', league=league_title).drop(columns='league')
        return members_df

    def store_players(self, players_df, league_title=None):
        df = players_df.reindex(columns=self.store_columns('Players'))
        self.upsert_table('Players', df)

        if league_title:
            self.store_members(df, league_title)

    def get_players(self):
        players_df = self.get_table('Players')
        return players_df

    def get_player_names(self, league_title):
        members_df = self.get_members(league_title)
        player_names = members_df['player'].to_list()
        return player_names

    def get_weights(self, version):
        sql = (f'SELECT DISTINCT ON(s.parameter) s.version, s.parameter, s.value '
               f'FROM (SELECT * FROM {self.table_name("Weights")} '
               f'WHERE version >= FLOOR({version}) ORDER BY version DESC) AS s;'
               )

        weights = read_sql(sql, self.connection, index_col='parameter')['value']
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

    def get_mask(self, league_title, default='<default>'):
        masks_df = self.get_table('Images')
        masks_df['ratio'] = masks_df['keyword'].apply(lambda x: self.get_mask_ratio(league_title, x, default))
        max_ratio = masks_df['ratio'].max()
        if max_ratio == 0:
            mask = masks_df[masks_df['keyword'] == default]['src']
        else:
            mask = masks_df[masks_df['ratio'] == max_ratio]['src']

        mask_src = mask.iloc[0].replace('/embed?', '/download?')

        return mask_src

    def get_mask_ratio(self, league_title, keyword, default):
        if (keyword == default) or (keyword.lower() not in league_title.lower()):
            ratio = 0
        else:
            ratio = SequenceMatcher(None, league_title, keyword).quick_ratio()
        return ratio

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

    def get_players_update_sp(self):
        sql = (f'SELECT username FROM {self.table_name("Players")} '
               f'WHERE (src IS NULL) OR (uri IS NULL) OR (followers IS NULL);')

        players_df = read_sql(sql, self.connection)

        return players_df


    def get_tracks_update_sp(self):
        sql = (f'SELECT DISTINCT track_url AS url FROM {self.table_name("Songs")} '
               f'WHERE track_url NOT IN '
               f'(SELECT url FROM {self.table_name("Tracks")})'
               )

        tracks_df = read_sql(sql, self.connection)

        return tracks_df
    
    def get_artists_update_sp(self):
        sql = (f'SELECT t.a_uri as uri FROM '
               f'(SELECT DISTINCT jsonb_array_elements(artist_uri)->>0 AS a_uri '
               f'FROM {self.table_name("Tracks")}) AS t '
               f'WHERE t.a_uri NOT IN (SELECT uri FROM {self.table_name("Artists")});'
               )

        artists_df = read_sql(sql, self.connection)

        return artists_df

    def get_albums_update_sp(self):
        sql = (f'SELECT DISTINCT album_uri AS uri FROM {self.table_name("Tracks")} '
               f'WHERE album_uri NOT IN (SELECT uri FROM {self.table_name("Albums")})'
               )

        albums_df = read_sql(sql, self.connection)

        return albums_df

    def get_genres_update_sp(self):
        sql = (f'SELECT u.genre FROM (SELECT DISTINCT '
               f'jsonb_array_elements(genres)->>0 AS genre '
               f'FROM {self.table_name("Artists")} '
               f'UNION SELECT jsonb_array_elements(genres)->>0 AS genre '
               f'FROM {self.table_name("Albums")}) AS u '
               f'WHERE u.genre NOT IN (SELECT name FROM {self.table_name("Genres")});'
               )

        genres_df = read_sql(sql, self.connection)

        return genres_df


    # LastFM functions
    def get_tracks_update_fm(self):
        sql = (f'SELECT t.url, t.name AS unclean, t.title, a.name AS artist, '
               f't.scrobbles, t.listeners, t.top_tags '
               f'FROM {self.table_name("Tracks")} as t '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON (t.artist_uri->>0) = a.uri '
               f'WHERE (t.title IS NULL) OR ((t.scrobbles IS NULL) AND (t.listeners IS NULL) AND (t.top_tags IS NULL));'
               )

        tracks_df = read_sql(sql, self.connection)

        return tracks_df

    # analytics functions
    def store_analysis(self, league_title, version, statuses):
        today = date.today()
        analyses_df = DataFrame([[league_title, today, version,
                                  statuses['open'], statuses['closed']]],
                                columns=['league', 'date', 'version', 'open', 'closed'])

        self.upsert_table('Analyses', analyses_df)

    #def store_analyses(self, results):

    def get_analyses(self):
        analyses_df = self.get_table('Analyses', order_by={'other': 'Leagues',
                                                           'on': ['league'],
                                                           'column': 'date',
                                                           'sort': 'ASC'})
        return analyses_df

    def store_rankings(self, rankings_df, league_title):
        df = rankings_df.reset_index().reindex(columns=self.store_columns('Rankings'))
        df['league'] = league_title
        self.upsert_table('Rankings', df)

    def get_rankings(self, league_title):
        # get rankings sorted by round date
        reindexer = self.get_round_order(league_title)
        rankings_df = self.get_table('Rankings', league=league_title).drop(columns='league')\
            .set_index(['round', 'player']).sort_values(['round', 'points'], ascending=[True, False]).reindex(reindexer, level=0)

        return rankings_df

    def get_round_order(self, league_title):
        reindexer = self.get_table('Rounds', columns=['round'], league=league_title, order_by={'column': 'date',
                                                                                               'sort': 'ASC'})['round']

        return reindexer

    def store_boards(self, boards_df, league_title):
        df = boards_df.reset_index().melt(id_vars='player',
                                          value_vars=boards_df.columns,
                                          var_name='round',
                                          value_name='place').dropna(subset=['place']).reindex(columns=self.store_columns('Boards'))
        df['league'] = league_title

        self.upsert_table('Boards', df)

    def get_boards(self, league_title):
        reindexer = self.get_round_order(league_title)
        boards_df = self.get_table('Boards', league=league_title, order_by={'other': 'Rounds', 
                                                                            'on': ['league', 'round'],
                                                                            'column': 'date',
                                                                            'sort': 'ASC'}).drop(columns='league')\
            .pivot(index='player', columns='round', values='place')
        
        reindexer = [r for r in reindexer if r in boards_df.columns]
        boards_df = boards_df.reindex(columns=reindexer)
        
        return boards_df

    # things that don't require analysis
    def get_dirtiness(self, league_title, vote=False):
        if vote:
            gb = 'player'
            sql = (f'SELECT v.player, '
                   f'count(CASE WHEN t.explicit THEN 1 END) / '
                   f'count(CASE WHEN NOT t.explicit THEN 1 END)::real AS dirtiness '
                   f'FROM {self.table_name("Votes")} AS v '
                   f'LEFT JOIN {self.table_name("Songs")} AS s '
                   f'ON (v.song_id = s.song_id) AND (v.league = s.league)'
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
                   f'WHERE s.league = {self.needs_quotes(league_title)} '
                   f'GROUP BY v.player;'
                   )
        else:
            gb = 'submitter'
            sql = (f'SELECT s.submitter, '
                   f'count(CASE WHEN t.explicit THEN 1 END) / '
                   f'count(CASE WHEN NOT t.explicit THEN 1 END)::real AS dirtiness '
                   f'FROM {self.table_name("Songs")} AS s '
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
                   f'WHERE s.league = {self.needs_quotes(league_title)} '
                   f'GROUP BY s.submitter;'
                   )

        dirtiness = read_sql(sql, self.connection).set_index(gb)['dirtiness']

        return dirtiness

    def get_audio_features(self, league_title, json=False, methods=None):
        values = ['duration', 'danceability', 'energy', 'key', 'loudness', #'mode',
                  'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']

        if not methods:
            methods = ['MIN', 'AVG', 'MAX']

        if json:
            jsons = ', '.join('json_build_object(' +
                              ', '.join(f'{self.needs_quotes(method)}, {method}(t.{k})' for method in methods) +
                              f') AS {k}' for k in values)

        else:
            jsons = ', '.join(f'{method}(t.{k}) AS {method}_{k}' for method in methods for k in values)

            sql = (f'SELECT s.round, {jsons} '
                   f'FROM {self.table_name("Songs")} AS s '
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
                   f'LEFT JOIN "mfranzonello/playpaws"."rounds" AS r ON s.round = r.round '
                   f'WHERE s.league = {self.needs_quotes(league_title)} '
                   f'GROUP BY s.round, r.date ORDER BY r.date;'
                   )

        features_df = read_sql(sql, self.connection)

        return features_df

    def get_discoveries(self, league_title, base=1000):
        sql = (f'SELECT s.round, s.song_id, '
               f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.round, s.song_id;'
               )

        discoveries_df = read_sql(sql, self.connection)

        return discoveries_df

    def get_discovery_scores(self, league_title, base=1000):
        sql = (f'SELECT s.submitter, '
               f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery, '
               f'AVG(t.popularity::real/100) AS popularity '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.submitter;'
               )

        discoveries_df = read_sql(sql, self.connection).set_index('submitter')

        return discoveries_df

    def get_genres_and_tags(self, league_title):
        sql = (f'SELECT a.genres, t.top_tags AS tags '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league = {self.needs_quotes(league_title)};'
               )

        genres_df = read_sql(sql, self.connection)

        return genres_df

    def get_song_results(self, league_title):
        sql = (f'SELECT s.round, s.song_id, '
               f't.title, ttt.artist, b.release_date, r.closed, r.points '
               f'FROM {self.table_name("Results")} AS r '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON (s.league = r.league) AND (s.song_id = r.song_id) '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Albums")} AS b '
               f'ON t.album_uri = b.uri '
               f'LEFT JOIN '
               f'(SELECT tt.url, json_agg(a.name) AS artist FROM '
               f'(SELECT jsonb_array_elements(artist_uri) AS a_uri, url '
               f'FROM {self.table_name("Tracks")}) AS tt '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON tt.a_uri ? a.uri '
               f'GROUP BY tt.url) AS ttt ON t.url = ttt.url '
               f'LEFT JOIN {self.table_name("Leagues")} AS l '
               f'ON s.league = l.league '
               f'LEFT JOIN {self.table_name("Rounds")} AS d '
               f'ON (s.league = d.league) AND (s.round = d.round) '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'ORDER BY l.date ASC, d.date ASC, r.points DESC;'
               )

        results_df = read_sql(sql, self.connection).drop_duplicates(subset='song_id')

        return results_df

    def get_all_artists(self, league_title):
        sql = (f'SELECT s.league, s.song_id, json_agg(DISTINCT a.name) as arist '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")}AS t ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Artists")} AS a ON t.artist_uri ? a.uri '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.song_id, s.league;'
               )

        all_artists_df = read_sql(sql, self.connection)

        return all_artists_df

    def get_all_info(self):
        sql = (f'SELECT q.league, x.song_id, q.round, x.artist, q.title, q.submitter FROM '
               f'(SELECT s.league, s.song_id, json_agg(DISTINCT a.name) as artist '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Artists")} AS a ON t.artist_uri ? a.uri '
               f'GROUP BY s.song_id, s.league) x '
               f'LEFT JOIN {self.table_name("Songs")} AS q '
               f'ON (x.song_id = q.song_id) AND (x.league = q.league);'
               )

        all_info_df = read_sql(sql, self.connection)

        return all_info_df
