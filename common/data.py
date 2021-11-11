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
        self.engine_string = (f'postgresql://{get_secret("BITIO_USERNAME")}{get_secret("BITIO_ADD_ON")}'
                              f':{get_secret("BITIO_PASSWORD")}@{get_secret("BITIO_HOST")}')

    @cache(allow_output_mutation=True, max_entries=10, ttl=10800)
    def connect(self):  
        engine = create_engine(self.engine_string)
        connection = engine.connect()
        return connection

class Database(Streamable):
    tables = {# MusicLeague data
              'Leagues': {'keys': ['league'],
                          'values': ['creator', 'date', 'url']},
              'Players': {'keys': ['username'],
                          'values': ['player', 'src', 'uri', 'followers', 'flagged']},
              'Rounds': {'keys': ['league', 'round'],
                         'values': ['creator', 'date', 'status', 'url', 'playlist_url', 'description', 'capture']},
              'Songs': {'keys': ['league', 'song_id'],
                        'values': ['round', 'submitter', 'track_url']},    
              'Votes': {'keys': ['league', 'player', 'song_id'],
                        'values': ['vote']},

              'Playlists': {'keys': ['league', 'theme'],
                            'values': ['uri', 'src', 'rounds']},

              # Spotify data
              'Tracks': {'keys': ['url'],
                         'values': ['uri', 'name', 'title', 'mix', 'artist_uri', 'album_uri', 'explicit', 'popularity',
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
              'Members': {'keys': ['league', 'player'],
                          'values': ['x', 'y', 'wins', 'dfc', 'likes', 'liked']},
              'Pulse': {'keys': ['league', 'p1', 'p2'],
                        'values': ['distance']},
              'Results': {'keys': ['league', 'song_id'],
                          'values': ['people', 'votes', 'closed', 'discovery', 'points']},
              'Rankings': {'keys': ['league', 'round', 'player'],
                           'values': ['points', 'score']},
              'Boards': {'keys': ['league', 'round', 'player'],
                         'values': ['place']},
              'Competitions': {'keys': ['league', 'competition'],
                               'values': ['start', 'finish']},
              'Analyses': {'keys': ['league'],
                           'values': ['date', 'open', 'closed', 'version']},
              
              # settings
              'Weights': {'keys': ['parameter', 'version'],
                          'values': ['value']},
              }

    def __init__(self, main_url, streamer=None):
        super().__init__()
        self.db = f'"{get_secret("BITIO_USERNAME")}/{get_secret("BITIO_DBNAME")}"'
        ##engine_string = (f'postgresql://{get_secret("BITIO_USERNAME")}{get_secret("BITIO_ADD_ON")}'
        ##                 f':{get_secret("BITIO_PASSWORD")}@{get_secret("BITIO_HOST")}')
        
        self.main_url = main_url
        self.add_streamer(streamer)
        
        self.keys = {table_name: self.tables[table_name]['keys'] for table_name in self.tables}
        self.values = {table_name: self.tables[table_name]['values'] for table_name in self.tables}
        self.columns = {table_name: self.tables[table_name]['keys'] + self.tables[table_name]['values'] for table_name in self.tables}
 
        self.streamer.print(f'Connecting to database {self.db}...')

        engineer = Engineer()
        self.connection = engineer.connect()
        self.streamer.print(f'\t...success!')
        
    def use_one_league(self, table_name):
        use_one = ('league' in self.get_keys(table_name)) and self.tables[table_name].get('use_one_league', True)
        return use_one
   
    def table_name(self, table_name:str) -> str:
        full_table_name = f'{self.db}."{table_name.lower()}"'

        return full_table_name

    def read_sql(self, sql, **kwargs):
        return read_sql(sql, self.connection, **kwargs)

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
        table = self.read_sql(sql, coerce_float=True)        

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
            if ('league' in self.get_keys(table_name)) and (len(df_store['league'].unique()) == 1):
                league = df_store['league'].iloc[0]
            else:
                league = None
            
            # get existing ids in database
            df_existing = self.get_table(table_name, columns=keys, league=league)

            # split dataframe into existing updates and new inserts
            df_updates, df_inserts = self.find_existing(df_store, df_existing, keys)

            # write SQL for updates and inserts
            sql_updates = self.update_rows(table_name, df_updates, keys)
            sql_inserts = self.insert_rows(table_name, df_inserts)         

            # execute SQL
            self.execute_sql(sql_updates)
            self.execute_sql(sql_inserts)

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

            names_df = self.read_sql(sql)
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
        creators_df = self.read_sql(sql)
        if len(creators_df):
            creator = creators_df['creator'].iloc[0]
        else:
            creator = None
        return creator

    def get_player_leagues(self, player_name):
        sql = (f'SELECT league FROM {self.table_name("Members")} '
               f'WHERE player = {self.needs_quotes(player_name)};'
               )
               
        league_titles = self.read_sql(sql)['league'].values

        return league_titles

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

        count_df = self.read_sql(sql)
        check = count_df['count'].gt(0).all()

        return check

    def get_rounds(self, league):
        rounds_df = self.get_table('Rounds', league=league, order_by={'column': 'date', 'sort': 'ASC'}).drop(columns='league')
        return rounds_df

    def get_url_status(self, url):
        table_name = 'Rounds'
        sql = f'SELECT league, round FROM {self.table_name(table_name)} WHERE url = {self.needs_quotes(url)}'
        results = self.read_sql(sql)

        if len(results):
            league, round_title = results.iloc[0]
        else:
            league, round_title = [None, None]

        return league, round_title

    def get_round_status(self, league_title, round_title):
        if (league_title is None) and (round_title is None):
            round_status = 'n/a'
        else:
            sql = (f'SELECT * FROM {self.table_name("Rounds")} '
                   f'WHERE (league = {self.needs_quotes(league_title)}) AND (round = {self.needs_quotes(round_title)});'
                   )
            status_df = self.read_sql(sql)

            if len(status_df) and (not isnull(status_df['status'].iloc[0])):
                round_status = status_df['status'].iloc[0] # ['new', 'open', 'closed']
            else:
                round_status = 'missing'

        return round_status

    def get_uncreated_rounds(self, league_title):
        sql = (f'SELECT round, description, creator FROM {self.table_name("Rounds")} '
               f'WHERE (league = {self.needs_quotes(league_title)}) AND (creator IS NULL);')

        rounds_df = self.read_sql(sql)

        return rounds_df

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

    def store_results(self, results_df, league_title):
        df = results_df.reindex(columns=self.store_columns('Results'))
        df['league'] = league_title
        self.upsert_table('Results', df)

    def get_members(self, league_title):
        members_df = self.get_table('Members', league=league_title).drop(columns='league')
        return members_df

    def store_pulse(self, pulse_df, league_title):
        df = pulse_df.reindex(columns=self.store_columns('Pulse'))
        df['league'] = league_title
        self.upsert_table('Pulse', df)

    def get_pulse(self, league_title):
        pulse_df = self.get_table('Pulse', league=league_title).drop(columns='league')
        return pulse_df

    def store_players(self, players_df, league_title=None):
        df = players_df.reindex(columns=self.store_columns('Players'))
        self.upsert_table('Players', df)

        if league_title:
            self.store_members(df, league_title)

    def get_players(self):
        players_df = self.get_table('Players')
        return players_df

    def get_player_names(self, league_title=None):
        if league_title:
            members_df = self.get_members(league_title)
            player_names = members_df['player'].to_list()

        else:
            player_names = self.get_players()['player'].sort_values(key=lambda s: s.str.lower()).to_list()
        return player_names

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

    def get_round_playlist(self, league_title, round_title):
        sql = (f'SELECT playlist_url AS url FROM {self.table_name("Rounds")} '
               f'WHERE (league = {self.needs_quotes(league_title)}) AND (round = {self.needs_quotes(round_title)}) '
               f'AND (playlist_url IS NOT NULL);')

        playlist_df = self.read_sql(sql)
        if len(playlist_df):
            playlist_url = playlist_df['url'].iloc[0]
        else:
            playlist_url = None

        return playlist_url

    def get_playlists(self, league_title=None):
        playlists_df = self.get_table('Playlists', league=league_title)
        return playlists_df

    def get_track_count(self, league_title):
        sql = (f'SELECT COUNT(DISTINCT track_url) FROM {self.table_name("Songs")} '
               f'WHERE league = {self.needs_quotes(league_title)};'
               )

        count = self.read_sql(sql)['count'].iloc[0]

        return count

    def get_track_durations(self, league_title):
        sql = (f'SELECT SUM(t.duration) AS duration FROM '
               f'(SELECT DISTINCT track_url FROM {self.table_name("Songs")} '
               f'WHERE league = {self.needs_quotes(league_title)}) as s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url;'
               )

        duration = self.read_sql(sql)['duration'].iloc[0]

        return duration

    def get_theme_playlists(self, theme):
        # get playlists or track URIs to pull songs from
        if theme == 'complete':
            # all songs
            sql = (f'SELECT league, round, playlist_url AS url FROM {self.table_name("Rounds")} '
                   f'WHERE playlist_url IS NOT NULL '
                   f'ORDER BY date;'
                   )

            wheres = f'theme = {self.needs_quotes(theme)}'
            
        elif theme == 'best':
            # based on performance
            sql = (f'SELECT s.league, s.round, t.url AS uri, r.points, d.date FROM {self.table_name("Results")} as r '
                   f'LEFT JOIN {self.table_name("Songs")} as s ON (r.league = s.league) AND (r.song_id = s.song_id) '
                   f'LEFT JOIN {self.table_name("Tracks")} as t ON s.track_url = t.url '
                   f'LEFT JOIN {self.table_name("Rounds")} as d ON (s.league = d.league) AND (s.round = d.round) '
                   f'WHERE d.status = {self.needs_quotes("closed")};'
                   )

            wheres = f'theme = {self.needs_quotes(theme)}'

        elif theme == 'favorite':
            # player favorite
            sql = (f'SELECT * FROM (SELECT s.league, s.round, t.uri, v.player, v.vote, d.date '
                   f'FROM {self.table_name("Votes")} as v '
                   f'LEFT JOIN {self.table_name("Songs")} as s ON (v.league = s.league) AND (v.song_id = s.song_id) '
                   f'LEFT JOIN {self.table_name("Tracks")} as t ON s.track_url = t.url '
                   f'LEFT JOIN {self.table_name("Rounds")} AS d ON (s.league = d.league) AND (s.round = d.round) '
                   f'UNION SELECT s.league, s.round, t.uri, s.submitter as player, -1 as vote,  d.date '
                   f'FROM {self.table_name("Songs")} as s '
                   f'LEFT JOIN {self.table_name("Tracks")} as t ON s.track_url = t.url '
                   f'LEFT JOIN {self.table_name("Rounds")} AS d ON (s.league = d.league) AND (s.round = d.round)) AS q '
                   f'WHERE q.player IS NOT NULL;'
                   )

            wheres = f'theme LIKE {self.needs_quotes(theme+"%%")}'

        rounds_df = self.read_sql(sql)

        # get comprehensive playlists
        selects = ', theme' if theme == 'favorite' else ''

        sql = (f'SELECT league, uri, src, rounds{selects} FROM {self.table_name("Playlists")} '
               f'WHERE {wheres};'
               )

        playlists_df = self.read_sql(sql)
     
        return rounds_df, playlists_df

    def store_playlists(self, playlists_df, theme='all'):
        df = playlists_df.reindex(columns=self.store_columns('Playlists'))
        if theme in ['complete', 'best']:
            df['theme'] = theme

        self.upsert_table('Playlists', df)

    def flag_player_image(self, player_name):
        sql = (f'UPDATE {self.table_name("Players")} '
               f'SET flagged = {self.needs_quotes(date.today())} '
               f'WHERE player = {self.needs_quotes(player_name)};'
               )

        self.execute_sql(sql)

    def get_players_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['src', 'uri', 'followers'])
        sql = (f'SELECT username FROM {self.table_name("Players")} '
               f'WHERE {wheres} OR (flagged <= {self.needs_quotes(date.today())});')

        players_df = self.read_sql(sql)

        return players_df


    def get_tracks_update_sp(self):
        wheres = ' OR '.join(f'({v} IS NULL)' for v in ['uri', 'name', 'artist_uri', 'album_uri',
                                                        'explicit', 'popularity', 'duration',
                                                        'key', 'mode', 'loudness', 'tempo',
                                                        'danceability', 'energy', 'liveness', 'valence',
                                                        'speechiness', 'acousticness', 'instrumentalness'])
        sql = (f'SELECT DISTINCT track_url AS url FROM {self.table_name("Songs")} '
               f'WHERE track_url NOT IN '
               f'(SELECT url FROM {self.table_name("Tracks")}) '
               f'UNION SELECT url FROM {self.table_name("Tracks")} WHERE {wheres};'
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
        sql = (f'SELECT t.url, t.name AS unclean, t.title, t.mix, a.name AS artist, '
               f't.scrobbles, t.listeners, t.top_tags '
               f'FROM {self.table_name("Tracks")} as t '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON (t.artist_uri->>0) = a.uri WHERE {wheres};'
               )

        tracks_df = self.read_sql(sql)

        return tracks_df

    # analytics functions
    def store_analysis(self, league_title, version, statuses=None, optimized=None):
        today = date.today()
        d = {'league': [league_title],
             'date': [today],
             'version': [version]}

        if statuses:
            d['open'] = [statuses['open']]
            d['closed'] = [statuses['closed']]

        if optimized is not None:
            d['optimized'] = [optimized]

        analyses_df = DataFrame(d)
        
        self.upsert_table('Analyses', analyses_df)

    #def store_analyses(self, results):

    def get_analyses(self):
        analyses_df = self.get_table('Analyses', order_by={'other': 'Leagues',
                                                           'on': ['league'],
                                                           'column': 'date',
                                                           'sort': 'ASC'})
        return analyses_df
    
    def get_analyzed(self, league_title, open_rounds, closed_rounds, version):
        sql = (f'SELECT COUNT(*) FROM {self.table_name("Analyses")} '
               f'WHERE (league = {self.needs_quotes(league_title)}) '
               f'AND (open = {self.needs_quotes(open_rounds)}) '
               f'AND (closed = {self.needs_quotes(closed_rounds)}) '
               f'AND (version = {version}::real);'
               )

        analyzed = self.read_sql(sql)['count'].iloc[0] > 0

        return analyzed

    def get_optimized(self, league_title):
        sql = (f'SELECT COUNT(*) FROM {self.table_name("Analyses")} '
               f'WHERE (league = {self.needs_quotes(league_title)}) '
               f'AND (optimized = TRUE);'
               )

        optimized = self.read_sql(sql)['count'].iloc[0] > 0

        return optimized

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
                   f'AVG(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS dirtiness '
                   f'FROM {self.table_name("Votes")} AS v '
                   f'LEFT JOIN {self.table_name("Songs")} AS s '
                   f'ON (v.song_id = s.song_id) AND (v.league = s.league) '
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

        dirtiness = self.read_sql(sql).set_index(gb)['dirtiness']

        return dirtiness

    def get_audio_features(self, league_title, json=False, methods=None):
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

            sql = (f'SELECT s.round, {jsons} '
                   f'FROM {self.table_name("Songs")} AS s '
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
                   f'LEFT JOIN {self.table_name("Rounds")} AS r ON s.round = r.round '
                   f'WHERE s.league = {self.needs_quotes(league_title)} '
                   f'GROUP BY s.round, r.date ORDER BY r.date;'
                   )

        features_df = self.read_sql(sql)

        return features_df

    def get_discoveries(self, league_title, base=1000):
        sql = (f'SELECT s.round, s.song_id, '
               f'AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) AS discovery '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.round, s.song_id;'
               )

        discoveries_df = self.read_sql(sql)

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

        discoveries_df = self.read_sql(sql).set_index('submitter')

        return discoveries_df

    def get_genres_and_tags(self, league_title, player_name=None):
        if player_name:
            wheres = f' AND s.submitter = {self.needs_quotes(player_name)}'
        else:
            wheres = ''

        sql = (f'SELECT a.genres, t.top_tags AS tags '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league = {self.needs_quotes(league_title)}{wheres};'
               )

        genres_df = self.read_sql(sql)
        
        if player_name:
            genres_df = set(genres_df.sum().sum())

        return genres_df

    def get_exclusive_genres(self, league_title):
        sql = (f'SELECT q.tag FROM '
               f'(SELECT jsonb_array_elements(a.genres) as tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'UNION '
               f'SELECT jsonb_array_elements(t.top_tags) AS tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)}) AS q '
               f'WHERE q.tag NOT IN '
               f'(SELECT jsonb_array_elements(a.genres) as tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'WHERE s.league != {self.needs_quotes(league_title)} '
               f'UNION '
               f'SELECT jsonb_array_elements(t.top_tags) AS tag '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'WHERE s.league != {self.needs_quotes(league_title)});'
               )

        exclusives = self.read_sql(sql)['tag']

        return exclusives

    def get_song_results(self, league_title):
        sql = (f'SELECT s.round, s.song_id, s.submitter, '
               f't.title, ttt.artist, b.release_date, b.src, r.closed, r.points, d.status '
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

        results_df = self.read_sql(sql).drop_duplicates(subset='song_id')

        return results_df

    def get_round_descriptions(self, league_title):
        descriptions_df = self.get_table('Rounds', columns=['round', 'description'],
                                         league=league_title, order_by={'column': 'date',
                                                                        'sort': 'ASC'})

        return descriptions_df

    def get_creators_and_winners(self, league_title):
        sql = (f'SELECT r.round, r.creator, jsonb_agg(b.player) AS winner '
               f'FROM {self.table_name("Rounds")} as r '
               f'LEFT JOIN {self.table_name("Boards")} as b '
               f'ON (r.league = b.league) AND (r.round = b.round) '
               f'WHERE (r.league = {self.needs_quotes(league_title)}) '
               f'AND ((b.place < 2) OR (b.place IS NULL)) ' ## can this be MIN without GROUP BY?
               f'GROUP BY r.round, r.creator, r.date '
               f'ORDER BY r.date;'
               )
        
        creators_winners_df = self.read_sql(sql)

        return creators_winners_df

    def get_all_artists(self, league_title):
        sql = (f'SELECT s.league, s.song_id, json_agg(DISTINCT a.name) as arist '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")}AS t ON s.track_url = t.url '
               f'LEFT JOIN {self.table_name("Artists")} AS a ON t.artist_uri ? a.uri '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.song_id, s.league;'
               )

        all_artists_df = self.read_sql(sql)

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

        all_info_df = self.read_sql(sql)

        return all_info_df

    def get_player_pulse(self, league_title, player_name):
        sql = (f'SELECT p.player, p.likes, p.liked, j.closest '
               f'FROM (SELECT player, likes, liked  '
               f'FROM {self.table_name("Members")} '
               f'WHERE (league = {self.needs_quotes(league_title)}) '
               f'AND (player = {self.needs_quotes(player_name)})) as p '

               f'CROSS JOIN '

               f'(SELECT p2 AS closest FROM {self.table_name("Pulse")} '
               f'WHERE (league = {self.needs_quotes(league_title)}) '
               f'AND (p1 = {self.needs_quotes(player_name)}) '
               f'AND (distance IN (SELECT MIN(distance) '
               f'FROM "mfranzonello/playpaws"."pulse" '
               f'WHERE (league = {self.needs_quotes(league_title)}) '
               f'AND (p1 = {self.needs_quotes(player_name)}) '
               f'GROUP BY league, p1)) LIMIT 1) AS j '

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

    def get_player_wins(self, league_title, player_name):
        sql = (f'SELECT round FROM {self.table_name("Boards")} '
               f'WHERE (player = {self.needs_quotes(player_name)}) '
               f'AND (league = {self.needs_quotes(league_title)}) AND (place = 1);'
               )

        player_wins_df = self.read_sql(sql)

        return player_wins_df

    def get_competition_wins(self, league_title, player_name):
        competition_titles = self.get_competitions(league_title)['competition'].unique()
        competition_wins = [competition for competition in competition_titles \
            if self.get_badge(league_title, player_name, competition_title=competition) == 1]

        if not len(competition_wins):
            competition_wins = None

        return competition_wins

    def get_awards(self, league_title, player_name, base=1000):
        ## Note that Discoverer, Dirtiest, etc should be based on MAX/MIN and not LIMIT 1
        sql = (f'SELECT (p.popular = 1) AS popular, (q.discoverer = 1) AS discoverer, '
               f'(r.dirtiest = 1) as dirtiest, (z.generous > 0.5) AS generous, (n.clean = 0) AS clean, '
               f'j.win_rate, k.play_rate '
               
               f'FROM '
               f'(SELECT v.popular FROM (SELECT s.submitter, RANK() OVER '
               f'(ORDER BY AVG(t.popularity::real/100) DESC) AS popular '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} ' 
               f'GROUP BY s.submitter) AS v '
               f'WHERE v.submitter = {self.needs_quotes(player_name)}) as p '

               f'CROSS JOIN'
               f'(SELECT u.discoverer FROM (SELECT s.submitter, RANK() OVER '
               f'(ORDER BY AVG(1/LOG({base}, GREATEST({base}, t.scrobbles))) DESC) AS discoverer '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} ' 
               f'GROUP BY s.submitter) AS u '
               f'WHERE u.submitter = {self.needs_quotes(player_name)}) as q '

               f'CROSS JOIN '
               f'(SELECT w.dirtiest FROM (SELECT s.submitter, RANK() OVER '
               f'(ORDER BY AVG(CASE WHEN t.explicit THEN 1 ELSE 0 END) DESC) AS dirtiest '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.submitter) AS w '
               f'WHERE w.submitter = {self.needs_quotes(player_name)}) AS r '

               f'CROSS JOIN '
               f'(SELECT w.clean FROM (SELECT s.submitter, '
               f'SUM(CASE WHEN t.explicit THEN 1 ELSE 0 END) AS clean '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.submitter) AS w '
               f'WHERE w.submitter = {self.needs_quotes(player_name)}) AS n '

               f'CROSS JOIN '
               f'(SELECT q.wins / p.total AS win_rate FROM '
               f'(SELECT COUNT(round)::real AS wins FROM {self.table_name("Boards")} '
               f'WHERE (player = {self.needs_quotes(player_name)}) '
               f'AND (league = {self.needs_quotes(league_title)}) AND (place = 1)) AS q '
               f'CROSS JOIN '
               f'(SELECT COUNT(round)::real AS total FROM {self.table_name("Boards")} '
               f'WHERE (player = {self.needs_quotes(player_name)}) '
               f'AND (league = {self.needs_quotes(league_title)})) AS p) AS j '

               f'CROSS JOIN '
               f'(SELECT q.plays / p.total AS play_rate FROM '
               f'(SELECT COUNT(round)::real AS plays FROM {self.table_name("Boards")} '
               f'WHERE (player = {self.needs_quotes(player_name)}) '
               f'AND (league = {self.needs_quotes(league_title)}) AND (place > 0)) AS q '
               f'CROSS JOIN '
               f'(SELECT COUNT(round)::real AS total FROM {self.table_name("Rounds")} '
               f'WHERE (league = {self.needs_quotes(league_title)}) '
               f'AND (status = {self.needs_quotes("closed")})) AS p) AS k '

               f'CROSS JOIN '
               f'(SELECT p.player, AVG(CASE WHEN p.votes/q.songs::real > r.avg_generosity THEN 1 ELSE 0 END) AS generous '
               f'FROM (SELECT v.player, count(v.player) AS votes, s.round, s.league '
               f'FROM {self.table_name("Votes")} AS v '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON (s.league = v.league) AND (s.song_id = v.song_id) '
               f'WHERE v.player IS NOT NULL '
               f'GROUP BY s.league, s.round, v.player) AS p '
               f'LEFT JOIN '
               f'(SELECT count(s.song_id) AS songs, s.round FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Rounds")} AS d '
               f'ON s.round = d.round '
               f'WHERE d.status = {self.needs_quotes("closed")} '
               f'GROUP BY s.league, s.round) AS q ON p.round = q.round ' 
               f'LEFT JOIN (SELECT p.round, AVG(p.votes/q.songs::real) AS avg_generosity '
               f'FROM (SELECT v.player, COUNT(v.player) AS votes, s.round FROM {self.table_name("Votes")} AS v '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON (s.league = v.league) AND (s.song_id = v.song_id) '
               f'WHERE v.player IS NOT NULL '
               f'GROUP BY s.league, s.round, v.player) AS p '
               f'LEFT JOIN (SELECT count(s.song_id) AS songs, s.round FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Rounds")} AS d '
               f'ON s.round = d.round WHERE d.status = {self.needs_quotes("closed")} '
               f'GROUP BY s.league, s.round) AS q '
               f'ON p.round = q.round GROUP BY p.round) AS r '
               f'ON p.round = r.round '
               f'WHERE p.player = {self.needs_quotes(player_name)} AND p.league = {self.needs_quotes(league_title)} '
               f'GROUP BY p.player) AS z '

               f';'
               )

        awards_df = self.read_sql(sql).squeeze(0)

        return awards_df

    def get_badge(self, league_title, player_name, competition=None, competition_title=None):
        if (not competition) and (not competition_title):
            sql = (f'SELECT q.badge FROM '
                   f'(SELECT player, RANK() OVER (ORDER BY wins DESC) AS badge '
                   f'FROM {self.table_name("Members")} '
                   f'WHERE league = {self.needs_quotes(league_title)}) AS q '
                   f'WHERE q.player = {self.needs_quotes(player_name)};'
                   )
        else:
            if not competition_title:
                competition_title = self.get_current_competition(league_title)
            
            if competition_title:
                sql = (f'SELECT q.badge FROM (SELECT r.player, RANK() '
                       f'OVER(ORDER BY SUM(r.points) DESC) AS badge '
                       f'FROM {self.table_name("Rounds")} AS d '
                       f'RIGHT JOIN {self.table_name("Competitions")} AS c '
                       f'ON d.league = c.league AND d.date >= c.start '
                       f'AND d.date <= (CASE WHEN c.finish IS NOT NULL THEN c.finish ELSE '
                       f'(SELECT MAX(date) FROM {self.table_name("Rounds")} '
                       f'WHERE c.league = {self.needs_quotes(league_title)} '
                       f'AND status = {self.needs_quotes("closed")}) END) '
                       f'RIGHT JOIN {self.table_name("Rankings")} AS r '
                       f'ON d.league = r.league AND d.round = r.round '
                       f'WHERE c.league = {self.needs_quotes(league_title)} '
                       f'AND c.competition = {self.needs_quotes(competition_title)} '
                       f'GROUP BY r.player) AS q WHERE q.player = {self.needs_quotes(player_name)};'
                       )
            else:
                sql = None

        if sql:
            badge = self.read_sql(sql).squeeze()
        else:
            badge = None

        return badge

    def get_competitions(self, league_title):
        sql = (f'SELECT c.competition, d.round '
               f'FROM {self.table_name("Rounds")} AS d '
               f'RIGHT JOIN {self.table_name("Competitions")} AS c '
               f'ON d.league = c.league '
               f'AND d.date >= c.start AND d.date <= (CASE WHEN c.finish IS NOT NULL '
               f'THEN c.finish ELSE (SELECT MAX(date) FROM {self.table_name("Rounds")} '
               f'WHERE c.league = {self.needs_quotes(league_title)} '
               f'AND status = {self.needs_quotes("closed")}) END) '
               f'WHERE c.league = {self.needs_quotes(league_title)} '
               f'ORDER BY d.date;'
               )

        competitions_df = self.read_sql(sql)

        return competitions_df

    def get_current_competition(self, league_title):
        sql = (f'SELECT competition FROM {self.table_name("Competitions")} '
               f'WHERE start <= CURRENT_DATE and (finish >= CURRENT_DATE OR finish IS NULL) '
               f'LIMIT 1;' )

        competition_title = self.read_sql(sql)
        
        if len(competition_title):
            competition_title = competition_title.squeeze()
        else:
            competition_title = None

        return competition_title

    def get_competition_results(self, league_title, competition_title=None):
        if competition_title is None:
            # get current competition
            competition_title = self.get_current_competition(league_title)

        if competition_title:
            sql = (f'SELECT r.player, RANK() '
                   f'OVER(ORDER BY SUM(r.points) DESC) AS place '
                   f'FROM {self.table_name("Rounds")} AS d '
                   f'RIGHT JOIN {self.table_name("Competitions")} AS c '
                   f'ON d.league = c.league AND d.date >= c.start '
                   f'AND d.date <= (CASE WHEN c.finish IS NOT NULL THEN c.finish ELSE '
                   f'(SELECT MAX(date) FROM {self.table_name("Rounds")} '
                   f'WHERE c.league = {self.needs_quotes(league_title)} '
                   f'AND status = {self.needs_quotes("closed")}) END) '
                   f'RIGHT JOIN {self.table_name("Rankings")} AS r '
                   f'ON d.league = r.league AND d.round = r.round '
                   f'WHERE c.league = {self.needs_quotes(league_title)} '
                   f'AND c.competition = {self.needs_quotes(competition_title)} '
                   f'GROUP BY r.player;'
                   )
               
            results_df = self.read_sql(sql)

        else:
            results_df = None

        return results_df

    def get_hoarding(self, league_title):
        sql = (f'SELECT q.round, q.player, q.votes/p.total::real*n.players/(n.players-1) AS pct '
               f'FROM (SELECT s.round, v.player, COUNT(v.song_id) AS votes '
               f'FROM {self.table_name("Votes")} AS v '
               f'LEFT JOIN {self.table_name("Songs")} AS s '
               f'ON v.song_id = s.song_id AND v.league = s.league '
               f'WHERE v.league = {self.needs_quotes(league_title)} AND v.player IS NOT NULL '
               f'GROUP BY s.round, v.player) AS q '
               f'LEFT JOIN (SELECT round, COUNT(song_id) AS total '
               f'FROM {self.table_name("Songs")} WHERE league = {self.needs_quotes(league_title)} '
               f'GROUP BY round) AS p ON q.round = p.round '
               f'LEFT JOIN (SELECT round, COUNT(DISTINCT submitter) AS players '
               f'FROM {self.table_name("Songs")} WHERE league = {self.needs_quotes(league_title)} '
               f'GROUP BY round) AS n ON q.round = n.round;'
               )

        hoarding_df = self.read_sql(sql).pivot(index='player', columns='round', values='pct')

        reindexer = [c for c in self.get_round_order(league_title) if c in hoarding_df.columns]
        hoarding_df = hoarding_df.reindex(columns=reindexer)
                
        return hoarding_df