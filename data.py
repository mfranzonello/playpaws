from datetime import date
import json
from PIL.ImageFont import load_default

from sqlalchemy import create_engine
from pandas import read_sql, DataFrame, isnull, concat
from pandas.api.types import is_numeric_dtype

from jason import Jason

class Database:
    tables = {# MusicLeague data
              'Leagues': {'keys': ['league'], 'values': ['creator', 'date', 'url', 'path']},
              'Players': {'keys': ['player'], 'values': ['username', 'src', 'uri', 'followers']},
              'Rounds': {'keys': ['league', 'round'], 'values': ['creator', 'date', 'status', 'url', 'path']},
              'Songs': {'keys': ['league', 'song_id'], 'values': ['round', 'artist', 'title', 'submitter', 'track_url']},    
              'Votes': {'keys': ['league', 'player', 'song_id'], 'values': ['vote']},

              ##'Playlists': {'keys': ['url'], 'values': []},

              # Spotify data
              'Tracks': {'keys': ['url'], 'values': ['uri', 'name', 'artist_uri', 'album_uri', 'explicit', 'popularity', 'duration',
                                                     'danceability', 'energy', 'key', 'loudness', #'mode',
                                                     'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']},
              'Artists': {'keys': ['uri'], 'values': ['name', 'genres', 'popularity', 'followers']},
              'Albums': {'keys': ['uri'], 'values': ['name', 'genres', 'popularity', 'release_date']},
              'Genres': {'keys': ['name'], 'values': []},
              
              # analytics
              'Members': {'keys': ['league', 'player'], 'values': ['x', 'y', 'wins', 'dfc', 'likes', 'liked']},
              'Rankings': {'keys': ['league', 'round', 'player'], 'values': ['points', 'score']},
              'Boards': {'keys': ['league', 'round', 'player'], 'values': ['place']},
              'Analyses': {'keys': ['league'], 'values': ['date', 'open', 'closed', 'version']},
              
              # settings
              'Weights': {'keys': ['parameter', 'version'], 'values': ['value']},
              
              # other 
              ##'Images': {'keys': ['player'], 'values': ['array']},
              }
   
    def __init__(self, credentials, structure={}):
        self.language = credentials.get('language')

        if self.language == 'sqlite':
            self.db = credentials['db_name']
            engine_string = f'sqlite:///{self.db}.db'
        elif self.language == 'postgres':
            self.db = f'"{credentials["username"]}/{credentials["db_name"]}"'
            engine_string = f'postgresql://{credentials["username"]}{credentials["add_on"]}:{credentials["password"]}@{credentials["host"]}'
        else:
            self.db = ''
            engine_string = ''

        self.directories = structure

        self.keys = {table_name: self.tables[table_name]['keys'] for table_name in self.tables}
        self.values = {table_name: self.tables[table_name]['values'] for table_name in self.tables}
        self.columns = {table_name: self.tables[table_name]['keys'] + self.tables[table_name]['values'] for table_name in self.tables}
 
        print(f'Connecting to database {self.db}...')
        self.engine = create_engine(engine_string)
        self.connection = self.engine.connect()
        print(f'\t...success!')

        self.jason = Jason()

    def table_name(self, table_name:str) -> str:
        if self.language == 'sqlite':
            full_table_name = table_name
        elif self.language == 'postgres':
            full_table_name = f'{self.db}."{table_name.lower()}"'
        else:
            full_table_name = ''

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
        is_json = isinstance(item, (list, dict, set)) or self.jason.is_json(item)
        return is_json

    def needs_quotes(self, item) -> str:
        # put quotes around strings to account for special characters
        if self.language == 'sqlite':
            char = '"'
        elif self.language == 'postgres':
            char = "'"
        else:
            char = ''

        if self.quotable(item):
            if (self.language == 'postgres') and self.datable(item):
                quoted = char + str(item) + char + '::date'
            elif (self.language == 'postgres') and self.jsonable(item):
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
            if self.language == 'sqlite':
                updates = []
                for i in df.index:
                    df_row = df.loc[i]
                    key_values = df_row[keys].values
                    columns = df_row.drop(keys).index
                    values = df_row.drop(keys).values
                    sets = ', '.join(f'{column} = ' + self.needs_quotes(value) for column, value in zip(columns, values))
                    wheres = ' AND '.join(f'({key} = ' + self.needs_quotes(key_value) + ')' for key, key_value in zip(keys, key_values))
                    updates.append(f'UPDATE {self.table_name(table_name)} SET {sets} WHERE {wheres};')
                sql = ' '.join(updates)

            elif self.language == 'postgres':
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
        links = ['(' + ' & '.join(f'({key} == ' + self.needs_quotes(key_value) + ')' \
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

    ### NEED TO FIX
    def get_keys(self, table_name):
        if self.language == 'sqlite':
            keys = read_sql(f'PRAGMA TABLE_INFO({table_name})', self.connection).query('pk > 0')['name']
        elif self.language == 'postgres':
            keys = self.keys[table_name]
        else:
            keys = []

        return keys

    ### NEED TO FIX
    def get_values(self, table_name, match=None):
        if self.language == 'sqlite':
            values = read_sql(f'PRAGMA TABLE_INFO({table_name})', self.connection).query('pk == 0')['name']
        elif self.language == 'postgres':
            values = self.values[table_name]
        else:
            values = []

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

    def get_song_ids(self, league_title:str, round_title, artists:list, titles:list) -> list:
        # first check for which songs already exists
        ids_df = self.get_table('Songs', league=league_title).drop(columns='league')
        merge_cols = ['artist', 'title', 'round']
        songs_df = DataFrame(data=zip(artists, titles, [round_title]*len(artists)), columns=merge_cols).merge(ids_df, on=merge_cols, how='left')[merge_cols + ['song_id']]
        
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

    def get_url(self, url_type, league_title, round_title=None):
        # return specific URL
        table_name = {'league': 'Leagues',
                      'round': 'Rounds'}[url_type]

        wheres = f'(league = {self.needs_quotes(league_title)})'
        if round_title is not None:
            wheres += f' AND (round = {self.needs_quotes(round_title)})'
    
        sql = f'SELECT url FROM {self.table_name(table_name)} WHERE {wheres}'
        urls = read_sql(sql, self.connection)['url'].values
        url = urls[0] if len(urls) else None

        # see if url exists
        if url is not None:
            # if file is stored locally, return URL with full directory
            if url[:len('http')] != 'http':
                # local file
                page_type = 'round' if (round_title is not None) else 'league'
                directory = self.directories['local'].get(page_type)

            else:
                # web URL
                directory = self.directories['web'].get('main_url')

            if url is not None:
                url = f'{directory}/{url}'.replace('//','/')
        
        return url

    def get_urls(self, url_type, directory):
        # return list of URLs for a type
        table_name = {'league': 'Leagues',
                      'round': 'Rounds'}[url_type]
        sql = f'SELECT url FROM {self.table_name(table_name)}'
        urls = read_sql(sql, self.connection)['url'].values

        urls = [f'{directory}/{url}' for url in urls]
        return urls

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
        ##joins = f'AS m JOIN {self.table_name("Songs")} AS f ON m.song_id = f.song_id'
        ##wheres = f'(f.round = {self.needs_quotes(round_title)}) AND (m.player IS NULL)' 
        ##sql = f'DELETE FROM {self.table_name("Votes")} {joins} WHERE {wheres}'
        ##self.execute_sql(sql)
        return
        
    def store_members(self, members_df, league_title):
        df = members_df.reindex(columns=self.store_columns('Members'))
        df['league'] = league_title
        self.upsert_table('Members', df)

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
        table_name = 'Weights'
        weights = read_sql(f'SELECT * FROM {self.table_name(table_name)} WHERE version = {version}', self.connection, index_col='parameter')['value']
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

    def store_analysis(self, league_title, version, statuses):
        today = date.today()
        analyses_df = DataFrame([[league_title, today, version,
                                  statuses['open'], statuses['closed']]],
                                columns=['league', 'date', 'version', 'open', 'closed'])

        self.upsert_table('Analyses', analyses_df)

    #def store_analyses(self, results):

    ##def get_analysis(self, league_title):
    ##    analyses_df = self.get_table('Analyses', league=league_title) #, order_by={'column': 'date', 'sort': 'DESC'})
        
    ##    if len(analyses_df):
    ##        results = self.jason.from_json(analyses_df['results'].iloc[0])

    ##    return results

    def get_analyses(self):
        analyses_df = self.get_table('Analyses', order_by={'other': 'Leagues',
                                                           'on': ['league'],
                                                           'column': 'date',
                                                           'sort': 'ASC'})
        ##sql = f'SELECT m.* FROM {self.table_name("Analyses")} AS m JOIN {self.table_name("Leagues")} AS f on f.league = m.league ORDER BY f.date ASC'
        ##analyses_df = read_sql(sql, self.connection)
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
    def get_dirtiness(self, league_title, vote):
        if vote:
            sql = (f'SELECT '
                   f'count(CASE WHEN t.explicit THEN 1 END) / '
                   f'count(CASE WHEN NOT t.explicit THEN 1 END)::real AS dirtiness '
                   f'FROM {self.table_name("Votes")} AS v '
                   f'LEFT JOIN {self.table_name("Songs")} AS s '
                   f'ON (v.song_id = s.song_id) AND (v.league = s.league)'
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
                   f'WHERE s.league = {self.needs_quotes(league_title)};'
                   )
        else:
            sql = (f'SELECT '
                   f'count(CASE WHEN t.explicit THEN 1 END) / '
                   f'count(CASE WHEN NOT t.explicit THEN 1 END)::real AS dirtiness '
                   f'FROM {self.table_name("Songs")} AS s '
                   f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
                   f'WHERE s.league = {self.needs_quotes(league_title)};'
                   )

        dirtiness = read_sql(sql, self.connection)['dirtiness'].iloc[0]

        return dirtiness

    def get_audio_features(self, league_title, round_title):
        values = ['duration', 'danceability', 'energy', 'key', 'loudness', #'mode',
                  'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo']
        methods = ['MIN', 'AVG', 'MAX']
        jsons = ', '.join('json_build_object(' +
                          ', '.join(f'{self.needs_quotes(method)}, {method}(s.{k})' for method in methods) +
                          f') AS {k}' for k in values)

        sql = (f'SELECT s.round, {jsons} '
               f'FROM {self.table_name("Songs")} AS s '
               f'LEFT JOIN {self.table_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.round;'
               )

        features_df = read_sql(sql, self.connection)

        return features_df

    def get_discoveries(self, league_title):
        sql = (f'SELECT s.song_id, 1-t.popularity/100::real AS discovery '
               f'FROM {self.talbe_name("Songs")} AS s '
               f'LEFT JOIN {self.talbe_name("Tracks")} AS t ON s.track_url = t.url '
               f'WHERE s.league = {self.needs_quotes(league_title)};'
               )

        discoveries_df = read_sql(sql, self.connection)

        return discoveries_df

    def get_genres(self, leauge_title):
        sql = (f'SELECT s.round, json_agg(DISTINCT g.name) AS genres '
               f'FROM {self.talbe_name("Songs")} AS s '
               f'LEFT JOIN {self.talbe_name("Tracks")} AS t '
               f'ON s.track_url = t.url '
               f'LEFT JOIN {self.talbe_name("Artists")} AS a '
               f'ON t.artist_uri ? a.uri '
               f'LEFT JOIN {self.talbe_name("Genres")} AS g '
               f'ON a.genres ? g.name '
               f'WHERE (s.league = {self.needs_quotes(league_title)}) AND (g.name IS NOT NULL) '
               f'GROUP BY s.round;'
               )

        genres_df = read_sql(sql, self.connection)

        return genres_df

    def get_all_artists(league_title):
        sql = (f'SELECT s.league, s.song_id, json_agg(DISTINCT a.name) as arist '
               f'FROM {self.talbe_name("Songs")} AS s '
               f'LEFT JOIN {self.talbe_name("Tracks")}AS t ON s.track_url = t.url '
               f'LEFT JOIN {self.talbe_name("Artists")} AS a ON t.artist_uri ? a.uri '
               f'WHERE s.league = {self.needs_quotes(league_title)} '
               f'GROUP BY s.song_id, s.league;'
               )

        all_artists_df = read_sql(sql, self.connection)

        return all_artists_df

    def get_all_info():
        sql = (f'SELECT q.league, x.song_id, q.round, x.artist, q.title, q.submitter FROM '
               f'(SELECT s.league, s.song_id, json_agg(DISTINCT a.name) as artist '
               f'FROM {self.talbe_name("Songs")} AS s '
               f'LEFT JOIN {self.talbe_name("Tracks")} AS t ON s.track_url = t.url '
               f'LEFT JOIN {self.talbe_name("Artists")} AS a ON t.artist_uri ? a.uri '
               f'GROUP BY s.song_id, s.league) x '
               f'LEFT JOIN {self.talbe_name("Songs")} AS q '
               f'ON (x.song_id = q.song_id) AND (x.league = q.league);'
               )

        all_info_df = read_sql(sql, self.connection)

        return all_info_df
        

    ###def get_artists_by_vote(self):
    ###    pass
        '''
        Getting artists voted for by a player

        SELECT DISTINCT m.player, q.name, v.artist, v.title FROM "mfranzonello/playpaws"."votes" AS m
        LEFT JOIN "mfranzonello/playpaws"."songs" AS v ON m.song_id = v.song_id
    
        LEFT JOIN "mfranzonello/playpaws"."tracks" AS f ON v.track_url = f.url
        LEFT JOIN "mfranzonello/playpaws"."artists" AS q ON f.artist_uri ? q.uri
  
        WHERE (m.league LIKE 'Play%') AND (v.league LIKE 'Play%') AND (m.player LIKE 'Michael%');
        '''

        '''
        Getting top genres voted for by a player

        SELECT r.name, count(r.name) FROM "mfranzonello/playpaws"."votes" AS m
          LEFT JOIN "mfranzonello/playpaws"."songs" AS v ON m.song_id = v.song_id
    
          LEFT JOIN "mfranzonello/playpaws"."tracks" AS f ON v.track_url = f.url
          LEFT JOIN "mfranzonello/playpaws"."artists" AS q ON f.artist_uri ? q.uri
          LEFT JOIN "mfranzonello/playpaws"."genres" AS r ON q.genres ? r.name
          WHERE (m.league LIKE 'Brad%') AND (v.league LIKE 'Brad%')
            AND (m.player LIKE 'Chris%') AND (r.name IS NOT NULL)
        GROUP BY r.name ORDER BY count(r.name) DESC LIMIT 5;
        '''

        
