from os import getlogin
from dateutil.parser import parse, ParserError
from datetime import date

from sqlalchemy import create_engine
from pandas import read_sql, DataFrame

class Database:
    keys = {'Leagues': ['league'],
            'Players': ['url'],
            'Members': ['league', 'player'],
            'Rounds': ['league', 'round'],
            'Songs': ['league', 'song_id'],    
            'Votes': ['league', 'player', 'song_id'],
            'Weights': ['parameter'],
            'Artists': ['uri'],
            'Tracks': ['uri'],
            'Albums': ['uri'],
            }

    values = {'Leagues': ['creator', 'date', 'url', 'path'],
              'Players': ['player'],
              'Rounds': ['creator', 'date', 'status', 'url', 'path'],
              'Member': ['x', 'y'],
              'Songs': ['round', 'artist', 'title', 'submitter'],
              'Votes': ['vote'],
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

        self.keys = Database.keys
        self.values = Database.values
        self.columns = {table_name: self.keys[table_name] + self.values.get(table_name, []) for table_name in self.keys}
        
        print(f'Connecting to database {self.db}')
        self.engine = create_engine(engine_string)
        self.connection = self.engine.connect()

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

        # check if column specific
        if columns is None:
            # return full table
            cols = '*'
        else:
            # return only specific rows, like finding keys
            cols = ', '.join(columns)

        # check if league specific
        if league is None:
            # return all values
            wheres = ''
        else:
            # return only league values
            wheres = f' WHERE league = {self.needs_quotes(league)}'

        if order_by:
            orders = f' ORDER BY {order_by[0]} {order_by[1]}'
        else:
            orders = ''

        # write and execute SQL
        sql = f'SELECT {cols} FROM {self.table_name(table_name)}{wheres}{orders};'
        table = read_sql(sql, self.connection, coerce_float=True)

        return table

    def quotable(self, item):
        # add SQL appropriate quotes to string variables
        try_string = str(item)
        quotable = not ((try_string in ['True', 'False']) or try_string.isnumeric())# or isinstance(item, date))
        return quotable

    def datable(self, item):
        # add cast information to date values
        if isinstance(item, date):
            is_date = True
        else:
            try:
                parse(str(item))
                is_date = True
            except ParserError:
                is_date = False

        return is_date

    def needs_quotes(self, item) -> str:
        # put quotes around strings to account for special characters
        if self.language == 'sqlite':
            char = '"'
        elif self.language == 'postgres':
            char = "'"
            add_on = '::date' if self.datable(item) else ''
        else:
            char = ''

        if self.quotable(item):
            quoted = char + str(item).replace(char, char*2) + char + add_on
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

    def execute_sql(self, sql):
        for s in sql.split(';'):
            if len(s):
                self.connection.execute(s.strip())

    def upsert_table(self, table_name, df):
        # update existing rows and insert new rows
        keys = self.get_keys(table_name)

        # only store columns that have values, so as to not overwrite with NA
        df_store = df.dropna(axis='columns', how='all')

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
        if url:
            wheres = f'url = {self.needs_quotes(url)}'
        else:
            like_name = f'{partial_name}%'
            wheres = f'player LIKE {self.needs_quotes(partial_name)}'

        sql = f'SELECT player FROM {self.table_name("Members")} WHERE {wheres}'

        names_df = read_sql(sql, self.connection)
        if len(names_df):
            if partial_name in names_df['name'].values:
                # the name is an exact match
                matched_name = partial_name
            else:
                # return the first name match
                matched_name = names_df['name'].iloc[0]
        elif partial_name:
            # no name found
            matched_name = None
        return

    def get_song_ids(self, league_title:str, artists:list, titles:list) -> list:
        # first check for which songs already exists
        ids_df = self.get_table('Songs', league=league_title).drop(columns='league')
        songs_df = DataFrame(data=zip(artists, titles), columns=['artist', 'title']).merge(ids_df, on=['artist', 'title'])[['artist', 'title', 'song_id']]
        
        # then fill in songs that are new
        needed_id_count = songs_df['song_id'].insa().sum()
        new_ids = self.get_new_song_ids(league_title, needed_id_count)
        songs_df[songs_df.insa()]['song_id'] = new_ids
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
        leagues_df = self.get_table('Leagues', order_by=['date', 'ASC'])
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
        rounds_df = self.get_table('Rounds', league=league, order_by=['date', 'ASC']).drop(columns='league')
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
            status = read_sql(sql, self.connection)

            if len(status):
                round_status = status['status'].iloc[0] # ['new', 'open', 'closed']
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

    def get_songs(self, league):
        songs_df = self.get_table('Songs', league=league).drop(columns='league')
        return songs_df

    def store_votes(self, votes_df, league):
        df = votes_df.reindex(columns=self.store_columns('Votes'))
        df['league'] = league
        self.upsert_table('Votes', df)

    def get_votes(self, league):
        votes_df = self.get_table('Votes', league=league).drop(columns='league')
        return votes_df

    def store_members(self, members_df, league_title):
        df = members_df.reindex(columns=self.store_columns('Members'))
        df['league'] = league_title
        self.upsert_table('Members', df)

    def get_members(self, league_title):
        members_df = self.get_table('Members', league=league_title).drop(columns='league')
        return members_df

    ##def store_player_names(self, player_names, league_title):
    ##    members_df = DataFrame(columns=['league', 'player'])
    ##    members_df['player'] = player_names
    ##    members_df['league'] = league_title
    ##    self.upsert_table('Members', members_df)

    def store_players(self, players_df, league_title):
        df = players_df.reindex(columns=self.store_columns('Players'))
        self.upsert_table('Players', df)

        self.store_members(df, league_title)

    def get_players(self):
        players_df = self.get_table('Players')
        return players_df

    def get_player_names(self, league_title):
        members_df = self.get_members(league_title)
        player_names = members_df['player'].to_list()
        return player_names

    def get_weights(self):
        table_name = 'Weights'
        weights = read_sql(f'SELECT * FROM {self.table_name(table_name)}', self.connection, index_col='parameter')['value']
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