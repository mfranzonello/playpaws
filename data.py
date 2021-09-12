from os import getlogin

from sqlalchemy import create_engine
from pandas import read_sql

class Database:
    def __init__(self, db_name):
        self.engine = create_engine(f'sqlite:///{db_name}.db')
        self.connection = self.engine.connect()

    def get_table(self, table_name, columns=None, league=None):
        # get values from database

        # check if column specific
        if columns is None:
            # return full table
            cols = '*'
        else:
            cols = ', '.join(columns)
            # return only specific rows, like finding keys

        # check if league specific
        if league is None:
            # return all values
            wheres = ''
        else:
            # return only league values
            wheres = f' WHERE league = "{league}"'

        # write and execute SQL
        sql = f'SELECT {cols} FROM {table_name}{wheres};'
        table = read_sql(sql, self.connection, coerce_float=True)

        return table

    def quotable(item):
        try_string = str(item)
        quotable = not ((try_string in ['True', 'False']) or try_string.isnumeric())
        return quotable

    def needs_quotes(item):
        # put quotes around strings to account for special characters
        quoted = '"' + str(item).replace('"', '""') + '"' if Database.quotable(item) else str(item)
        return quoted

    def update_rows(table_name, df, keys):
        # write SQL for existing rows
        if df.empty:
            sql = ''
        else:
            updates = []
            for i in df.index:
                df_row = df.loc[i]
                key_values = df_row[keys].values
                columns = df_row.drop(keys).index
                values = df_row.drop(keys).values
                sets = ', '.join(f'{column} = ' + Database.needs_quotes(value) for column, value in zip(columns, values))
                wheres = ' & '.join(f'({key} = ' + Database.needs_quotes(key_value) + ')' for key, key_value in zip(keys, key_values))
                updates.append(f'UPDATE {table_name} SET {sets} WHERE {wheres};')
            sql = ' '.join(updates)

        return sql

    def insert_rows(table_name, df):
        # write SQL for new rows
        if df.empty:
            sql = ''
        else:
            columns = '(' + ', '.join(df.columns) + ')'
            values = ', '.join('(' + ', '.join(Database.needs_quotes(value) for value in df.loc[i]) + ')' for i in df.index)
            sql = f'INSERT INTO {table_name} {columns} VALUES {values};'

        return sql

    def find_existing(df, df_existing, keys):
        # split dataframe between old and new rows
        links = ['(' + ' & '.join(f'({key} == ' + Database.needs_quotes(key_value) + ')' \
            for key, key_value in zip(keys, df_existing[keys].loc[i])) + ')' \
            for i in df_existing.index]
        
        if len(df_existing):
            # there is already data in the database
            sql_up = ' | '.join(links)
            sql_in = f'~({sql_up})'

            df_updates = df.query(sql_up)
            df_inserts = df.query(sql_in)
        else:
            # all data is new
            df_updates = df.reindex([])
            df_inserts = df.reindex()
        
        return df_updates, df_inserts

    def get_keys(self, table_name):
        keys = read_sql(f'PRAGMA TABLE_INFO({table_name})', self.connection).query('pk > 0')['name']
        return keys

    def execute_sql(self, sql):
        for s in sql.split(';'):
            ##print(s)
            self.connection.execute(s.strip())

    def upsert_table(self, table_name, df):
        # update existing rows and insert new rows
        keys = self.get_keys(table_name)

        # get current league
        league = df['league'].iloc[0]

        # get existing ids in database
        df_existing = self.get_table(table_name, columns=keys, league=league)

        # split dataframe into existing updates and new inserts
        df_updates, df_inserts = Database.find_existing(df, df_existing, keys)
        
        # write SQL for updates and inserts
        sql_updates = Database.update_rows(table_name, df_updates, keys)
        sql_insert = Database.insert_rows(table_name, df_inserts)

        # write combined SQL
        sql = ' '.join(s for s in [sql_updates, sql_insert] if len(s))
        
        # execute SQL
        self.execute_sql(sql)

    def get_next_song_ids(self, retrieve, league):
        # get next available song_ids
        existing_song_ids = self.get_table('Songs', columns=['song_id'], league=league)['song_id']
        max_song_id = 0 if existing_song_ids.empty else existing_song_ids.max()
        next_song_ids = [song_id for song_id in range(1, max_song_id + retrieve + 1) \
            if song_id not in existing_song_ids.values][0:retrieve]
        return next_song_ids

    def store_leagues(self, leagues_df):
        # store league names
        keys = self.get_keys('Leagues')
        df_existing = self.get_table('Leagues')
        _, df_inserts = Database.find_existing(leagues_df, df_existing, keys)
        sql_insert = Database.insert_rows('Leagues', df_inserts)

        self.execute_sql(sql_insert)

    def store_league_url(self, league_title, url):
        # add URL for league
        if url is not None:
            sql_update = f'UPDATE Leagues SET url = "{url}" WHERE league = "{league_title}"'
            self.execute_sql(sql_update)

    def get_rounds(self, league):
        rounds_df = self.get_table('Rounds', league=league).drop(columns='league')
        return rounds_df

    def get_url_status(self, url):
        sql = f'SELECT league, round FROM Rounds WHERE url = "{url}"'
        results = read_sql(sql, self.connection)

        if len(results):
            league, round_title = results.iloc[0]
        else:
            league, round_title = [None, None]

        return league, round_title

    def get_round_status(self, league, round_title):
        if (league is None) and (round_title is None):
            round_status = 'n/a'
        else:
            sql = f'SELECT * FROM Rounds WHERE (league = "{league}") & (round = "{round_title}")' 
            status = read_sql(sql, self.connection)

            if len(status):
                round_status = status['status'].iloc[0] # ['new', 'open', 'closed']
            else:
                round_status = 'missing'

        return round_status

    def store_round(self, league_title, round_title, new_status, url=None):
        round_status = self.get_round_status(league_title, round_title)

        # add round if not in DB
        if round_status == 'missing':
            # don't store URL if it is blank
            set_items = ['league', 'round', 'status', 'url']
            value_items = [league_title, round_title, new_status, url]
            items_limit = len(set_items) if (url is not None) else len(set_items)-1

            sets = '(' + ', '.join(item for item in set_items[:items_limit]) + ')'
            values = '(' + ', '.join(f'"{item}"' for item in value_items[:items_limit]) +')'
            sql = f'INSERT INTO Rounds {sets} VALUES {values}'

        # update round if in DB and there is a change
        elif (round_status in ['new', 'open', 'closed']) and (round_status != new_status):
            sets = f'status = "{new_status}"'
            if (url is not None) & (url != self.get_url('round', league_title, round_title=round_title)):
                sets += f', url = "{url}"'
            sql = f'UPDATE Rounds SET {sets} WHERE (league = "{league_title}") & (round = "{round_title}")'

        # skip if error
        else:
            sql = None

        # execute if there is a needed update or insert
        if sql is not None:
            self.connection.execute(sql)

    def get_url(self, url_type, directory, league_title, round_title=None):
        # return specific URL
        table_name = {'league': 'Leagues',
                      'round': 'Rounds'}[url_type]

        wheres = f'(league = "{league_title}")'
        if round_title is not None:
            wheres += f' & (round = "{round_title}")'
        sql = f'SELECT url FROM {table_name} WHERE {wheres}'
        urls = read_sql(sql, self.connection)['url'].values
        url = urls[0] if len(urls) else None

        url = f'{directory}/{url}'
        return url

    def get_urls(self, url_type, directory):
        # return list of URLs for a type
        table_name = {'league': 'Leagues',
                      'round': 'Rounds'}[url_type]
        sql = f'SELECT url FROM {table_name}'
        urls = read_sql(sql, self.connection)['url'].values

        urls = [f'{directory}/{url}' for url in urls]
        return urls

    def store_songs(self, songs_df, league):
        df = songs_df.reindex(columns=['song_id', 'round', 'artist', 'title', 'submitter'])
        df['league'] = league
        self.upsert_table('Songs', df)

    def get_songs(self, league):
        songs_df = self.get_table('Songs', league=league).drop(columns='league')
        return songs_df

    def store_votes(self, votes_df, league):
        df = votes_df.reindex(columns=['song_id', 'player', 'vote'])
        df['league'] = league
        self.upsert_table('Votes', df)

    def get_votes(self, league):
        votes_df = self.get_table('Votes', league=league).drop(columns='league')
        return votes_df

    def get_weights(self):
        weights = read_sql('SELECT * FROM Weights', self.connection, index_col='parameter')['value']
        weights = weights.apply(Database.clean_up_weight)
        return weights

    def clean_up_weight(value):
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

