from contextlib import AbstractAsyncContextManager
from pandas import DataFrame

from comparisons import Patternizer

class Results:
    def __init__(self, columns=[], int_columns=[]):
        self.df = DataFrame(columns=columns)
        self.int_columns = int_columns

    def int_cols(self):
        for col in self.int_columns:
            self.df[col] = self.df[col].apply(lambda x: int(x) if str(x).isnumeric() else x)

class Songs(Results):
    columns = ['song_id', 'round', 'artist','title','submitter',
                                  'votes', 'people', 'closed', 'points']
    int_columns = ['song_id', 'votes']

    def __init__(self):
        super().__init__(columns=Songs.columns, int_columns=Songs.int_columns)
        self.patternizer = None

    def __repr__(self):
        return f'SONGS\n{self.df}\n'

    def sub_round(self, round_title, artists, titles, submitters, next_song_ids):
        songs_df = DataFrame(columns=Songs.columns)
        songs_df['song_id'] = next_song_ids
        songs_df['round'] = round_title
        songs_df['artist'] = artists
        songs_df['title'] = titles
        if len(submitters):
            songs_df['submitter'] = submitters

        return songs_df
        
    def add_round(self, round_title, artists, titles, submitters, next_song_ids):
        songs_df = self.sub_round(round_title, artists, titles, submitters, next_song_ids)

        self.df = self.df.append(songs_df, ignore_index=True)
        self.int_cols()

    def add_round_db(self, songs_df):
        if not songs_df.empty:
            self.df = self.df.append(songs_df, ignore_index=True)
            self.int_cols()

    ##def get_round(self, round_title):
    ##    df = self.df.query(f'round == "{round_title}"')
    ##    return df

    def get_songs_ids(self, round_title):
        round_song_ids = self.df.query(f'round == "{round_title}"')['song_id'].to_list()
        return round_song_ids

    def add_patternizer(self, votes, players=None, player_names=None):
        if player_names is None:
            player_names = players.player_names

        self.patternizer = Patternizer(self, votes, player_names)

    def get_patternizer(self, votes=None, players=None, player_names=None):
        if self.patternizer is None:
            if player_names is None:
                player_names = players.player_names
            self.add_patternizer(votes, player_names)

        return self.patternizer

    def calculate_points(self, votes, rounds,
                         weights={'votes': 1, 'people': 0.25, 'closed': 0.5, 'must_vote': True}):        
        # calculate points based on votes and participation
        must_vote = weights.get('must_vote', True)
        if must_vote and (self.patternizer is not None):
            
            did_count = self.patternizer.get_counted(must_vote=must_vote)
            did_count['submitter'] = self.df.merge(did_count, on='song_id')['submitter']

            counted = did_count.apply(lambda x: x[x['submitter']], axis=1)
        else:
            counted = 1

        self.df['votes'] = self.df.merge(votes.df.groupby('song_id').sum()[['vote']], on='song_id', how='left')['vote']
        self.df['people'] = self.df.merge(votes.df.groupby('song_id').count()[['player']].reset_index(), on='song_id', how='left')['player']
        self.df['closed'] = self.df['people'] == self.df.merge(rounds.df, on='round')['player_count']-1

        points_columns = ['votes', 'people', 'closed']
        weight_columns = [weights[f'{col}_points'] for col in points_columns]
        self.df['points'] = self.df[points_columns].mul(weight_columns).sum(1).mul(counted)

class Votes(Results):
    columns = ['song_id', 'player', 'vote']
    int_columns = ['song_id', 'vote']

    def __init__(self):
        super().__init__(columns=Votes.columns, int_columns=Votes.int_columns)

    def __repr__(self):
        return f'VOTES\n{self.df}\n'

    def sub_round(self, song_ids, players, vote_counts, next_song_ids):
        votes_df = DataFrame(columns=Votes.columns)
        votes_df['song_id'] = [next_song_ids[i-1] for i in song_ids]
        votes_df['player'] = players
        votes_df['vote'] = vote_counts

        return votes_df

    def add_round(self, song_ids, players, vote_counts, next_song_ids):
        votes_df = self.sub_round(song_ids, players, vote_counts, next_song_ids)

        self.df = self.df.append(votes_df, ignore_index=True)
        self.int_cols()

    def add_round_db(self, votes_df):
        if not votes_df.empty:
            self.df = self.df.append(votes_df, ignore_index=True)
            self.int_cols()

    ##def get_round(self, song_ids):
    ##    df = self.df.query(f'song_id in {song_ids}')
    ##    return df

    def name_rounds(self, songs):
        self.df['round'] = self.df.merge(songs.df, on='song_id')['round']

class Rounds(Results):
    columns = ['round', 'status', 'player_count']

    def __init__(self):
        super().__init__(columns=Rounds.columns)
        ##self.name_rounds(songs)
        ###self.close_rounds(songs, votes)
        ##self.count_players(votes)
        
    def __repr__(self):
        return f'ROUNDS\n{self.df}\n'

    def sub_rounds(self, round_titles, league_creator=None, **cols):
        rounds_df = DataFrame(columns=Rounds.columns)
        #rounds_df['league'] = league_titles
        rounds_df['round'] = round_titles

        for col in cols:
            rounds_df[col] = cols[col]

        if 'creator' in cols:
            rounds_df.loc[rounds_df['creator'].isna(), 'creator'] = league_creator

        return rounds_df

    def add_rounds_db(self, rounds_df):
        self.df = rounds_df.reindex(columns=Rounds.columns)

    def sort_titles(self, round_titles):
        self.df = self.df.set_index('round').reindex(round_titles).reset_index().reindex(columns=Rounds.columns)

    def get_titles(self):
        round_titles = self.df['round'].values
        return round_titles

    ##def name_rounds(self, songs):
    ##    self.df['round'] = songs.df['round'].unique()
        
    #def close_rounds(self, songs, votes):
    #    self.df['submitted'] = self.df['round'].apply(lambda x: x in songs.df['round'].values)
    #    self.df['voted'] = self.df['round'].apply(lambda x: x in votes.df[votes.df['player'].notna()]['round'].values)
        
    def count_players(self, votes):
        self.df['player_count'] = votes.df.groupby('round')[['player']].nunique().reset_index()['player']
       
class Leagues(Results):
    columns = ['league']
    def __init__(self):
        super().__init__(columns=Leagues.columns)

    def sub_leagues(self, league_titles, **cols):
        leagues_df = DataFrame(columns=Leagues.columns)
        leagues_df['league'] = league_titles

        for col in cols:
            leagues_df[col] = cols[col]

        return leagues_df

    def add_leagues(self, league_titles):
        self.df = self.sub_leagues(league_titles)

    def get_leagues(self):
        return self.df

    def get_league_titles(self):
        return self.df['league'].values

    def add_leagues_db(self, leagues_df):
        self.df = leagues_df.reindex(columns=Leagues.columns)

##class Members(Results):
##    columns = ['player']
##    def __init__(self):
##        super().__init__(columns=Members.columns)

##    def add_members(self, players)
