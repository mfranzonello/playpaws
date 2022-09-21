from pandas import DataFrame

from crunching.comparisons import Patternizer

class Results:
    def __init__(self, columns=[], int_columns=[]):
        self.df = DataFrame(columns=columns)
        self.int_columns = int_columns

    def int_cols(self):
        for col in self.int_columns:
            self.df[col] = self.df[col].apply(lambda x: int(x) if str(x).isnumeric() else x)

class Songs(Results):
    columns = ['song_id', 'round_id', 'submitter_id', 'track_uri',
               'votes', 'people', 'closed', 'discovery', 'points']
    int_columns = ['song_id', 'votes']

    def __init__(self):
        super().__init__(columns=Songs.columns, int_columns=Songs.int_columns)
        self.patternizer = None

    def __repr__(self):
        return f'SONGS\n{self.df.reindex(columns=self.columns)}\n'

    def sub_round(self, round_title, track_urls, submitters, next_song_ids, **cols):
        songs_df = DataFrame(columns=Songs.columns)
        songs_df['song_id'] = next_song_ids
        songs_df['round_id'] = round_title
        songs_df['track_uri'] = track_urls

        if len(submitters):
            songs_df['submitter_id'] = submitters

        for col in cols:
            songs_df[col] = cols[col]

        return songs_df

    def add_round_db(self, songs_df):
        if not songs_df.empty:
            self.df = self.df.append(songs_df, ignore_index=True)
            self.int_cols()

    def get_songs_ids(self, round_id):
        round_song_ids = self.df.query(f'round_id == "{round_id}"')['song_id'].to_list()
        return round_song_ids

    def add_patternizer(self, votes, members=None, player_ids=None):
        if player_ids is None:
            player_ids = members.player_ids

        self.patternizer = Patternizer(self, votes, player_ids)

    def get_patternizer(self, votes, player_ids):
        if self.patternizer is None:
            self.add_patternizer(votes, player_ids=player_ids)

        return self.patternizer

    ##def add_discoveries(self, discoveries_df):
    ##    self.df['discovery'] = self.df.drop(columns='discovery').merge(discoveries_df, how='left', on='song_id')['discovery'].fillna(0)

    def calculate_points(self, votes, rounds,
                         weights={'votes': 1, 'people': 0.25, 'closed': 0.5,
                                  'must_vote': True, 'discovery_points': 0.1}):

        # calculate points based on votes and participation
        must_vote = weights.get('must_vote', True)
        if must_vote and (self.patternizer is not None):
            
            did_count = self.patternizer.get_counted(must_vote=must_vote)
            did_count['submitter_id'] = self.df.merge(did_count, on='song_id')['submitter_id']

            counted = did_count.apply(lambda x: x[x['submitter_id']], axis=1)
        else:
            counted = 1

        self.df['votes'] = self.df.merge(votes.df.groupby('song_id').sum()[['vote']], on='song_id', how='left')['vote']
        self.df['people'] = self.df.merge(votes.df.groupby('song_id').count()[['player_id']].reset_index(), on='song_id', how='left')['player_id']
        self.df['closed'] = self.df['people'] == self.df.merge(rounds.df, on='round_id')['player_count']-1

        points_columns = ['votes', 'people', 'closed']
        weight_columns = [weights[f'{col}_points'] for col in points_columns]
        ##discovery_points = self.df['discovery'].mul(self.df['votes'].gt(0)).mul(weights['discovery_points'])
        self.df['points'] = self.df[points_columns].mul(weight_columns).sum(1)##.add(discovery_points).mul(counted)

class Votes(Results):
    columns = ['song_id', 'player_id', 'vote']
    int_columns = ['song_id', 'vote']

    def __init__(self):
        super().__init__(columns=Votes.columns, int_columns=Votes.int_columns)

    def __repr__(self):
        return f'VOTES\n{self.df.reindex(columns=self.columns)}\n'

    def sub_round(self, song_ids, player_ids, vote_counts, vote_totals, next_song_ids):
        votes_df = DataFrame(columns=Votes.columns)
        if song_ids:
            votes_df['song_id'] = [next_song_ids[i-1] for i in song_ids]
        else:
            votes_df['song_id'] = next_song_ids
        
        if len(player_ids):
            votes_df['player_id'] = player_ids
            votes_df['vote'] = vote_counts
        elif len(vote_totals):
            votes_df['vote'] = vote_totals

        return votes_df

    def add_round_db(self, votes_df):
        if not votes_df.empty:
            self.df = self.df.append(votes_df, ignore_index=True)
            self.int_cols()

    def name_rounds(self, songs):
        self.df['round_id'] = self.df.merge(songs.df, on='song_id')['round_id']

class Rounds(Results):
    columns = ['round_id', 'player_count']

    def __init__(self):
        super().__init__(columns=Rounds.columns)
        
    def __repr__(self):
        return f'ROUNDS\n{self.df}\n'

    def sub_rounds(self, round_ids, league_creator=None, **cols):
        rounds_df = DataFrame(columns=Rounds.columns)
        rounds_df['round_id'] = round_ids

        for col in cols:
            rounds_df[col] = cols[col]

        if 'creator_id' in cols:
            rounds_df.loc[rounds_df['creator_id'].isna(), 'creator_id'] = league_creator

        return rounds_df

    def add_rounds_db(self, rounds_df):
        self.df = rounds_df.reindex(columns=Rounds.columns)

    ##def sort_ids(self, round_ids):
    ##    self.df = self.df.set_index('round_id').reindex(round_ids).reset_index().reindex(columns=Rounds.columns)

    def get_ids(self):
        round_ids = self.df['round_id'].to_list()
        return round_ids
        
    def count_players(self, votes):
        self.df['player_count'] = votes.df.groupby('round_id')[['player_id']].nunique().reset_index()['player_id']
       
class Leagues(Results):
    columns = ['league_id']
    def __init__(self):
        super().__init__(columns=self.columns)

    def sub_leagues(self, league_ids, **cols):
        leagues_df = DataFrame(columns=self.columns)
        leagues_df['league_id'] = league_ids

        for col in cols:
            leagues_df[col] = cols[col]

        return leagues_df

    def add_leagues(self, league_ids):
        self.df = self.sub_leagues(league_ids)

    def get_leagues(self):
        return self.df

    def get_league_ids(self):
        return self.df['league_id'].values

    def add_leagues_db(self, leagues_df):
        self.df = leagues_df.reindex(columns=Leagues.columns)

class Players(Results):
    columns = ['player_id', 'username', 'src']
    def __init__(self):
        super().__init__(columns=self.columns)

    def sub_players(self, player_ids, **cols):
        players_df = DataFrame(columns=self.columns)
        players_df['player_id'] = player_ids
        for col in cols:
            players_df[col] = cols[col]

        return players_df

    def add_players(self, player_ids):
        self.df = self.sub_players(player_ids)

    def add_players_db(self, players_df):
        self.df = players_df.reindex(columns=self.columns)