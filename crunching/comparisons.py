''' Round comparisons '''

from itertools import combinations
from math import pi, sin, cos, log

from scipy.optimize import minimize
from pandas import DataFrame, concat

class Patternizer:
    def __init__(self, songs, votes, player_ids):
        self.songs = songs
        self.votes = votes
        self.player_ids = player_ids

        self.patterns = None
        self.did_count = None

    def get_patterns(self):
        if self.patterns is None:
            patterns = self.votes.df.pivot(index='song_id', columns='player_id', values='vote').reset_index() #dropna(subset=['player_id'])
            columns = ['song_id', 'round_id', 'submitter_id']
            patterns = self.songs.df[columns].merge(patterns, on='song_id', how='left').reindex(columns=columns + self.player_ids)

            average_votes_group_sum = self.votes.df.groupby('song_id').sum().reset_index()
            average_votes_group_count = self.votes.df.groupby('song_id').count().reset_index()
            patterns['v_'] = average_votes_group_sum['vote'].div(average_votes_group_count['vote'])

            average_votes_submitter_sum = self.votes.df.groupby(['round_id', 'player_id']).sum().reset_index()
            average_votes_submitter_count = self.votes.df.groupby(['round_id', 'player_id']).count().reset_index()
            average_votes_submitter = average_votes_submitter_sum.copy()
            average_votes_submitter['vote'] = average_votes_submitter_sum['vote'].div(average_votes_submitter_count['vote'])
            patterns['v^'] = self.songs.df.merge(average_votes_submitter, on='round_id')['vote']

            self.patterns = patterns

        return self.patterns

    def get_rounds_played(self):
        vote_rounds = self.votes.df.groupby(['round_id','player_id']).count()['vote'].ge(1).reset_index()
        rounds_played = vote_rounds.pivot(index='round_id', columns='player_id', values='vote').reset_index().reindex(columns=['round_id'] + self.player_ids)
        return rounds_played

    def get_counted(self, must_vote=False):
        if self.did_count is None:
            rounds_played = self.get_rounds_played()

            did_vote = self.songs.df[['song_id', 'round_id']].merge(rounds_played,
                                                                 on=['round_id']).reindex(columns=['song_id'] + self.player_ids).set_index('song_id').fillna(False)
       
            if must_vote:   
                did_submit = False
            else:
                did_submit = concat([self.songs.df['song_id'],
                                     self.songs.df[['song_id', 'submitter_id']].pivot(columns='submitter_id')['song_id'].notna()],
                                    axis=1).reindex(columns=['song_id'] + self.player_ids).set_index('song_id') # ensure all players

            did_count = (did_vote | did_submit).reset_index()

            self.did_count = did_count

        return self.did_count

    def get_distance(self, p1, p2=None):
        patterns = self.get_patterns()
        did_count = self.get_counted()

        if p2 is not None:
            counted = did_count[p1] & did_count[p2]
            pattern1 = patterns[['v^', p2]].max(1).where(patterns['submitter_id'] == p1, patterns[p1])
            pattern2 = patterns[['v^', p1]].max(1).where(patterns['submitter_id'] == p2, patterns[p2])
            
        else:
            counted = did_count[p1]
            pattern1 = patterns[['v^', 'v_']].max(1).where(patterns['submitter_id'] == p1, patterns[p1])
            pattern2 = patterns['v_']

        if counted.sum():                
            distances = (pattern1.where(counted, 0).fillna(0).sub(pattern2.where(counted, 0).fillna(0))).pow(2)
            distance = distances.sum() ** 0.5 / counted.sum()
        else:
            distance = None

        return distance

class Pulse:
    distance_threshold = 0.75

    def __init__(self, distances):
        self.df = distances

    def __repr__(self):
        return f'PULSE\n{self.df}\n'

    def normalize_distances(self):
        ''' calculate similarity '''
        print('\t...normalizing')
        # normalize results
        self.df['distance'] = self.df['distance'] / self.df[self.df['distance'].ne(0)]['distance'].min()

        # remove outliers and keep within range
        quantile = self.df['distance'].quantile(self.distance_threshold)
        std_dev = self.df['distance'].std()
        mean = self.df['distance'].mean()

        outliers = self.df['distance'] > quantile
        UB = mean + std_dev
        below_UB = self.df['distance'] <= UB
       
        self.df['plot_distance'] = self.df['distance'].where(~outliers, UB).where(below_UB, UB)

class Members:
    columns = ['player_id', 'x', 'y']

    def __init__(self, player_ids):
        self.df = DataFrame(columns=self.columns)
        self.df['player_id'] = player_ids
        self.player_combinations = list(combinations(self.df['player_id'], 2))

        self.player_ids = player_ids

        self.coordinates = {'success': False,
                            'message': None}

    def __repr__(self):
        printed = self.df.sort_values(['wins', 'dfc'], ascending=[False, True]).dropna(axis='columns', how='all')
        return f'MEMBERS\n{printed}\n'

    def distdiff(self, xy, D, N, xy0=[0,0]):
        # first point is fixed at 0,0; second point at 0, D[0]
        x = [xy0[0]] + xy.tolist()[0:int(len(xy)/2)] # x coordinates
        y = [xy0[1]] + xy.tolist()[int(len(xy)/2):] # y coordinates
    
        c = list(combinations(range(len(x)), 2))

        difference = sum((N[i] * (((x[c[i][0]] - x[c[i][1]])**2 + (y[c[i][0]] - y[c[i][1]])**2)**0.5 - D[i]))**2 \
            for i in range(len(c)))**0.5
        return difference

    def get_members(self):
        return self.df.reindex(columns=self.columns)

    def seed_xy(self, pulse):
        # place the first player at the origin
        # find the average distance between players as R
        # place the other players at radius R angle pi / #
        # consider placing the most central player first
        R = pulse.df['distance'].mean() if pulse.df['distance'].mean() > 0 else 1
        angle = 2*pi / len(self.df)

        seeded = concat([self.df.apply(lambda x: R * cos(angle * (x.name - 1)) if x.name > 0 else 0, axis=1),
                         self.df.apply(lambda x: R * sin(angle * (x.name - 1)) if x.name > 0 else 0, axis=1)],
                        axis=1)

        self.df[['x', 'y']] = self.df.mask(self.df[['x', 'y']].isna().sum(1).ne(0), seeded)[['x', 'y']]

    def update_coordinates(self, pulse, xy_=None, max_iters=5000):
        # best fit player nodes
        n_players = len(self.player_ids)

        if n_players <= 2:
            # trivial case with only 2 or less players
            self.df['x'] = [0, 1]
            self.df['y'] = [0, 0]

            self.coordinates['success'] = True
            self.coordinates['message'] = f'trivial case of {n_players} players'

        else:
            n = len(self.player_combinations)
            max_iterations = max(1, min(max_iters, int(10**(8/log(n*(n+1)/2)))))

            distances = DataFrame(data=self.player_combinations, columns=['player_id', 'opponent_id'])
            distances['distance'] = distances.merge(pulse.df, on=['player_id', 'opponent_id'], how='left')['plot_distance']
        
            needed = distances['distance'].notna() # only include if pair voted together

            # update from db if exists
            if xy_ is not None:
                self.df[['x', 'y']] = self.df.drop(columns=[c for c in ['x', 'y'] if c in self.df.columns]).merge(xy_, on='player_id')[['x', 'y']]

            ##if self.df[['x', 'y']].isna().all().all():
            self.seed_xy(pulse)

            xy0 = self.melt_xy(self.df)
            print('\t...minimizing')
            xy = minimize(self.distdiff, xy0, args=(distances['distance'], needed), options={'maxiter': max_iterations})

            self.df['x'] = [0] + xy.x.tolist()[0:int(len(xy.x)/2)]
            self.df['y'] = [0] + xy.x.tolist()[int(len(xy.x)/2):]

            self.coordinates['success'] = xy.success
            self.coordinates['message'] = xy.message

            if xy.success:
                print('\t\t...optimal solution found')
            else:
                print(xy.message)

            if xy_ is not None:
                dist0 = self.distdiff(self.melt_xy(xy_), distances['distance'], needed)
                dist1 = self.distdiff(self.melt_xy(self.df), distances['distance'], needed)
                improvement = 1 - dist1/dist0
                print(f'\t\t...improved by {improvement:.2%}')

    def melt_xy(self, df):
        xy = df[['x','y']][1:].fillna(0).melt()['value']
        return xy