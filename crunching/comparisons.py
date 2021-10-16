from itertools import permutations, combinations
from math import pi, sin, cos, log

from scipy.optimize import minimize
from pandas import DataFrame, Series, concat

class Patternizer:
    def __init__(self, songs, votes, player_names):
        self.songs = songs
        self.votes = votes
        self.player_names = player_names

        self.patterns = None
        self.did_count = None

    def get_patterns(self):
        if self.patterns is None:
            patterns = self.votes.df.pivot(index='song_id', columns='player', values='vote').reset_index() #dropna(subset=['player'])
            columns = ['song_id', 'round', 'submitter']
            patterns = self.songs.df[columns].merge(patterns, on='song_id', how='left').reindex(columns=columns + self.player_names)

            average_votes_group_sum = self.votes.df.groupby('song_id').sum().reset_index()
            average_votes_group_count = self.votes.df.groupby('song_id').count().reset_index()
            patterns['v_'] = average_votes_group_sum['vote'].div(average_votes_group_count['vote'])

            average_votes_submitter_sum = self.votes.df.groupby(['round', 'player']).sum().reset_index()
            average_votes_submitter_count = self.votes.df.groupby(['round', 'player']).count().reset_index()
            average_votes_submitter = average_votes_submitter_sum.copy()
            average_votes_submitter['vote'] = average_votes_submitter_sum['vote'].div(average_votes_submitter_count['vote'])
            patterns['v^'] = self.songs.df.merge(average_votes_submitter, on='round')['vote']

            self.patterns = patterns

        return self.patterns

    def get_rounds_played(self):
        vote_rounds = self.votes.df.groupby(['round','player']).count()['vote'].ge(1).reset_index()
        rounds_played = vote_rounds.pivot(index='round', columns='player', values='vote').reset_index().reindex(columns=['round'] + self.player_names)
        return rounds_played

    def get_counted(self, must_vote=False):
        if self.did_count is None:
            rounds_played = self.get_rounds_played()

            did_vote = self.songs.df[['song_id', 'round']].merge(rounds_played,
                                                                 on=['round']).reindex(columns=['song_id'] + self.player_names).set_index('song_id').fillna(False)
       
            if must_vote:
                did_submit = False
            else:
                did_submit = concat([self.songs.df['song_id'],
                                     self.songs.df[['song_id', 'submitter']].pivot(columns='submitter')['song_id'].notna()],
                                    axis=1).reindex(columns=['song_id'] + self.player_names).set_index('song_id') # ensure all players

            did_count = (did_vote | did_submit).reset_index()

            self.did_count = did_count

        return self.did_count

    def get_distance(self, p1, p2=None):
        patterns = self.get_patterns()
        did_count = self.get_counted()

        if p2 is not None:
            counted = did_count[p1] & did_count[p2]
            pattern1 = patterns[['v^', p2]].max(1).where(patterns['submitter'] == p1, patterns[p1])
            pattern2 = patterns[['v^', p1]].max(1).where(patterns['submitter'] == p2, patterns[p2])
            
        else:
            counted = did_count[p1]
            pattern1 = patterns[['v^', 'v_']].max(1).where(patterns['submitter'] == p1, patterns[p1])
            pattern2 = patterns['v_']

        if counted.sum():                
            distances = (pattern1.where(counted, 0).fillna(0).sub(pattern2.where(counted, 0).fillna(0))).pow(2)
            distance = distances.sum() ** 0.5 / counted.sum()
        else:
            distance = None

        return distance

class Pulse:
    distance_threshold = 0.75

    def __init__(self, members):
        self.player_names = members.player_names
        self.player_permutations = list(permutations(self.player_names, 2))
        self.player_combinations = list(combinations(self.player_names, 2))
        self.df = DataFrame(data=self.player_permutations, columns=['p1', 'p2'])

    def __repr__(self):
        printed = self.df.drop(columns=[w for w in ['win'] if w in self.df.columns]).dropna(axis=1, how='all')
        return f'PULSE\n{printed}\n'

    # calculate who likes whom
    def calculate_likers(self, songs, votes):
        # calculate likes
        likes_per = votes.df.merge(songs.df[['song_id', 'submitter']], on='song_id').groupby(['player', 'submitter']).sum()['vote'].reset_index()
        likes_total = votes.df.merge(songs.df[['song_id', 'submitter']], on='song_id').groupby(['player']).sum()['vote'].reset_index()
        likes = likes_per.merge(likes_total, on='player')
        likes['pct'] = likes['vote_x']/likes['vote_y']

        # calculate liked
        liked_per = votes.df.merge(songs.df[['song_id', 'submitter']], on='song_id').groupby(['submitter', 'player']).sum()['vote'].reset_index()
        liked_total = votes.df.merge(songs.df[['song_id', 'submitter']], on='song_id').groupby(['submitter']).sum()['vote'].reset_index()
        liked = liked_per.merge(liked_total, on='submitter')
        liked['pct'] = liked['vote_x']/liked['vote_y']

        self.df['likes'] = self.df.merge(likes, left_on=['p1','p2'], right_on=['player','submitter'], how='left')['pct']
        self.df['liked'] = self.df.merge(liked, left_on=['p1','p2'], right_on=['submitter','player'], how='left')['pct']

    # calculate similarity
    def calculate_similarity(self, songs, votes):
        print('\t...getting patterns')
        patternizer = songs.get_patternizer(votes, self.player_names)
        
        print('\t...permutations')
        for p1, p2 in self.player_combinations:
            print(f'\t\t...{p1} vs {p2}')
            distance = patternizer.get_distance(p1, p2)
            self.df.loc[[self.player_permutations.index((p1, p2)),
                         self.player_permutations.index((p2, p1))], 'distance'] = distance

        print('\t...normalizing')
        # normalize results
        self.df['distance'] = self.df['distance'] / self.df[self.df['distance'].ne(0)]['distance'].min()

        # remove non-voters if outliers and keep voters within range
        voters = (votes.df.groupby('player').count()['vote'] > 0)
        voters_p1 = voters.reindex(self.df['p1']).fillna(False)
        voters_p2 = voters.reindex(self.df['p2']).fillna(False)
        voted = Series(DataFrame(zip(voters_p1, voters_p2)).prod(1) == 1, index=self.df.index)

        quantile = self.df['distance'].quantile(self.distance_threshold)
        std_dev = self.df['distance'].std()
        mean = self.df['distance'].mean()

        outliers = self.df['distance'] > quantile
        UB = mean + std_dev
        below_UB = self.df['distance'] <= UB
       
        self.df['plot_distance'] = self.df['distance'].where(voted | ~outliers, UB).where(below_UB, UB) ## double check non-voting logic

    def calculate_wins(self, songs, votes):
        patternizer = songs.get_patternizer(votes, self.player_names)
        did_count = patternizer.get_counted()
        for p1, p2 in self.player_permutations:
            counted = did_count[p1] & did_count[p2]

            if counted.sum():
                battle = songs.df['points'].mul(counted, axis=0)
                battle_results = battle.mul(songs.df['submitter'] == p1) - battle.mul(songs.df['submitter'] == p2)
                self.df.loc[self.player_permutations.index((p1, p2)), 'battle'] = battle_results.sum() / counted.sum()
                self.df['win'] = self.df['battle'] > 0

class Members:
    columns = ['player', 'wins', 'dfc', 'likes', 'liked'] #'x', 'y', 

    def __init__(self, player_names):
        self.df = DataFrame(columns=self.columns)
        self.df['player'] = player_names
        self.player_combinations = list(combinations(self.df['player'], 2))

        self.player_names = player_names

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
        n = len(self.player_combinations)
        max_iterations = max(1, min(max_iters, int(10**(8/log(n*(n+1)/2)))))

        distances = DataFrame(data=self.player_combinations, columns=['p1', 'p2'])
        distances['distance'] = distances.merge(pulse.df, on=['p1', 'p2'], how='left')['plot_distance']
        
        needed = distances['distance'].notna() # only include if pair voted together

        # update from db if exists
        if xy_ is not None:
            self.df[['x', 'y']] = self.df.drop(columns=[c for c in ['x', 'y'] if c in self.df.columns]).merge(xy_, on='player')[['x', 'y']]

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

    def who_likes_whom(self, pulse):
        # calculate likes and liked values
        like_cols = ['likes', 'liked']
        for like in like_cols:
            most_like = pulse.df.sort_values(['p1', like], ascending=False).groupby('p1').first().reset_index()
            merged_like = self.df.drop(columns=like_cols).merge(most_like, left_on='player', right_on='p1', how='left')
            self.df[like] = merged_like['p2'].where(merged_like[like] > 0)

    def get_dfc(self, songs, votes):
        patternizer = songs.get_patternizer(votes, self.player_names)
        self.df['dfc'] = self.df.apply(lambda x: patternizer.get_distance(x['player']), axis=1)

    def battle(self, pulse):
        # update to look at PPR
        self.df['wins'] = self.df.merge(pulse.df[['p1', 'win']].groupby('p1').sum().reset_index(),
                                        left_on='player', right_on='p1')['win']

class Rankings:
    def __init__(self, songs, votes, weights={'must_vote': True}):
        self.df = self.calculate_points(songs, votes, weights)
        
        self.voted_per_round = self.get_votes_per_round(votes)
        self.submitted_per_round = self.get_submitted_per_round(songs)
 
        self.normalize_rankings(songs, must_vote=weights.get('must_vote', True))
        self.sort_rankings(songs)

    def __repr__(self):
        return f'RANKINGS\n{self.df.fillna("DNF")}\n'

    def calculate_points(self, songs, votes, weights):
        # calculate points earned during round
        points_df = songs.df.drop(columns=['song_id', 'people']).groupby(['round', 'submitter']).sum()

        # calculate bonus points
        bonus_points = songs.df.merge(votes.df[['song_id', 'vote']], on='song_id', how='left')\
            .groupby(['round', 'submitter', 'song_id'])['vote'].sum().eq(0)\
            .reset_index().groupby(['round', 'submitter'])['vote'].sum().mul(-1).add(1)\
            .mul(weights.get('all_songs', 0))

        points_df['points'] += bonus_points
        points_df.index.rename(['round', 'player'], inplace=True)

        return points_df

    def get_votes_per_round(self, votes):
        voted_per_round = votes.df.groupby(['round', 'player'])[['vote']].sum() > 0

        return voted_per_round

    def get_submitted_per_round(self, songs):
        submit_df = songs.df.drop(columns=['song_id', 'people']).groupby(['round', 'submitter']).sum()
        submitted_per_round = submit_df.mask(submit_df >= 0, True).reset_index()\
            .pivot(columns='round', values='points', index='submitter')\
            .reindex(columns=submit_df.index.levels[0]).fillna(False)

        return submitted_per_round

    def normalize_rankings(self, songs, must_vote=True):
        # suppress points where no votes and normalize score
        if must_vote:
            self.df['points'] = self.df.where(self.voted_per_round['vote'])['points']

        votes_per_round = songs.df.groupby('round').sum()[['votes']] 
        self.df['score'] = self.df['points'].div(self.df.apply(lambda x: \
            votes_per_round.loc[x.name[0],'votes'], axis=1)).mul(100)\
            .apply(Rankings.round_me)

    def round_me(x):
        try:
            i = round(x)
        except:
            i = x
        return i
        
    def sort_rankings(self, songs):
        reindexer = songs.df['round'].drop_duplicates()
        self.df = self.df.sort_values(['round', 'points'], ascending=[True, False]).reindex(reindexer, level=0)

    def get_board(self):
        pointsboard = self.df.reset_index().pivot(columns='round', values='points', index='player')\
            .reindex(columns=self.df.index.levels[0])

        leaderboard = pointsboard.rank(ascending=False, method='average')
        t1 = pointsboard.isna()
        t2 = self.submitted_per_round

        dnf = -pointsboard.fillna(1).where(pointsboard.isna()).where(self.submitted_per_round).rank(method='first')
        
        board = leaderboard.add(dnf, fill_value=0)
        
        return board