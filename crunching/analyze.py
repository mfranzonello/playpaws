''' Number crunching for round results '''

from crunching.results import Songs, Votes, Rounds, Leagues ##, Players
from crunching.comparisons import Members, Rankings, Pulse

class Analyzer:
    version = 1.3
    def __init__(self, database):
        self.database = database
        self.weights = database.get_weights(self.version)

    def analyze_all(self):
        league_ids = self.get_league_ids()

        for league_id in league_ids:
            if self.database.check_data(league_id):
                league_title = self.database.get_league_name(league_id)
                rounds_db = self.database.get_rounds(league_id)

                round_ids = rounds_db['round_id'].to_list()
                
                if self.database.get_analyzed(league_id, round_ids, self.version):
                    print(f'Analysis for {league_title} already up to date')

                else:
                    analysis = self.analyze_league(league_id, league_title)

                    if analysis:
                        songs = analysis['songs']
                        members = analysis['members']
                        rankings = analysis['rankings']
                        board = rankings.get_board()
                        rounds = analysis['rounds']
                        
                        self.database.store_results(songs.df, league_id)
                        self.database.store_members(members.df, league_id)
                        self.database.store_rankings(rankings.df, league_id)
                        self.database.store_boards(board, league_id)

                        self.database.store_analysis(league_id, self.version,
                                                     round_ids,
                                                     optimized=False)

    def place_all(self):
        league_ids = self.get_league_ids()

        for league_id in league_ids:
            if self.database.check_data(league_id):
                league_title = self.database.get_league_name(league_id)
                # place
                if self.database.get_optimized(league_id):
                    print(f'Placements for {league_title} already up to date')

                else:
                    placement = self.place_league(league_id, league_title)

                    if placement:
                        members = placement['members']
                        pulse = placement['pulse']

                        self.database.store_members(members.df, league_id)
                        self.database.store_pulse(pulse.df, league_id)

                        self.database.store_analysis(league_id, self.version, optimized=members.coordinates['success'])
        
    def analyze_league(self, league_id, league_title):
        print(f'Setting up league {league_title}')
        members = self.get_members(league_id)

        songs, votes, rounds = self.get_songs_and_votes(league_id)

        if self.check_songs_and_votes(songs, votes):
            print(f'\t...analyzing {league_title}')
            rankings = self.crunch_rounds(songs, votes, rounds, members)

            pulse = self.get_pulse(songs, votes, members)

            # display results           
            print(f'{league_title} Results\n')
            for df in [songs, rounds, pulse, members, rankings]:
                print(df)

            analysis = {'league_id': league_id,
                        'songs': songs,
                        'votes': votes,
                        'rounds': rounds,
                        'rankings': rankings,
                        'pulse': pulse,
                        'members': members,
                        }

        else:
            print(f'\t...no data for {league_title}')
            analysis = None

        return analysis

    def place_league(self, league_id, league_title):
        print(f'Placing members in league {league_title}')
        members = self.get_members(league_id)

        songs, votes, _ = self.get_songs_and_votes(league_id)

        if self.check_songs_and_votes(songs, votes):
            print(f'\t...analyzing {league_title}')
            xy_ = self.get_coordinates(league_id)

            pulse = self.get_placements(songs, votes, members, xy_)

            # display results
            print(f'{league_title} Results\n')
            for df in [pulse, members]:
                print(df)

            placement = {'league_id': league_id,
                         'pulse': pulse,
                         'members': members,
                         }

        else:
            print(f'\t...no data for {league_title}')
            placement = None

        return placement

    def get_league_ids(self):
        leagues = Leagues()
        db_leagues = self.database.get_leagues()
        leagues.add_leagues_db(db_leagues)
        league_ids = leagues.get_league_ids()

        return league_ids

    def get_members(self, league_id):
        player_ids = self.database.get_player_ids(league_id)

        members = Members(player_ids)

        return members
        
    def get_songs_and_votes(self, league_id):
        print('Loading from database')
        rounds = Rounds()
        songs = Songs()
        votes = Votes()

        db_rounds = self.database.get_rounds(league_id)
        db_songs = self.database.get_songs(league_id)
        db_votes = self.database.get_votes(league_id)

        rounds.add_rounds_db(db_rounds)
        round_ids = rounds.get_ids()

        for round_id in round_ids:
            if self.database.check_data(league_id, round_id=round_id):
                songs.add_round_db(db_songs.query(f'round_id == "{round_id}"'))
                round_song_ids = songs.get_songs_ids(round_id)
                votes.add_round_db(db_votes.query(f'song_id in {round_song_ids}'))

        votes.name_rounds(songs)
        
        ### add discovery scores
        ##db_discoveries = self.database.get_discoveries(league_title)
        ##songs.add_discoveries(db_discoveries)
            
        return songs, votes, rounds

    def check_songs_and_votes(self, songs, votes):
        # make sure there were songs and votes
        check = (songs.df['submitter_id'].notna().sum() > 0) and (votes.df['player_id'].notna().sum() > 0)
        return check

    def crunch_rounds(self, songs, votes, rounds, members):
        print('Crunching rounds')
        # count how many submitted per round
        rounds.count_players(votes)

        # total points per song
        songs.add_patternizer(votes, members=members)
        songs.calculate_points(votes, rounds, self.weights)

        # list winners of each round
        rankings = Rankings(songs, votes, self.weights)

        return rankings

    def get_pulse(self, songs, votes, members):
        # get group pulse
        print('Getting pulse')

        pulse = Pulse(members)
        print('\t...likes')
        pulse.calculate_likers(songs, votes)
        print('\t...wins')
        pulse.calculate_wins(songs, votes)

        print('\t...likes')
        members.who_likes_whom(pulse)
        print('\t...battle')
        members.battle(pulse)

        return pulse

    def get_placements(self, songs, votes, members, xy_=None):
        # get group pulse
        print('Getting pulse')

        pulse = Pulse(members)

        # calculate distances
        print('\t...similarity')
        pulse.calculate_similarity(songs, votes)
        print('\t...coordinates')
        members.update_coordinates(pulse, xy_=xy_)
        print('\t...dfc')
        members.get_dfc(songs, votes)

        return pulse

    def get_coordinates(self, league_id):
        members = self.database.get_members(league_id)
        xy_ = members[['player_id', 'x', 'y']]
        return xy_