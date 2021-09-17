from results import Songs, Votes, Rounds, Leagues
from comparisons import Members, Rankings, Pulse

class Analyzer:
    def __init__(self, database):
        self.database = database
        self.weights = database.get_weights()

    def analyze_all(self):
        league_titles = self.get_league_titles()

        analyses = []
        for league_title in league_titles:
            if self.database.check_data(league_title):
                analysis = self.analyze_league(league_title, summary=True)

                if analysis:
                    analyses.append(analysis)

        return analyses

    def analyze_league(self, league_title, summary=True):
        print(f'Setting up league {league_title}')
        members = self.get_members(league_title)

        weights = self.database.get_weights()

        songs, votes, rounds = self.get_songs_and_votes(league_title)

        if self.check_songs_and_votes(songs, votes):
            print(f'\t...analyzing {league_title}')
            rankings = self.crunch_rounds(songs, votes, rounds, members, weights)

            xy_ = self.get_coordinates(league_title)

            pulse = self.get_pulse(songs, votes, members, xy_)

            self.store_coordinates(league_title, members)

            # display results
            if summary:           
                print(f'{league_title} Results\n')
                for df in [songs, rounds, pulse, members, rankings]:
                    print(df)

            analysis = {'league_title': league_title,
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

    def get_league_titles(self):
        leagues = Leagues()
        db_leagues = self.database.get_leagues()
        leagues.add_leagues_db(db_leagues)
        league_titles = leagues.get_league_titles()

        return league_titles

    def get_members(self, league_title):
        player_names = self.database.get_player_names(league_title)

        members = Members(player_names)

        return members
        
    def get_songs_and_votes(self, league_title):
        print('Loading from database')
        rounds = Rounds()
        songs = Songs()
        votes = Votes()

        db_rounds = self.database.get_rounds(league_title)
        db_songs = self.database.get_songs(league_title)
        db_votes = self.database.get_votes(league_title)

        rounds.add_rounds_db(db_rounds)
        round_titles = rounds.get_titles()
        ##rounds.sort_titles(round_titles) <- need to ensure rounds are in correct order by adding dates to DB

        for round_title in round_titles:
            if self.database.check_data(league_title, round_title=round_title):
                songs.add_round_db(db_songs.query(f'round == "{round_title}"'))
                round_song_ids = songs.get_songs_ids(round_title)
                votes.add_round_db(db_votes.query(f'song_id in {round_song_ids}'))
            
        return songs, votes, rounds

    def check_songs_and_votes(self, songs, votes):
        # make sure there were songs and votes
        check = len(songs.df) > 0 # & (len(votes.df) > 0)
        return check

    def crunch_rounds(self, songs, votes, rounds, members, weights):
        print('Crunching rounds')
        # count how many submitted per round
        votes.name_rounds(songs)
        rounds.count_players(votes)

        # total points per song
        songs.add_patternizer(votes, members=members)
        songs.calculate_points(votes, rounds, weights)

        # list winners of each round
        rankings = Rankings(songs, votes, weights)

        return rankings

    def get_pulse(self, songs, votes, members, xy_=None):
        # get group pulse
        print('Getting pulse')

        pulse = Pulse(members)
        print('\t...likes')
        pulse.calculate_likers(songs, votes)
        print('\t...similarity')
        pulse.calculate_similarity(songs, votes)
        print('\t...wins')
        pulse.calculate_wins(songs, votes)

        # calculate distances
        print('Getting placements')

        print('\t...coordinates')
        members.update_coordinates(pulse, xy_=xy_)
        print('\t...likes')
        members.who_likes_whom(pulse)
        print('\t...dfc')
        members.get_dfc(songs, votes)
        print('\t...battle')
        members.battle(pulse)

        return pulse

    def get_coordinates(self, league_title):
        members = self.database.get_members(league_title)
        xy_ = members[['player', 'x', 'y']]
        return xy_

    def store_coordinates(self, league_title, members):
        print('Storing coordinates')
        members_df = members.get_members()
        print(members_df)
        self.database.store_members(members_df, league_title)
