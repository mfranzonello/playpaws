from results import Songs, Votes, Rounds, Leagues
from comparisons import Players, Rankings, Pulse

class Analyzer:
    def __init__(self, database):
        self.database = database
        self.weights = database.get_weights()

    def analyze_all(self):
        league_titles = self.get_league_titles()

        analyses = []
        for league_title in league_titles:
            analysis = self.analyze_league(league_title, summary=True)
            analyses.append(analysis)

        return analyses

    def analyze_league(self, league_title, summary=True):
        print(f'Setting up league {league_title}')
        players = self.get_players(league_title)

        weights = self.database.get_weights()

        songs, votes, rounds = self.get_songs_and_votes(league_title)

        rankings = self.crunch_rounds(songs, votes, rounds, players, weights)

        xy = self.get_coordinates(league_title)

        pulse = self.get_pulse(songs, votes, players, xy)

        self.store_coordinates(league_title, players)

        # display results
        if summary:           
            print(f'{league_title} Results\n')
            for df in [songs, rounds, pulse, players, rankings]:
                print(df)

        analysis = {'league_title': league_title,
                    'songs': songs,
                    'votes': votes,
                    'rounds': rounds,
                    'rankings': rankings,
                    'pulse': pulse,
                    }

        return analysis

    def get_league_titles(self):
        leagues = Leagues()
        db_leagues = self.database.get_leagues()
        leagues.add_leagues_db(db_leagues)
        league_titles = leagues.get_leagues()

        return league_titles

    def get_players(self, league_title):
        player_names = self.database.get_player_names(league_title)
        players = Players(player_names)

        return players
        
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
            songs.add_round_db(db_songs.query(f'round == "{round_title}"'))
            round_song_ids = songs.get_songs_ids(round_title)
            votes.add_round_db(db_votes.query(f'song_id in {round_song_ids}'))
            
        return songs, votes, rounds

    def crunch_rounds(self, songs, votes, rounds, players, weights):
        print('Crunching rounds')
        # count how many submitted per round
        votes.name_rounds(songs)
        rounds.count_players(votes)

        # total points per song
        songs.calculate_points(votes, rounds, players, weights)

        # list winners of each round
        rankings = Rankings(songs, votes, weights)

        return rankings

    def get_pulse(self, songs, votes, players, xy=None):
        # get group pulse
        print('Getting pulse')
    
        pulse = Pulse(players)
        print('...likes')
        pulse.calculate_likers(songs, votes)
        print('...similarity')
        pulse.calculate_similarity(songs, votes)
        print('...wins')
        pulse.calculate_wins(songs, votes)

        # calculate distances
        print('Getting placements')

        print('...coordinates')
        players.update_coordinates(pulse, xy=xy)
        print('...likes')
        players.who_likes_whom(pulse)
        print('...dfc')
        players.get_dfc(songs, votes)
        print('...battle')
        players.battle(pulse)

        return pulse

    def get_coordinates(self, league_title):
        members = self.database.get_members(league_title)
        xy = members[['player', 'x', 'y']]
        return xy

    def store_coordinates(self, league_title, players):
        members_df = players.get_members()
        self.database.store_members(members_df, league_title)
