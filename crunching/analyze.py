''' Number crunching for round results '''

from crunching.comparisons import Members, Pulse

class Analyzer:
    def __init__(self, database):
        self.database = database

    def place_all(self):
        league_ids = self.database.get_league_ids()

        for league_id in league_ids:
            if self.database.check_data(league_id):
                league_title = self.database.get_league_name(league_id)
                # place
                if self.database.get_optimized(league_id):
                    print(f'Placements for {league_title} already up to date')

                else:
                    placement = self.place_league(league_id, league_title)

                    if placement:
                        self.database.store_members(placement['members'], league_id)
                        self.database.store_optimizations(league_id, self.version, optimized=placement['optimized'])
        
    def place_league(self, league_id, league_title):
        print(f'Placing members in league {league_title}')
        
        if self.database.check_data(league_id):
            print(f'\t...analyzing {league_title}')

            placement = self.get_placements(league_id)

        else:
            print(f'\t...no data for {league_title}')
            placement = None

        return placement

    def get_placements(self, league_id):
        # get group pulse
        print('Getting pulse')
        print('\t...coordinates')

        members_df = self.database.get_members(league_id)
        xy_ = members_df[['player_id', 'x', 'y']]
        members = Members(members_df)

        distances_df = self.database.get_distances(league_id)
        pulse = Pulse(distances_df)

        members.update_coordinates(pulse, xy_=xy_)
        members_df = members.get_members()
        
        placement = {'members': members.df, 'optimized': members.coordinates['success']}

        return placement