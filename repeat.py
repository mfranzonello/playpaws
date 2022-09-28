from preparing.extract import Scraper
from common.data import Database

def update_rounds():
    scraper = Scraper()
    database = Database()

    inactive_players = database.get_inactive_players()
    league_ids = database.get_extendable_leagues()

    for league_id in league_ids:
        # extend cascading deadlines when the open round has a deadline coming up and outstanding voters
        scraper.extend_deadlines(league_id, inactive_players=inactive_players, days=1, hours_left=24)

def main():
    update_rounds()

if __name__ == '__main__':
    main()