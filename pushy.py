''' Manual override from JSON to fix league misses '''

import json

from preparing.update import Extender

def reopen_rounds():
    extender = Extender(database=None)
    jason = './jsons/reopens.json'
    with open(jason, 'r+') as f:
        reopens  = json.load(f)

    for league_id in reopens:
        round_id = reopens[league_id]
        extender.reopen_round(league_id, round_id)
        extender.reopen_league(league_id)
        
    with open(jason, 'r+') as f:
        f.seek(0)
        json.dump({}, f)
        f.truncate()

def main():
    reopen_rounds()

if __name__ == '__main__':
    main()