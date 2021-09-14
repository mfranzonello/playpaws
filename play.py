#from itertools import filterfalse
from os import getlogin

from data import Database
from analyze import Analyzer
from update import Updater
from plotting import Printer, Plotter
from secret import credentials

# inputs
## have local be based on if there is a network connection
settings = {'local': False,
            'silent': False,
            'update_db': False,
            }
# structure
directory = f'C:/Users/{getlogin()}/OneDrive/Projects/Play Paws'
html_folder = 'html'
structure = {'local': {'home': f'{directory}/{html_folder}/home',
                       'league': f'{directory}/{html_folder}/leagues',
                       'round': f'{directory}/{html_folder}/rounds',
                       },
             'web': {'chrome_driver': f'C:/Users/{getlogin()}/OneDrive/Projects/Play Paws/Play Paws/Play Paws/',
                     'chrome_profile': f'C:/Users/{getlogin()}/AppData/Local/Google/Chrome/User Data',
                     'main_url': 'https://musicleague.app',
                     },
             }

db_server = ['sqlite', 'bitio'][1]

def main():
    local = settings.get('local', False)
    silent = settings.get('silent', False)
    update_db = settings.get('update_db', False)

    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = Database(credentials[db_server], directories=structure['local'])
    
    # update data in database from web or local
    if update_db:
        updater = Updater(database, structure, credentials, silent)
        updater.update_database()
        updater.turn_off()

    # analyze data
    analyzer = Analyzer(database)
    analyses = analyzer.analyze_all()
    
    # plot results for all leagues
    plotter = Plotter()
    plotter.add_anaylses(analyses)
    plotter.plot_results()

main()