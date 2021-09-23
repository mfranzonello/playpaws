from structure import Setter
from data import Database
from analyze import Analyzer
from update import Updater
from plotting import Printer, Plotter
from secret import credentials

def main():
    setter = Setter(update_db=True)
    settings, server, structure = setter.get_settings()

    update_db = settings.get('update_db', False)

    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = Database(credentials[server['db_name']], structure)
    
    # update data in database from web or local
    if update_db:
        updater = Updater(database, structure, credentials, settings)
        updater.update_database()
        ##updater.turn_off()
        updater.update_spotify()
        updater.update_lastfm()

    # analyze data
    analyzer = Analyzer(database)
    analyzer.analyze_all()
    
    # plot results for all leagues
    plotter = Plotter(database)
    plotter.add_anaylses()
    plotter.plot_results()

main()