from structure import Setter
from data import Database
from analyze import Analyzer
from update import Updater
from plotting import Printer, Plotter
from secret import credentials
from streaming import streamer

def main(update_db=True, analyze_data=True, plot_data=True):
    setter = Setter()
    if not setter.connected:
        streamer.print('No network connection!')

    else:
        server, structure = setter.get_settings()

        printer = Printer('display.max_columns', 'display.max_rows')

        # prepare database
        database = Database(credentials, server, structure)
    
        # update data in database from web
        if update_db:
            updater = Updater(database, structure, credentials)
            updater.update_database()
            updater.update_spotify()
            updater.update_lastfm()

        # analyze data
        if analyze_data:
            analyzer = Analyzer(database)
            analyzer.analyze_all()
    
        # plot results for all leagues
        if plot_data:
            plotter = Plotter(database)
            plotter.add_anaylses()
            plotter.plot_results()

main(update_db=False, analyze_data=False, plot_data=True)