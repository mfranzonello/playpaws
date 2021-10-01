from data import Database
from analyze import Analyzer
from update import Updater
from plotting import Printer, Plotter
from streaming import streamer
 
def update_data(database):
    # scrape data from MusicLeague, Spotify and LastFM
    status_pre = 1

    updater = Updater(database)
    updater.update_database()
    updater.update_spotify()
    updater.update_lastfm()

    status_post = 1

    update_changed = status_pre != status_post

    return update_changed

def analyze_data(database):
    # analyze MusicLeague data
    status_pre = 1

    analyzer = Analyzer(database)
    analyzer.analyze_all()
    
    status_post = 1

    analysis_changed = status_pre != status_post

    return analysis_changed

def plot_data(database):
    # plot results of analysis
    plotter = Plotter(database)
    plotter.add_analyses()
    plotter.plot_results()

def main():
    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = Database('https://musicleague.app')
    
    plot_data(database)
    
    # update data in database from web
    update_changed = update_data(database)

    if update_changed:
        analysis_changed = analyze_data(database)

        if analysis_changed:
            plot_results()

main()


