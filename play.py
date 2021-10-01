from data import Database
from analyze import Analyzer
from plotting import Printer, Plotter
from streaming import streamer
 
def analyze_data(database):
    # analyze MusicLeague data
    analyzer = Analyzer(database)
    analyzer.analyze_all()
    
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
    
    # analyze data
    analyze_data(database)

main()