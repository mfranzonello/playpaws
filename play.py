from data import Database
from plotting import Printer, Plotter
from streaming import streamer
 
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

main()