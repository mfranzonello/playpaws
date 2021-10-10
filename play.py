from common.data import Database
from display.plotting import Plotter
from display.streaming import Printer#, streamer
 
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