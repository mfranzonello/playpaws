from common.data import Database
from display.plotting import Plotter
from display.streaming import Printer, Streamer
 
def plot_data(database, streamer):
    # plot results of analysis
    plotter = Plotter(database, streamer)
    plotter.add_analyses()
    plotter.plot_results()

def main():
    printer = Printer()
    streamer = Streamer()

    # prepare database
    database = Database(streamer=streamer)
    
    plot_data(database, streamer)

if __name__ == '__main__':
    main()