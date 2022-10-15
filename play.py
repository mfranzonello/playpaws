''' Shows results using Streamlit '''

from common.data import Database
from display.plotting import Plotter
from display.streaming import Printer, Streamer
 
def plot_data(database, streamer):
    # plot results of analysis
    plotter = Plotter(database, streamer)
    plotter.add_data()
    plotter.plot_results()

def main():
    printer = Printer()
    printer.clear_screen()
    streamer = Streamer()

    # prepare database
    database = Database()
    
    plot_data(database, streamer)

if __name__ == '__main__':
    main()