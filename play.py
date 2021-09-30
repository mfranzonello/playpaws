import streamlit as st

from structure import Setter
from data import Database
from analyze import Analyzer
from update import Updater
from plotting import Printer, Plotter
from streaming import streamer

#@st.cache(suppress_st_warning=True)
def connect_to_database():
    database = Database(setter.server, setter.structure)
    return database

def main(update_db=True, analyze_data=True, plot_data=True):
    setter = Setter()

    printer = Printer('display.max_columns', 'display.max_rows')

    # prepare database
    database = connect_to_database()
    #database = Database(setter.server, setter.structure) #credentials
    
    # update data in database from web
    if update_db:
        updater = Updater(database, setter.structure) #, credentials)
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
        plotter.add_analyses()
        plotter.plot_results()

main(update_db=False, analyze_data=False, plot_data=True)
