import logging
import warnings
import os

from pandas import set_option

class Printer:
    annoying_loggers = ['googleapiclient.discovery_cache',
                        'fuzz',
                        'streamlit.logger',
                        'streamlit.config',
                        'dropbox']

    def __init__(self, *options):
        self.options = [*options] if len(options) else ['display.max_columns', 'display.max_rows']

        for option in self.options:
            set_option(option, None)

    def clear_screen(self):
        os.system('cls')

    def silence(self):
        warnings.filterwarnings('ignore')
        for logger in self.annoying_loggers:
            logging.getLogger(logger).setLevel(logging.ERROR)

printer = Printer()
printer.silence()
printer.clear_screen()