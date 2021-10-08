from pandas import set_option
import streamlit as st

from media import Texter

class Printer:
    def __init__(self, *options):
        self.options = [*options]

        for option in self.options:
            set_option(option, None)

class Streamer:
    def __init__(self):
        self.sidebar = st.sidebar
        
        self.selectbox = self.sidebar.empty() #selectbox()
        self.status_bar = self.sidebar.progress(0)
        self.base_status = 0.0
        
        self.text_print = self.sidebar.empty()
        self.base_text = ''

        self.texter = Texter()
               
    def title(self, text):
        st.title(text)
        
    def pyplot(self, figure, header=None, tooltip=None):
        if header:
            st.header(header)
        st.pyplot(figure, help=self.texter.split_long_text(tooltip))
        st.write('\n')

    def print(self, text, base=True):
        if base:
            self.base_text = text
            new_text = text
        else:
            new_text = self.base_text + ' ' + text
        
        # print to Streamlit
        self.text_print.write(new_text)

        # print to cmd
        print(text)

    def clear_printer(self):
        self.text_print.empty()

    def status(self, pct, base=False):
        if base:
            self.base_status = pct
            new_pct = pct
        else:
            new_pct = min(1.0, self.base_status + pct)
            self.base_status = new_pct
            
        self.status_bar.progress(new_pct)

streamer = Streamer()

