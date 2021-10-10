from pandas import set_option
import streamlit as st
from streamlit.components.v1 import html as st_html

from words import Texter

class Printer:
    def __init__(self, *options):
        self.options = [*options]

        for option in self.options:
            set_option(option, None)

class Streamer:
    def __init__(self):
        self.sidebar = st.sidebar
        
        self.selectbox = self.sidebar.empty() #selectbox('Loading app...', ['']) 
        self.status_bar = self.sidebar.progress(0)
        self.base_status = 0.0
        
        self.text_print = self.sidebar.empty()#write('')
        self.base_text = ''

        self.texter = Texter()
               
    def wrapper(self, header, tooltip, header2=None):
        self.header(header)
        self.header2(header2)
        self.tooltip(tooltip)
        
    def header(self, header):
        if header:
            st.header(header)

    def header2(self, header2):
        if header2:
            st.write(f'**{header2}**')

    def tooltip(self, tooltip):
        if tooltip:
            with st.expander(tooltip['label']):
                st.write(tooltip['content'])

    def title(self, text, tooltip=None):
        st.title(text)
        self.tooltip(tooltip)
       
    def pyplot(self, figure, header=None, header2=None, tooltip=None):
        self.wrapper(header, tooltip, header2=header2)
        st.pyplot(figure)
        
    def image(self, image, header=None, header2=None, tooltip=None):
        self.wrapper(header, tooltip, header2=header2)
        st.image(image)

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
        self.text_print.write('')

    def status(self, pct, base=False):
        if base:
            self.base_status = pct
            new_pct = pct
        else:
            new_pct = min(1.0, self.base_status + pct)
            self.base_status = new_pct
            
        self.status_bar.progress(new_pct)

    def embed(self, html, height=150, header=None, header2=None, tooltip=None):
        self.wrapper(header, tooltip, header2=header2)
        st_html(html, height=height)

streamer = Streamer()

