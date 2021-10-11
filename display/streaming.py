from pandas import set_option
import streamlit as st
from streamlit.components.v1 import html as st_html

from common.words import Texter

class Printer:
    def __init__(self, *options):
        self.options = [*options]

        for option in self.options:
            set_option(option, None)

class Streamable:
    def __init__(self):
        self.streamer = Streamer(deployed=False)

    def add_streamer(self, streamer):
        if streamer:
            self.streamer = streamer

class Streamer:
    def __init__(self, deployed=True):
        self.texter = Texter()

        self.deployed = deployed
        if self.deployed:
            self.sidebar = st.sidebar

            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    self.player_box = st.empty()
                with col2:
                    self.selectbox = st.empty()

            with st.container():
                self.status_bar = st.progress(0)
        
            #with self.sidebar.container():
            #    self.selectbox = st.empty() #selectbox('Loading app...', ['']) 
            
            ##self.status_bar = self.sidebar.progress(0)
        
            with self.sidebar.container():
                self.text_print = self.sidebar.empty()

            with self.sidebar.container():
                self.side_image = self.sidebar.empty()
        
            self.base_status = 0.0
            self.base_text = ''
               
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

    def set_side_image(self, image=None):
        if image:
            self.side_image.image(image, use_column_width=True)
        else:
            self.side_image = st.empty()

    def title(self, text, tooltip=None):
        st.title(text)
        self.tooltip(tooltip)
       
    def in_expander(self, in_expander, func, item, **args):
        if in_expander:
            with st.expander(label='', expanded=True):
                func(item, **args)
        else:
            func(item)

    def pyplot(self, figure, header=None, header2=None, tooltip=None, in_expander=True):
        self.wrapper(header, tooltip, header2=header2)
        self.in_expander(in_expander, st.pyplot, figure)
        
    def image(self, image, header=None, header2=None, tooltip=None, in_expander=True):
        self.wrapper(header, tooltip, header2=header2)
        self.in_expander(in_expander, st.image, image)

    def print(self, text, base=True):
        if self.deployed:
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
        if self.deployed:
            if base:
                self.base_status = pct
                new_pct = pct
            else:
                new_pct = min(1.0, self.base_status + pct)
                self.base_status = new_pct
            
            self.status_bar.progress(new_pct)

    def embed(self, html, height=150, header=None, header2=None, tooltip=None, in_expander=True):
        self.wrapper(header, tooltip, header2=header2)
        self.in_expander(in_expander, st_html, html, height=height)
        ##st_html(html, height=height)