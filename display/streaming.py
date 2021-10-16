from pandas import set_option
import streamlit as st
from streamlit.components.v1 import html as st_html

from common.words import Texter

def cache(**args):
    ##hash_funcs = {t: lambda _: None for t in ['_thread.RLock',
    ##                                          '_thread.lock',
    ##                                          'builtins.PyCapsule',
    ##                                          '_io.TextIOWrapper',
    ##                                          'builtins.weakref',
    ##                                          'builtins.dict',
    ##                                          'streamlit.delta_generator'
    ##                                          ]}
    return st.cache(**args) #hash_funcs=hash_funcs, 

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

    def __hash__(self):
        return hash((self.streamer.deployed))

class Streamer:
    def __init__(self, deployed=True):
        ##print(st.get_option('theme.primaryColor'))
        self.texter = Texter()

        self.deployed = deployed
        if self.deployed:
            st.set_page_config(page_title='MobiMusic',
                               page_icon='headphones',
                               )

            self.sidebar = st.sidebar

            with st.container():
                col1, col2 = st.columns(2)
                with col1:
                    self.player_box = st.empty()
                with col2:
                    self.selectbox = st.empty()

            with st.container():
                self.status_bar = st.progress(0)
        
            with st.container():
                col3, col4 = st.columns(2)
                with col3:
                    self.player_image = st.empty()
                    self.player_footer = st.empty()
                with col4:
                    self.player_caption = st.empty()
        
            with self.sidebar.container():
                self.text_print = self.sidebar.empty()

            with self.sidebar.container():
                self.side_image = self.sidebar.empty()
        
            self.base_status = 0.0
            self.base_text = ''

    def get_session_state(self, key):
        item = st.session_state.get(key)
        ok = (item is not None)
        return item, ok

    def store_session_state(self, key, item):
        st.session_state[key] = item
               
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
                st.markdown(tooltip['content'], unsafe_allow_html=True)
                
    def sidebar_image(self, image=None):
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
            func(item, **args)

    def right_column(self, right_column, func1, item1, func2, **args):
        if right_column:
            col_left, col_right = st.columns(2)
            with col_left:
                func1(item1, **args)
            with col_right:
                func2(right_column)
        
    def plotly(self, figure, header=None, header2=None, tooltip=None, in_expander=False):
        self.wrapper(header, tooltip, header2=header2)
        self.in_expander(in_expander, st.ploty_chart, figure, transparent=True)

    def pyplot(self, figure, header=None, header2=None, tooltip=None, in_expander=False):
        self.wrapper(header, tooltip, header2=header2)
        self.in_expander(in_expander, st.pyplot, figure, transparent=True)
        
    def viewer(self, image, footer=None, footer_height=None):
        self.player_image.image(image)
        ##if footer:
        ##    self.player_footer.st_html(footer, height=footer_height)

    def image(self, image, header=None, header2=None, tooltip=None, right_column=None,
              in_expander=False):
        self.wrapper(header, tooltip, header2=header2)
        if right_column:
            self.right_column(right_column, st.image, image, st.markdown)
        else:
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

    def embed(self, html, height=150, header=None, header2=None, tooltip=None, in_expander=False):
        self.wrapper(header, tooltip, header2=header2)
        self.in_expander(in_expander, st_html, html, height=height)
        ##st_html(html, height=height)