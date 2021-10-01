import streamlit as st

class Streamer:
    def __init__(self):
        self.sidebar = st.sidebar.write('Loading PlawPays MusicLeague analyzer...')
        
        self.members_plot = st.empty()
        self.boards_plot = st.empty()
        self.rankings_plot = st.empty()
        self.features_plot = st.empty()
        self.tags_plot = st.empty()
        self.top_songs_plot = st.empty()
        
        self.text_print = st.empty()
        self.status_bar = st.empty()
        

    def print(self, text):
        # print to Streamlit
        self.text_print.text(text)

        # print to cmd
        print(text)

    def clear_printer(self):
        self.text_print.empty()

    def update_status(self, pct):
        self.status_bar.progress(pct)

streamer = Streamer()

