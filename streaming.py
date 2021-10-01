import streamlit as st

class Streamer:
    def __init__(self):
        self.sidebar = st.sidebar
        
        self.selectbox = self.sidebar.empty() #selectbox()
        self.text_print = self.sidebar.empty()
        
        self.container = st.container()
        
        #self.text_print = st.empty()
        self.status_bar = st.empty()
        
    def pyplot(self, figure):
        #self.container.pyplot(figure)
        st.pyplot(figure)

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

