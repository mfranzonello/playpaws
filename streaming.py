import streamlit

class Streamer:
    def __init__(self):
        self.text_print = streamlit.empty()
        self.status_bar = streamlit.empty()

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

