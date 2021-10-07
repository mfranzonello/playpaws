from re import compile, UNICODE

class Texter:
    emoji_pattern = compile('['
                            u'\U0001F600-\U0001F64F' # emoticons
                            u'\U0001F300-\U0001F5FF' # symbols & pictographs
                            u'\U0001F680-\U0001F6FF' # transport & maps
                            u'\U0001F1E0-\U0001F1FF' # flags
                            u'\U00002500-\U00002BEF' # chinese char
                            u'\U00002702-\U000027B0'
                            u'\U00002702-\U000027B0'
                            u'\U000024C2-\U0001F251'
                            u'\U0001F926-\U0001F937'
                            u'\U00010000-\U0010FFFF'
                            u'\U000023E9-\U000023F3' # play, pause
                            ']+', flags=UNICODE)

    fonts = {'Segoe UI': 'fonts/segoeui.ttf'}

    emoji_fonts = {'Segoe UI Emoji': 'fonts/seguiemj.ttf'}

    bold_fonts = {'Segoe UI Semibold': 'fonts/seguisb.ttf'}

    def __init__(self):
        pass

    def clean_text(self, text:str) -> str:
        cleaned_text = self.emoji_pattern.sub(r'', text).strip()
        return cleaned_text

    def get_display_name(self, name:str) -> str:
        if ' ' in name:
            display_name = name[:name.index(' ')]
        elif '.' in name:
            display_name = name[:name.index('.')]
        else:
            display_name = name
        return display_name.title()

    def get_display_name_full(self, name):
        if (name == name.lower()) and ('.' in name) and (' ' not in name):
            display_name = name.replace('.', ' ')
        else:
            display_name = name
        return display_name.title()
