import re
from fuzzywuzzy import fuzz

class Texter:
    emoji_pattern = re.compile('['
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
                                ']+', flags=re.UNICODE)

    sans_fonts = {'Segoe UI': 'segoeui.ttf'}
    emoji_fonts = {'Segoe UI Emoji': 'seguiemj.ttf'}
    bold_fonts = {'Segoe UI Semibold': 'seguisb.ttf'}

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

    def slashable(self, char):
        slash_chars = ['[', '(', ']', ')', '.', '\\', '-']
        slash = '\\' if char in slash_chars else ''
        return slash

    def remove_parenthetical(self, text, words, position, parentheses=[['(', ')'], ['[', ']']],
                             middle=None, case_sensitive=False):
        captured = None
        if text:

            punctuation = '!,.;()/\\-[]'
            punctuation_s = '^' + punctuation
            punctuation_e = punctuation + '$'

            if parentheses == 'all':
                parentheses = [[start, end] for start in punctuation_s for end in punctuation_e]
            elif parentheses == 'all_start':
                [[start, ''] for end in punctuation_s]
            elif parentheses == 'all_end':
                parentheses = [['', end] for end in punctuation_e]

            capture_s = '(.*?)' if position == 'end' else ''
            capture_e = '(.*?)' if position == 'start' else ''
            capture_m = f'.*?{middle}.*?' if middle else ''

            pattern = '|'.join(f'({self.slashable(s)}{s}{capture_s}'
                               f'{w}{capture_m}'
                               f'{capture_e}{self.slashable(e)}{e})' for w in words for s, e in parentheses)
        
            flags = re.IGNORECASE if not case_sensitive else 0

            searched = re.search(pattern, text, flags=flags)
            if searched:
                # find the shortest captured text
                captured = sorted((s for s in searched.groups()[1::2] if s), key=len)[0].strip()
                replaceable = sorted((s for s in searched.groups()[0::2] if s), key=len)[0].strip()
                
                text = text.replace(replaceable, '').strip()
            
        return text, captured

    def drop_dash(self, text):
        # remove description after dash
        if ' - ' in text:
            p = text.find(' - ')
            if p > 0 and not ((text[:p].count('(') > text[:p].count(')')) and (text[p:].find(')') > text[p:].find('('))):
                # don't drop if - is in between parenthesis
                text = text[:text.find(' - ')].strip()

        return text

    def split_long_text(self, text, limit=100):
        if text:
            splits = text.split('\n')

            split_text = []
            for sp in splits:
                split_t = []
                p = 0
                iterations = 0

                while (p < len(sp)):
                    last_space = sp[p:p+limit][::-1].find(' ')
            
                    if last_space == -1:
                        cutoff = min(len(sp), p+limit)
                    else:
                        cutoff = limit - last_space + p
                               
                    split_t += [sp[p:cutoff]]
                    p = cutoff
            
                split_text += ['\n'.join(split_t)]

            text = '\n\n'.join(split_text)

        return text

    def find_closest_match(self, text, texts, threshold=40, similarity=5):
        if (not text) or (text in texts):
            closest_text = text

        else:
            ratios = [fuzz.ratio(text.lower(), t.lower()) for t in texts]
            max_ratio = max(ratios)
            if max_ratio >= threshold:
                all_sorted = [(r, t) for r, t in sorted(zip(ratios, texts), reverse=True) if (r >= max_ratio - similarity)]
                ratios_sorted = [r for r, t in all_sorted]
                matches_sorted = [t for r, t in all_sorted]

                if (len(matches_sorted) == 1) or (similarity == 0):
                    # only one value is close
                    closest_text = matches_sorted[0]
                elif similarity > 0:
                    # look at abbreviations
                    closest_text = self.find_closest_match(self.abbreviate_name(text), texts,
                                                           threshold=threshold, similarity=0)
                   
            else:
                closest_text = text

        return closest_text

    def abbreviate_name(self, text):
        punctuation = '- .'
        pattern = '|'.join(f'{self.slashable(p)}{p}' for p in punctuation)
        abbreviation = ''.join(s if i == 0 else s[0] for i, s in enumerate(re.split(pattern, text)))
        return abbreviation