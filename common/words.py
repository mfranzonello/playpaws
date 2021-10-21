import re
from thefuzz import process

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
                                u'\U00002049' # ?!
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
        display_name = self.get_display_name_full(name).split()[0]
        return display_name

    def get_display_name_full(self, name):
        punctuation = ['.', '_']
        if (name == name.lower()) and any(p in name for p in punctuation) and (' ' not in name):
            for p in punctuation:
                display_name = name.replace(p, ' ')
        else:
            display_name = name

        if not any(str(n) in name for n in range(10)):
            display_name = display_name.title()

        return display_name

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
                [[start, ''] for start in punctuation_s]
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
        if text[-2] == ' -':
            text = text[:-2]

        return text

    def split_long_text(self, text, limit=100):
        # break up long text with newlines
        if text:
            splits = text.split('\n')

            split_text = []
            for sp in splits:
                split_t = []
                p = 0

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
            # text is None or text is already a match
            closest_text = text

        else:
            # find fuzzy match ratios
            matches = list(filter(lambda x: x[1] > threshold,
                                  process.extract(text.lower(), [t.lower() for t in texts], limit=10)))

            if (len(matches) == 1) or (similarity == 0):
                # only one matching result or being forced to choose best match
                closest_text = matches[0][0]

            elif similarity > 0:
                # more than one result and others that are close
                texts_sublist = [m[0] for m in matches if m[1] >= (matches[0][1] - similarity)]
                closest_text = self.find_closest_match(self.abbreviate_name(text), texts_sublist,
                                                      threshold=threshold, similarity=0)

            else:
                # no match
                closest_text = text

        return closest_text

    def abbreviate_name(self, text):
        # return any words after the first word as initials
        punctuation = '- .'
        pattern = '|'.join(f'{self.slashable(p)}{p}' for p in punctuation)
        abbreviation = ''.join(s if i == 0 else s[0] for i, s in enumerate(re.split(pattern, text)))

        return abbreviation

    def get_plurals(self, text, markdown=None):
        plurals = {}
        if markdown:
            text = [f'{markdown}{t}{markdown[::-1]}' for t in text]

        if text is not None:
            if len(text) == 1:
                plurals['be'] = 'is'
                plurals['s'] = ''
                plurals['text'] = text[0]
            else:
                plurals['be'] = 'are'
                plurals['s'] = 's'
                plurals['text'] = ', '.join(text[:-1]) + ' and ' + text[-1]
        
        return plurals

    def get_times(self, time, markdown=None):
        # time in minutes
        day = max(0, time // (60*24))
        hour = max(0, (time - 60*24*day) // 60)
        minute = max(0, round(time - 60*24*day - 60*hour))
        times = {'day': int(day), 'hour': int(hour), 'minute': int(minute)}
        
        durations = [f'{times[t]} {t}{"s" if times[t] > 1 else ""}' for t in times if times[t] > 0]
        if markdown:
            durations = [f'{markdown}{d}{markdown[::-1]}' for d in durations]

        duration = ', '.join(durations[:-1]) + ' and ' + durations[-1]
        
        return duration

    def match_emoji(self, text, emojis, default=''):
        emoji = None
        for key, value in emojis.items():
            for v in value:
                if v.lower() in [t.lower() for t in text.replace(',', ' ').replace('.', ' ').split()]:
                    emoji = key
                    break
            if emoji:
                break

        if emoji is None:
            emoji = default

        return emoji

    def get_ordinal(self, num):
        p = 1 if num >= 0 else -1
        n = abs(num)
        digit = int(n - n//10*10) * p
        two_digits = int(n - n//100*100) * p

        if (digit == 1) and (two_digits != 11):
            nth = 'st'
        elif (digit == 2) and (two_digits != 12):
            nth = 'nd'
        elif (digit == 3) and (two_digits != 13):
            nth = 'rd'
        else:
            nth = 'th'

        ordinal = f'{num}{nth}'

        return ordinal