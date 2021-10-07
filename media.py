from urllib.request import urlopen
from base64 import b64encode
from io import BytesIO
from re import compile, UNICODE

from PIL import Image, ImageDraw, ImageOps, UnidentifiedImageError

from streaming import streamer

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
        slash_chars = ['[', '(', ']', ')', '.']
        slash = '\\' if char in slash_chars else ''
        return slash

    def remove_parenthetical(self, text, words, position, parentheses=[['(', ')'], ['[', ']']], middle=None):
        capture_s = '(.*?)' if position == 'end' else ''
        capture_e = '(.*?)' if position == 'start' else ''
        capture_m = f'.*?{middle}.*?' if middle else ''

        pattern = '|'.join(f'({self.slashable(s)}{s}{capture_s}'
                           f'{w}{capture_m}'
                           f'{capture_e}{self.slashable(e)}{e})' for w in words for s, e in parentheses)
        
        searched = re.search(pattern, text, flags=re.IGNORECASE)
        if searched:
            captured = next(s for s in searched.groups() if s).strip()
            text = text.replace(captured, '').strip()
            
        else:
            captured = None

        return text, captured

    def drop_dash(self, text):
        # remove description after dash
        if ' - ' in text:
            text = text[:text.find(' - ')].strip()
        return text


class Byter:
    def __init__(self):
        pass

    def byte_me(self, image_src, extension='JPEG', size=(300, 300),
                overlay=None, overlay_pct=0.5):
        """Convert image and overlay to bytes object"""
        # check image source type
        if isinstance(image_src, str):
            image = Image.open(urlopen(image_src))
        
        elif isinstance(image_src, BytesIO):
            image = Image.open(image_src)
        
        else:
            image = image_src

        # convert to 300 x 300
        image = image.resize(size)

        # check if there is an overlay
        if overlay:
            # check overlay source type
            if isinstance(overlay, str):
                print(overlay)
                overlay_image = Image.open(urlopen(overlay))
                
            elif isinstance(overlay, BytesIO):
                overlay_image = Image.open(overlay)

            else:
                overlay_image = overlay

            W, H = image.size
            w_0, h_0 = overlay_image.size
            max_wh = max(w_0, h_0)
            resize = (int(overlay_pct * w_0 / max_wh * W),
                      int(overlay_pct * h_0 / max_wh * H))

            overlay_resize = overlay_image.resize(resize)
            w_1, h_1 = overlay_resize.size
            image.paste(overlay_resize, ((W-w_1)//2, (H-h_1)//2), overlay_resize)

        buffered = BytesIO()
        image.save(buffered, format=extension)
        image_b64 = b64encode(buffered.getvalue())

        return image_b64

class Imager:
    def __init__(self):
        self.images = {}

    def get_color_image(self, color, size):
        image = self.crop_image(Image.new('RGB', size, color))

        return image
    
    def crop_image(self, image):
        if image:
            W, H = image.size
            if W != H:
                # crop to square
                wh = min(W, H)
                left = (W - wh)/2
                right = (W + wh)/2
                top = (H - wh)/2
                bottom = (H + wh)/2
                image = image.crop((left, top, right, bottom))

            mask = Image.new('L', image.size, 0)
            drawing = ImageDraw.Draw(mask)
            drawing.ellipse((0, 0) + image.size, fill=255)
            cropped = ImageOps.fit(image, mask.size, centering=(0.5, 0.5))
            cropped.putalpha(mask)

        else:
            cropped = None

        return cropped

class Gallery(Imager):
    def __init__(self, database, download_all=False, crop=False):
        super().__init__()
        self.database = database
        self.players_df = self.database.get_players()

        self.crop = crop

        self.images = self.download_images() if download_all else {}
        
    def get_image(self, name):
        if name not in self.images:
            self.download_image(name)

        image = self.images.get(name)

        return image

    def store_image(self, name, image):
        self.images[name] = image
   
    def download_image(self, name):
        streamer.print(f'\t...{name}', base=False)

        # download image
        src = self.players_df[self.players_df['player']==name]['src'].iloc[0]
        if src:
            # Spotify profile image exists
            if src[:len('http')] != 'http':
                src = f'https://{src}'
            fp = urlopen(src)

            try:
                # see if image can load
                image = Image.open(fp)

                if self.crop:
                    image = self.crop_image(image)

            except UnidentifiedImageError:
                # image is unloadable
                streamer.print(f'...unable to read image for {name}', base=False)
                image = None

        else:
            # no Spotify profile image exists
            image = None

        # store in images dictionary
        self.images[name] = image

    def download_images(self):
        images = {}

        streamer.status(0)
        streamer.print('Downloading profile images...')
        for i in self.players_df.index:
            self.download_image(self.players_df['player'][i])

            streamer.status(i/len(self.players_df))
            
        return images

    def crop_player_images(self):
        for player_name in self.images:
            self.images[player_name] = self.crop_image(self.images[player_name])
