from urllib.request import urlopen
from urllib.error import HTTPError
from base64 import b64encode
from io import BytesIO

from PIL import Image, ImageDraw, ImageOps, UnidentifiedImageError

from common.words import Texter
from display.streaming import Streamer

class Byter:
    def __init__(self):
        pass

    def bit_me(self, image, size=None):
        if size:
            image = image.resize(size)
        file_object = BytesIO()
        image.save(file_object, 'PNG')

        return file_object

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

        if size:
            # convert to specific size
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

        if image.mode == 'RGBA':
            image = Image.Image.convert(image, 'RGB')

        buffered = BytesIO()
        image.save(buffered, format=extension)
        image_b64 = b64encode(buffered.getvalue())
        buffered.seek(0)

        return image_b64

class Imager:
    def __init__(self):
        self.antialias = 2
        self.images = {}
        self.streamer = Streamer(deployed=False)

    def get_color_image(self, color, size):
        image = self.crop_image(Image.new('RGB', size, color))

        return image
    
    def crop_image(self, image, antialias=True):
        if image:
            a = self.antialias if antialias else 1
            w0, h0 = image.size
            image = image.resize((a*w0, a*h0))
            W, H = image.size
            if W != H:
                # crop to square
                wh = min(W, H)
                left = (W - wh)/2
                right = (W + wh)/2
                top = (H - wh)/2
                bottom = (H + wh)/2
                image = image.crop((left, top, right, bottom))

            mask = Image.new('L', (W, H), 0)
            drawing = ImageDraw.Draw(mask)
            drawing.ellipse((0, 0) + (W, H), fill=255)
            cropped = ImageOps.fit(image, mask.size, centering=(0.5, 0.5))
            cropped.putalpha(mask)
            cropped = cropped.resize((w0, h0), resample=Image.ANTIALIAS)
            
        else:
            cropped = None

        return cropped
    
class Gallery(Imager):
    def __init__(self, database, streamer=None, download_all=False, crop=False):
        super().__init__()
        self.database = database
        self.streamer = streamer if streamer else Streamer(deployed=False)

        self.players_df = self.database.get_players()

        self.crop = crop

        self.images = self.download_images() if download_all else {}
        
    def get_image(self, name):
        if (name not in self.images) and (name in self.players_df['player'].values):
            self.download_image(name)

        image = self.images.get(name)

        return image

    def store_image(self, name, image):
        self.images[name] = image
   
    def download_image(self, name):
        image_key = ('gallery', name)
        image, ok = self.streamer.get_session_state(image_key)
        if not ok:
            self.streamer.print(f'\t...downloading image for {name}', base=False)

            # download image
            src = self.players_df[self.players_df['player']==name]['src'].iloc[0]
            if src:
                # Spotify profile image exists
                if src[:len('http')] != 'http':
                    src = f'https://{src}'

                try:
                    # see if image can load
                    image = Image.open(urlopen(src))

                    if self.crop:
                        image = self.crop_image(image)

                except UnidentifiedImageError:
                    # image is unloadable
                    self.streamer.print(f'...unable to read image for {name}', base=False)
                    image = None

                except HTTPError:
                    #  image is unreachable
                    self.streamer.print(f'...image is expired for {name}', base=False)
                    self.database.flag_player_image(name)
                    image = None

            else:
                # no Spotify profile image exists
                image = None

        # store in images dictionary
        self.images[name] = image

    def download_images(self):
        images = {}

        self.streamer.status(0)
        self.streamer.print('Downloading profile images...')
        for i in self.players_df.index:
            self.download_image(self.players_df['player'][i])

            self.streamer.status(i/len(self.players_df))
            
        return images

    def crop_player_images(self):
        for player_name in self.images:
            self.images[player_name] = self.crop_image(self.images[player_name])
