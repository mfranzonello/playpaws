from urllib.request import urlopen
from base64 import b64encode
from io import BytesIO

from PIL import Image

class Byter:
    def __init__(self):
        pass

    def byte_me(self, image_url, extension='JPEG', size=(300, 300)):
        image = Image.open(urlopen(image_url)).resize(size)
        buffered = BytesIO()
        image.save(buffered, format=extension)
        image_b64 = b64encode(buffered.getvalue())

        return image_b64

class Gallery:
    def __init__(self, database):
        self.database = database
        self.players_df = self.database.get_players()
        self.images = self.download_images()
        self.crop_player_images()
               
    def download_images(self):
        images = {}

        streamer.status(0)
        streamer.print('Downloading profile images...')
        for i in self.players_df.index:
            player_name = self.players_df['player'][i]
            streamer.print(f'\t...{player_name}', base=False)

            # download image
            src = self.players_df['src'][i]
            if src[:len('http')] != 'http':
                src = f'https://{src}'
            fp = urlopen(src)

            try:
                # see if image can load
                image = Image.open(fp)
            except UnidentifiedImageError:
                # image is unloadable
                streamer.print(f'...unable to read image for {player_name}', base=False)
                image = None

            # store in images dictionary
            images[player_name] = image

            streamer.status(i/len(self.players_df))
            
        return images

    def get_player_image(self, player_name):
        image = self.images[player_name]

        return image