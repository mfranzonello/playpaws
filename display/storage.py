''' Accessing and storing image data '''

from io import BytesIO
from random import randint

from dropbox import Dropbox
from google_images_search import GoogleImagesSearch

from common.secret import get_secret
from common.words import Texter

class Boxer:
    media_folder = '/media'
    covers_folder = f'{media_folder}/covers'
    masks_folder = f'{media_folder}/masks'
    clipart_folder = f'{media_folder}/clipart'
    mobi_folder = f'{media_folder}/mobi'

    def __init__(self):
        self.dbx = Dropbox(get_secret('DROPBOX_TOKEN'))

        self.texter = Texter()

        self.mobis = self.get_mobis()

    def get_mobis(self):
        folder = self.dbx.files_list_folder(self.mobi_folder)
        mobis = [entry.path_lower for entry in folder.entries]
        return mobis

    def file_in_folder(self, folder, name, ext):
        contents = self.dbx.files_list_folder(folder)
        filenames = [item.name.lower() for item in contents.entries]
        clean_name = self.texter.clean_text(name).lower() + f'.{ext}'

        if clean_name in filenames:
            path = f'{folder}/{filenames[filenames.index(clean_name)]}'
        else:
            path = None

        return path

    def get_bytes(self, path=None, folder=None, name=None, ext=None):
        if not path:
            path = self.file_in_folder(folder, name, ext)
        response = self.dbx.files_download(path) if path else None
        if response:
            _, result = response
            image_bytes = BytesIO(result.content)
        else:
            image_bytes = None

        return image_bytes

    def get_cover(self, name):
        image_bytes = self.get_bytes(folder=self.covers_folder, name=name, ext='jpg')
        
        return image_bytes

    def get_mask(self, name):
        image_bytes = self.get_bytes(folder=self.masks_folder, name=name, ext='png')

        return image_bytes

    def get_clipart(self, name):
        image_bytes = self.get_bytes(folder=self.clipart_folder, name=name, ext='png')

        return image_bytes

    def get_mobi(self):
        if len(self.mobis):
            # Mobi pictures available
            position = randint(0, len(self.mobis)-1)
            mobi_path = self.mobis.pop(position)
            image_bytes = self.get_bytes(path=mobi_path)
        else:
            # exhausted Mobi pictures, so reset
            self.mobis = self.get_mobis()
            image_bytes = self.get_mobi()

        return image_bytes

    def get_welcome(self):
        image_bytes = self.get_bytes(folder=self.clipart_folder, name='welcome', ext='jpg')

        return image_bytes

class Googler:
    def __init__(self):
        self.gis = GoogleImagesSearch(get_secret('GCP_API_KEY'), get_secret('GCP_PROJECT_CX'))

    def get_image_src(self, name):
        self.gis.search(search_params={'q': name, 'num': 1})
        results = self.gis.results()
        if len(results):
            src = results[0].url
        else:
            src = None

        return src