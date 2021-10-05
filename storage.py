from os import getenv
from dropbox import Dropbox

from plotting import Texter

class Boxer:
    media_folder = '/media'
    covers_folder = f'{media_folder}/covers'
    masks_folder = f'{media_folder}/masks'
    clipart_folder = f'{media_folder}/clipart'

    def __init__(self):
        self.dbx = Dropbox(getenv('DROPBOX_TOKEN'))

        self.texter = Texter()

    def file_in_folder(self, folder, name, ext):
        contents = self.dbx.files_list_folder(folder)
        filenames = [item.name.lower() for item in contents.entries]
        clean_name = self.texter.clean_text(name).lower() + f'.{ext}'

        if clean_name in filenames:
            path = f'{folder}/{filenames[filenames.index(clean_name)]}'
        else:
            path = None

        return path

    def get_url(self, folder, name, ext):
        path = self.file_in_folder(folder, name, ext)
        url = self.dbx.files_get_temporary_link(path).link if path else None
        
        return url

    def get_cover(self, name):
        url = self.get_url(self.covers_folder, name, 'jpg')
        
        return url

    def get_mask(self, name):
        url = self.get_url(self.masks_folder, name, 'png')

        return url

    def get_clipart(self, name):
        url = self.get_url(self.clipart_folder, name, 'png')

        return url