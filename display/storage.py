''' Accessing and storing image data '''

from io import BytesIO
from random import randint
import pickle
from bz2 import compress, decompress

from dropbox import Dropbox
from google_images_search import GoogleImagesSearch
from google.oauth2.service_account import Credentials as SACredentials
from google.cloud.storage import Client

from common.secret import get_secret
from common.locations import GCP_TOKEN_URI, GCP_AUTH_URL, GCP_APIS_URL
from common.structure import GCP_S_PROJECT_ID, GCP_S_ACCOUNT_NAME
from common.words import Texter
from common.calling import Caller

class Boxer:
    ''' retrieve Dropbox media '''
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

class GImager:
    ''' retrieve Google images '''
    def __init__(self):
        self.gis = GoogleImagesSearch(get_secret('GCP_I_API_KEY'), get_secret('GCP_I_PROJECT_CX'))

    def get_image_src(self, name):
        self.gis.search(search_params={'q': name, 'num': 1})
        results = self.gis.results()
        if len(results):
            src = results[0].url
        else:
            src = None

        return src

class GClouder(Caller):
    ''' store and retrieve objects '''
    def __init__(self, bucket_name='playpaws'):
        super().__init__()

        gcp_s_email = f'{GCP_S_ACCOUNT_NAME}-service@{GCP_S_PROJECT_ID}.iam.gserviceaccount.com'
        gcp_service_account = gcp_s_email.replace('@', '%40')

        info = {'type': 'service_account',
                'project_id': GCP_S_PROJECT_ID,
                'private_key_id': get_secret('GCP_S_PRIVATE_KEY_ID'),
                'private_key': get_secret('GCP_S_PRIVATE_KEY').replace('\\n', '\n'),
                'client_email': gcp_s_email,
                'client_id': get_secret('GCP_S_CLIENT_ID'),
                'auth_uri': f'{GCP_AUTH_URL}/o/oauth2/auth',
                'token_uri': GCP_TOKEN_URI,
                'auth_provider_x509_cert_url': f'{GCP_APIS_URL}/oauth2/v1/certs',
                'client_x509_cert_url': f'{GCP_APIS_URL}/robot/v1/metadata/x509/{gcp_service_account}'
                }

        self.credentials = SACredentials.from_service_account_info(info)
        self.storage = Client(credentials=self.credentials)

        self.bucket_name = bucket_name

    # pickling actions
    def save_item(self, key, item):
        self.store_blob(self.bucket_name, key, item)

    def load_item(self, key):
        return self.get_blob(self.bucket_name, key)

    def find_item(self, key):
        return self.find_blob(self.bucket_name, key)

    def get_item(self, key):
        ok = self.find_item(key)
        stored = self.load_item(key) if ok else None

        return stored, ok

    def clear_items(self, key):
        self.clear_blobs(self.bucket_name, key)

    # google cloud interactions
    def get_bucket(self, bucket_name):
        ''' get bucket container '''
        bucket = self.storage.get_bucket(bucket_name)

        return bucket

    def list_blobs(self, bucket_name):
        ''' get list of blobs in a bucket '''
        bucket = self.get_bucket(bucket_name)
        blobs = bucket.list_blobs()

        return blobs

    def find_blob(self, bucket_name, blob_name):
        ''' see if blob exists '''
        print(f'\t...looking for {bucket_name}/{blob_name}', end='')
        found = blob_name in [blob.name for blob in self.list_blobs(bucket_name)]
        print(f'...{"found" if found else "not found"}!')

        return found

    def store_blob(self, bucket_name, blob_name, contents):
        ''' store blob contents '''
        print(f'\t...storing {bucket_name}/{blob_name}', end='')
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        p = compress(pickle.dumps(contents))
        b = BytesIO(p)
        blob.upload_from_file(b)
        print(f'...complete!')

    def get_blob(self, bucket_name, blob_name):
        ''' retrive blob contents '''
        print(f'\t...retrieving {bucket_name}/{blob_name}', end='')
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        b = blob.download_as_bytes()
        contents = pickle.loads(decompress(b))
        print(f'...complete!')

        return contents

    def clear_blob(self, bucket_name, blob_name):
        ''' remove a specific blob '''
        bucket = self.get_bucket(bucket_name)
        bucket.delete_blob(blob_name)

    def clear_blobs(self, bucket_name, partial_blob_name):
        ''' remove all matching blobs '''
        bucket = self.get_bucket(bucket_name)
        blobs = self.list_blobs(bucket_name)
        remove_blobs = [blob for blob in blobs if f'/{partial_blob_name}' in blob.name]
        bucket.delete_blobs(remove_blobs)
        
class Closet:
    ''' store and retrieve items from session state or cloud '''
    def __init__(self, streamer, gclouder=None):
        self.streamer = streamer
        self.gclouder = gclouder if gclouder else GClouder()

    def get_items(self, key):
        ''' check session and cloud for a stored item '''
        stored, ok = self.streamer.get_session_state(self.get_session_key(key))
        if (not ok) and (self.gclouder is not None):
            stored, ok = self.gclouder.get_item(self.get_cloud_key(key))
        
        return stored, ok

    def store_items(self, key, to_store, session=True, cloud=True):
        ''' store an item in session and cloud '''
        if (cloud) and (self.gclouder is not None):
            self.gclouder.save_item(self.get_cloud_key(key), to_store)
        if session:
            self.streamer.store_session_state(self.get_session_key(key), to_store)

    def get_key(self, category, **kwargs):
        key = {'category': category}
        key.update(kwargs)

        return key

    def get_session_key(self, key):
        return tuple(key.values())

    def get_cloud_key(self, key):
        return '/'.join('/'.join([k, key[k]]) for k in key if key[k])[len('category/'):]