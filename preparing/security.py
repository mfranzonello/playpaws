from base64 import b64encode
from nacl import encoding, public

import browser_cookie3 as browsercookie
import requests

from common.secret import get_secret, set_secret

class Lockbox:
    git_url = 'https://api.github.com'

    def __init__(self):
        self.repository_id = get_secret('GITHUB_REPOSITORY_ID')
        self.environment_name = get_secret('GITHUB_ENVIRONMENT_NAME')
        self.token = get_secret('GITHUB_TOKEN')

    def get_headers(self):
        ''' headers for GitHub '''
        headers = {'Accept': 'application/vnd.github+json',
                   'Authorization': f'Bearer {self.token}'}

        return headers

    def call_api(self, url, method, jason=None):
        ''' communicate with GitHub '''
        r_method = {'get': requests.get,
                    'post': requests.post,
                    'put': requests.put}[method]

        response = r_method(url, headers=self.get_headers(), json=jason)

        if response.ok:
            content = response.content
            jason = response.json() if len(content) and response.headers.get('Content-Type').startswith('application/json') else None

        else:
            content = None
            jason = None

        return content, jason

    def get_public_key(self):
        ''' get public key for encryption '''
        url = (f'{self.git_url}/repositories/{self.repository_id}/'
               f'environments/{self.environment_name}/secrets/public-key'
               )

        _, key_jason = self.call_api(url, 'get')

        if key_jason:
            public_key_id = key_jason['key_id']
            public_key = key_jason['key']
        else:
            public_key_id = None
            public_key = None

        return public_key_id, public_key

    def encrypt_secret(self, public_key:str, secret_value:str) -> str:
        ''' encrypt a unicode string using the public key. '''
        public_key = public.PublicKey(public_key.encode('utf-8'), encoding.Base64Encoder())
        sealed_box = public.SealedBox(public_key)
        encrypted = sealed_box.encrypt(secret_value.encode('utf-8'))

        return b64encode(encrypted).decode('utf-8')

    def store_secret(self, secret_name, secret_value):
        ''' store a secret using encryption '''
        url = (f'{self.git_url}/repositories/{self.repository_id}/'
               f'environments/{self.environment_name}/secrets/{secret_name}'
               )

        public_key_id, public_key = self.get_public_key()

        encrypted_value = self.encrypt_secret(public_key, secret_value)

        jason = {'encrypted_value': encrypted_value,
                 'key_id': public_key_id}

        self.call_api(url, 'put', jason=jason)

class Baker:
    def __init__(self, app_url):
        self.domain_name = app_url.replace('https://', '') 

    def mix_cookies(self):
        ''' get cookie values '''
        cj = browsercookie.chrome(domain_name=self.domain_name)
        cookie_name = list(cj._cookies[f'.{self.domain_name}']['/'].keys())[0]
        cookie_value = cj._cookies[f'.{self.domain_name}']['/'][cookie_name].value
        
        return cookie_name, cookie_value

    def bake_cookies(self, cookie_name, cookie_value, lockbox):
        ''' add new values to environments '''
        ##set_secret('ML_COOKIE_NAME', cookie_name)
        set_secret('ML_COOKIE_VALUE', cookie_value)

        if lockbox:
            ##lockbox.store_secret('ML_COOKIE_NAME', cookie_name)
            lockbox.store_secret('ML_COOKIE_VALUE', cookie_value)

    def reset_cookies(self, lockbox=None):
        ''' retrieve and store cookie values '''
        cookie_name, cookie_value = self.mix_cookies()
        self.bake_cookies(cookie_name, cookie_value, lockbox)