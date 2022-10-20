''' Updating cookies locally and in GitHub '''

from base64 import b64encode
from nacl import encoding, public
from datetime import datetime, timedelta
import re
import os

import browser_cookie3 as browsercookie
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
#from webdriver_manager.chrome import ChromeDriverManager

from common.calling import Caller
from common.secret import get_secret, set_secret, list_secrets
from common.locations import GITHUB_URL, APP_URL

class Lockbox(Caller):
    def __init__(self):
        super().__init__()

        self.repository_id = get_secret('GITHUB_REPOSITORY_ID') ## should this be a location?
        self.environment_name = get_secret('GITHUB_ENVIRONMENT_NAME') ## should this be a location?
        self.token = get_secret('GITHUB_TOKEN')

    def get_headers(self):
        ''' headers for GitHub '''
        headers = {'Accept': 'application/vnd.github+json',
                   'Authorization': f'Bearer {self.token}'}

        return headers

    def call_api(self, url, method, jason=None):
        ''' communicate with GitHub '''
        content, jason = self.invoke_api(url, method, headers=self.get_headers(), json=jason)

        return content, jason

    def get_public_key(self):
        ''' get public key for encryption '''
        url = (f'{GITHUB_URL}/repositories/{self.repository_id}/'
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
        url = (f'{GITHUB_URL}/repositories/{self.repository_id}/'
               f'environments/{self.environment_name}/secrets/{secret_name}'
               )

        public_key_id, public_key = self.get_public_key()

        encrypted_value = self.encrypt_secret(public_key, secret_value)

        jason = {'encrypted_value': encrypted_value,
                 'key_id': public_key_id}

        self.call_api(url, 'put', jason=jason)

    def get_secret(self, secret_name):
        ''' get secret details (but not encrypted value) '''
        url = (f'{GITHUB_URL}/repositories/{self.repository_id}/'
               f'environments/{self.environment_name}/secrets/{secret_name}')

        _, secret = self.call_api(url, 'get')

        return secret

    def update_secrets(self):
        ''' update all new .env secrets in GitHub '''
        local_secrets = list_secrets()
        for secret_name in local_secrets:
            if not self.get_secret(secret_name):
                self.store_secret(secret_name, local_secrets[secret_name])

class Baker:
    def __init__(self):
        self.domain_name = APP_URL.replace('https://', '') 

    def mix_cookies(self):
        ''' get cookie values '''
        print('Getting cookies...')
        cj = browsercookie.chrome(domain_name=self.domain_name)
        cookie_name = list(cj._cookies[f'.{self.domain_name}']['/'].keys())[0]
        cookie_value = cj._cookies[f'.{self.domain_name}']['/'][cookie_name].value
        expiration_date = datetime.utcfromtimestamp(cj._cookies[f'.{self.domain_name}']['/'][cookie_name].expires)
        
        return cookie_name, cookie_value, expiration_date

    def bake_cookies(self, cookie_name, cookie_value, lockbox):
        ''' add new values to environments '''
        ##set_secret('ML_COOKIE_NAME', cookie_name)
        print('\t...storing secrets locally')
        set_secret('ML_COOKIE_VALUE', cookie_value)

        if lockbox:
            print('\t...storing secrets remotely')
            ##lockbox.store_secret('ML_COOKIE_NAME', cookie_name)
            lockbox.store_secret('ML_COOKIE_VALUE', cookie_value)

    def is_stale(self, date, days_left=0):
        return date <= datetime.utcnow() - timedelta(days=days_left)

    def check_freshness(self):
        _, _, expiration_date = self.mix_cookies()
        stale = self.is_stale(expiration_date)
        if stale:
            ## launch browser and get new cookie
            print('\t...cookies have expired!')
        else:
            print('\t...cookies are still fresh!')

        return stale

    def reset_cookies(self, lockbox=None):
        ''' retrieve and store cookie values '''
        cookie_name, cookie_value, _ = self.mix_cookies()

        self.bake_cookies(cookie_name, cookie_value, lockbox)

class Selena:
    def __init__(self, credentials=None):
        self.main_url = APP_URL
        self.credentials = credentials

        self.options = Selena.get_options()
        self.driver = None
        self.logged_in = False

    def get_options():
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--window-size=%s" % '1920,1080')

        return options

    def turn_on(self):
        print('Running Chrome in background...')
        self.driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()),
                                       options=self.options)

    def turn_off(self):
        print('Turning off background Chrome')
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    def go_to_site(self, url=None):
        if not url:
            url = APP_URL

        print(f'\t...going to {url}')
        self.driver.get(url)

    def login(self, attempts=5):
        attempt = 0
        while (attempt < attempts) and (not self.logged_in):
            if self.driver is None:
                self.turn_on()

            self.go_to_site()\

            pre_url = self.driver.current_url

            self.driver.find_element_by_link_text('Log In!').click()

            if self.credentials:
                for credential in self.credentials:
                    self.driver.find_element_by_id(credential).send_keys(self.credentials[credential])

            self.driver.find_element_by_id('login-button').click()

            post_url = self.driver.current_url

            # log in is successful if the page looks different but has same starting HTTPS
            self.logged_in = (pre_url != post_url) & (pre_url in post_url)

            if self.logged_in:
                print('Log in successful!')
                print(f'pre: {pre_url}, post: {post_url}')
            else:
                self.turn_off()
                print(f'Log in failed! (attempt {attempt+1}/{attempts}')

            attempt += 1


    def login_and_get_cookie(self):
        # go to MusicLeague
        driver.get(APP_URL)
        driver.find_element(By.CLASS_NAME, 'loginButton').click()

        # log in to Spotify
        driver.find_element(By.ID, 'login-username').send_keys(get_secret('SPOTIFY_USERNAME'))
        driver.find_element(By.ID, 'login-password').send_keys(get_secret('SPOTIFY_PASSWORD'))
        driver.find_element(BY.ID, 'login-button').click()

        # authorize Spotify
        driver.find_element(By.X_PATH, '//*[@id="root"]/div/div[2]/div/div/div[3]/button/div[1]').click()

        # get MusicLeague cookie
        found_cookie = None

        cookies = driver.get_cookies()
        for cookie in cookies:
            if cookie['domain'] in APP_URL.replace('https://', '.'):
                found_cookie = {'name': cookie['name'],
                                'value': cookie['value']}
                break

        return found_cookie
