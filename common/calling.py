''' Calling APIs, receiving JSONs and updating JSONs '''

import requests
import json
from datetime import datetime
import time

class Caller:
    methods = {'get': requests.get,
               'post': requests.post,
               'put': requests.put}
    def __init__(self, url):
        self.url = url

    def invoke_api(self, url, method='get', **kwargs):
        content = None
        jason = None
        print(f'Calling {url}...')
        try:
            response = self.methods[method](url=url, timeout=10, **kwargs)
            if response.ok:
                content, jason = self.extract_json(response)

        except Exception as e:
            print(f'...call failed due to {e}.')

        return content, jason

    def extract_json(self, response):
        if response.ok:
            content = response.content
            jason = response.json() if len(r_content) and response.headers.get('Content-Type').startswith('application/json') else None
        else:
            content = None
            jason = None

        return content, jason

    def get_token(self, url, expiry='expires_at', refresh_token=None, **kwargs):
        _, token_info = invoke_api(url, method='post', **kwargs)
 
        token_info[expiry] = int(time.time()) + token_info['expires_in']
        token_info['refresh_token'] = refresh_token if refresh_token else token_info['refresh_token']

        return token_info

class Recorder:
    def __init__(self, filename='checked'):
        self.jason = f'./preparing/{filename}.json'

    def set_time(self, item):
        with open(self.jason, 'r+') as f:
            j  = json.load(f)
            j[item] = datetime.now().isoformat()
            f.seek(0)
            json.dump(j, f)
            f.truncate()
        
    def get_time(self, item):
        with open(self.jason, 'r+') as f:
            j = json.load(f)
            dt = datetime.fromisoformat(j[item])
            
        return dt