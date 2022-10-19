''' Calling APIs, receiving JSONs and updating JSONs '''

import requests
import json
from datetime import datetime, timedelta
import time

class Caller:
    methods = {'get': requests.get,
               'post': requests.post,
               'put': requests.put}

    def __init__(self):
        pass

    def invoke_api(self, url, method='get', **kwargs):
        content = None
        jason = None

        print_url = self.get_print_url(url)
        
        print(f'Calling {print_url}...')
        try:
            response = self.methods[method](url=url, timeout=10, **kwargs)
            if response.ok:
                content, jason = self.extract_json(response)

        except Exception as e:
            print(f'...call failed due to {e}.')

        return content, jason

    def get_print_url(self, url, max_length=60, middle='...', end_length=10):
        print_url = url.replace('https://', '').replace('http://', '')
        if len(print_url) > max_length:
            if '&' in print_url:
                print_url = print_url[:print_url.index('&')]
            if len(print_url) > max_length:
                print_url = print_url[:max_length-len(middle)-end_length] + middle + print_url[-end_length:]

        return print_url

    def extract_json(self, response):
        if response.ok:
            content = response.content
            jason = response.json() if len(content) and response.headers.get('Content-Type').startswith('application/json') else None
        else:
            content = None
            jason = None

        return content, jason

    def get_token(self, url, refresh_token=None, **kwargs):
        _, token_info = self.invoke_api(url, method='post', **kwargs)

        now = time.time()
        today = datetime.now()

        token_info['expires_at'] = int(now + token_info['expires_in'])
        token_info['expiry'] = (today + timedelta(seconds=token_info['expires_in'])).strftime('%Y-%m-%dT%H:%M:%S')
        if refresh_token:
            token_info['refresh_token'] = refresh_token

        return token_info

class Recorder:
    def __init__(self):
        j_names = {'mail': 'checked',
                   'reopen': 'reopens'}
        self.jasons = {j: f'./jsons/{j_names[j]}.json' for j in j_names}

    def get_time(self, item):
        with open(self.jasons['mail'], 'r+') as f:
            j = json.load(f)
            dt = datetime.fromisoformat(j[item])
            
        return dt

    def set_time(self, item):
        with open(self.jasons['mail'], 'r+') as f:
            j  = json.load(f)
            j[item] = datetime.now().isoformat()
            f.seek(0)
            json.dump(j, f)
            f.truncate()
        
    def get_reopens(self):
        with open(self.jasons['reopen'], 'r+') as f:
            reopens  = json.load(f)

        return reopens

    def set_reopens(self):
        with open(self.jasons['reopen'], 'r+') as f:
            f.seek(0)
            json.dump({}, f)
            f.truncate()