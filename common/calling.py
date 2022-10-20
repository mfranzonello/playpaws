''' Calling APIs, receiving JSONs and updating JSONs '''

import requests
import json
from datetime import date, datetime, timedelta
import time

from pandas import isnull
from pandas.api.types import is_numeric_dtype

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
    def __init__(self, database):
        j_names = {'mail': 'checked',
                   'reopen': 'reopens'}
        self.jasons = {j: f'./jsons/{j_names[j]}.json' for j in j_names}

        self.database = database

    def get_time(self, item):
        dt = self.database.get_update(item)

        return dt

    def set_time(self, item):
        self.database.store_update(item, datetime.now())
      
    def get_reopens(self):
        with open(self.jasons['reopen'], 'r+') as f:
            reopens  = json.load(f)

        return reopens

    def set_reopens(self):
        with open(self.jasons['reopen'], 'r+') as f:
            f.seek(0)
            json.dump({}, f)
            f.truncate()

class Quoter:
    ''' changes input text to SQL compatible '''
    def __init__(self):
        pass

    def quotable(self, item):
        ''' add SQL appropriate quotes to string variables '''
        is_quote = not (self.numberable(item) or self.nullable(item))
        return is_quote

    def numberable(self, item):
        ''' do not add quotes or cast information to numbers '''
        is_number = (not self.nullable(item)) and is_numeric_dtype(type(item))
        return is_number

    def timable(self, item):
        ''' add cast information to datetime values '''
        is_time = isinstance(item, datetime)
        return is_time

    def datable(self, item):
        ''' add cast information to date values '''
        is_date = isinstance(item, date)
        return is_date

    def nullable(self, item):
        ''' change to None for None, nan, etc '''
        is_null = (not self.jsonable(item)) and (isnull(item) or (item == 'nan'))
        return is_null

    def jsonable(self, item):
        ''' add cast information to lists and dicts as JSON '''
        is_json = isinstance(item, (list, dict, set))
        return is_json

    def replace_for_sql(self, text):
        ''' fix special characters '''
        char = "'"
        pct = '%'
        for_sql = char + text.replace(char, char*2).replace(pct, pct*2).replace(pct*4, pct*2) + char
        return for_sql

    def put_quotes(self, item) -> str:
        ''' put quotes around strings to account for special characters '''
        if self.quotable(item):
            if self.timable(item):
                quoted = self.replace_for_sql(str(item)) + '::timestamp'
            if self.datable(item):
                quoted = self.replace_for_sql(str(item)) + '::date'
            elif self.jsonable(item):
                quoted = self.replace_for_sql(json.dumps(item)) + '::jsonb'
            else:
                quoted = self.replace_for_sql(str(item))
        elif self.nullable(item):
            quoted = 'NULL'
        else:
            quoted = str(item)

        return quoted

    def load_json(self, string):
        ''' convert string to list or dictionary '''
        return json.loads(string)

    def dump_json(self, data):
        ''' convert list or dictionary to string '''
        return json.dumps(data)