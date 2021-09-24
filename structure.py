import requests
from os import getlogin

class Setter:
    # structure
    structure = {'main_url': 'https://musicleague.app'
                 }

    server = {'db_name': 'bitio',
              'location': 'https://bit.io',
              }

    def __init__(self):
        self.connected = self.check_network(self.structure['main_url']) and self.check_network(self.server['location'])

    def check_network(self, url, timeout=5):
        print(f'Checking network for {url}')
        try:
            requests.head(url, timeout=timeout)
            print('\t...passed!')
            connected = True
        except requests.ConnectionError:
            print('\t...failed!')
            connected = False

        return connected

    def get_settings(self):
        return self.server, self.structure