import requests
from os import getlogin

class Setter:
    # structure
    directory = f'C:/Users/{getlogin()}/OneDrive/Projects/Play Paws'
    html_folder = 'html'
    structure = {'local': {'home': f'{directory}/{html_folder}/home',
                           'league': f'{directory}/{html_folder}/leagues',
                           'round': f'{directory}/{html_folder}/rounds',
                           },
                 'web': {'chrome_driver': f'C:/Users/{getlogin()}/OneDrive/Projects/Play Paws/Play Paws/Play Paws/',
                         'chrome_profile': f'C:/Users/{getlogin()}/AppData/Local/Google/Chrome/User Data/Default/',
                         'main_url': 'https://musicleague.app',
                         },
                 }

    servers = {'local': {'db_name': 'sqlite',
                         'location': f'{directory}',
                         },
               'web': {'db_name': 'bitio',
                       'location': f'https://bit.io',
                       },
               }

    def __init__(self, silent=True, update_db=False):
        self.settings = {'silent': silent,
                         'update_db': update_db,
                         'local': not Setter.check_network(self, Setter.structure['web']['main_url'])
                         }

        self.server = Setter.servers['web'] if self.check_network(Setter.servers['web']['location']) else Setter.servers['local']
        self.structure = Setter.structure

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
        return self.settings, self.server, self.structure