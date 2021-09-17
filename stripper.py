import codecs
from os import listdir
from difflib import SequenceMatcher
import re
from dateutil.parser import parse
#from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup

class Simulator:
    def __init__(self, main_url=None, credentials=None,
                 chrome_directory=None, chrome_profile=None,
                 silent=True):
        self.main_url = main_url
        self.credentials = credentials
        
        self.chrome_directory = chrome_directory
        self.chrome_profile = chrome_profile
        self.silent = silent#False
        self.options = self.get_options()
        self.driver = None

        self.logged_in_url = f'{self.main_url}/user/'
        self.logged_in = False

    def get_options(self):
        options = Options()
        options.add_argument(f'user-data-dir={self.chrome_profile}')
        options.add_experimental_option('excludeSwitches', ['enable-logging']) # shut up debugger
        if self.silent:
            options.add_argument("--headless")
            options.add_argument("--window-size=%s" % '1920,1080')
        
        return options

    def turn_on(self):
        self.driver = webdriver.Chrome(f'{self.chrome_directory}/chromedriver.exe',
                                       options=self.options)
        
    def turn_off(self):
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    def login(self, attempts=5):
        attempt = 0

        if self.driver is None:
            self.turn_on()

        while (attempt < attempts) and (not self.logged_in):

            self.driver.get(self.main_url)

            # check if already on user page
            pre_url = self.driver.current_url
            if self.logged_in_url in pre_url:
                # already logged in
                self.logged_in = True
                post_url = self.driver.current_url
                   
            else:
                # still on main page
                self.driver.find_element_by_link_text('Log In!').click()

                self.authenticate()

                post_url = self.driver.current_url

                # log in is successful if the page looks different but has same starting HTTPS
                self.logged_in = self.logged_in_url in post_url

            if self.logged_in:
                print('Log in successful!')
                #print(f'pre: {pre_url}, post: {post_url}')
            else:
                #self.turn_off()
                print(f'Log in failed! (attempt {attempt+1}/{attempts})')
                print(f'pre: {pre_url}, post: {post_url}')

            attempt += 1

    def authenticate(self):
        pre_url = self.driver.current_url
        for credential in self.credentials:
            self.driver.find_element_by_id(credential).clear()
            self.driver.find_element_by_id(credential).send_keys(self.credentials[credential])

        self.driver.find_element_by_id('login-button').click()

        self.driver.implicitly_wait(2)
        try:
            element = WebDriverWait(self.driver, 10).until(expected_conditions.url_contains(self.logged_in_url)) #url_changes(pre_url)) #
            print('\t\t...authenticated')
        except TimeoutException:
            print('\t\t...failed to authenticate')

    def get_html_text(self, url):
        if not self.logged_in:
            self.login()

        print(f'Accessing {url}...')
        self.driver.get(url)

        html_text = self.driver.page_source

        return html_text

class Scraper:
    def __init__(self, simulator, stripper):
        self.simulator = simulator
        self.stripper = stripper
        
    def get_html_text(self, path):
        if path[0] == '/':
            path = f'{self.simulator.main_url}{path}'
        if path[:len('http')] == 'http':
            html_text = self.get_from_web(path)
        else:
            html_text = self.get_from_local(path)
            
        return html_text

    def get_from_web(self, url):
        html_text = self.simulator.get_html_text(url)
        return html_text

    def get_from_local(self, path):
        response = codecs.open(path, 'r', 'utf-8')
        html_text = response.read()
        return html_text

    def get_urls(self, directory, name=None, not_urls=[]):
        # get list of HTML files in directory
        urls = [fn for fn in listdir(directory) if '.html' in fn]

        # sort if specified
        if name is not None:
            ratios = [SequenceMatcher(None, name, url).ratio() for url in urls]
            urls = [u for _, u in sorted(zip(ratios, urls))]
        if len(not_urls):
            urls = [u for u in urls if u not in not_urls]

        urls = [f'{directory}/{u}' for u in urls]

        return urls

    def check_url(self, url, league_title, round_title=None):
        # check if correct url
        page_type = 'round' if (round_title is not None) else 'league'
        html_text = self.get_html_text(url)
        results = self.stripper.get_results(html_text, page_type)
        match = (results['league'][0] == league_title)
        if round_title is not None:
            match = match & (results['round'][0] == round_title)
        return match

    def get_right_url(self, directory, league_title=None, round_title=None, not_urls=[]):
        # find the right url and search by order
        name = round_title if (round_title is not None) else league_title if (league_title is not None) else None
        urls = self.stripper.get_urls(directory, name=name, not_urls=not_urls)
        match = False
        i = -1
        while (not match) & (i < len(urls) - 1):
            i += 1
            match = self.stripper.check_url(f'{directory}/{urls[i]}', league_title, round_title=round_title)

        if match:
            round_url = f'{directory}/{urls[i]}'
        else:
            round_url = None

        return round_url

class Stripper:
    home_types = {'league': {'tag': 'div',
                             'attrs': {'class': 'league-title'},
                             }, # completed date
                  'league_urls': {'tag': 'a',
                                  'attrs': {'class': 'league-tile'},
                                  'href': True,#_like': 'l/',
                                  },
                  'user_name': {'tag': 'span',
                                'attrs': {'id': 'user-name'},
                                },
                  'league_creators': {'tag': 'a',
                                      'attrs': {'class': 'creator-name'},
                                       #'href': True,
                                       },
                  'league_dates': {'tag': 'span',
                                   'attrs': {'data-timestamp': True},
                                   },
                  }

    league_types = {'league': {'tag': 'span',
                               'attrs': {'class': 'league-title'},
                               },
                    'round': {'tag': 'a',
                              'attrs': {'class': 'round-title'},
                              'href': 'l/',
                              'multiple': {'title': {},
                                           'url': {'href': True},
                                           },
                              },
                    'viewable_rounds': {'tag': 'a',
                                        'attrs': {'class': 'action-link', 
                                                  'title': 'Round Results',
                                                  },
                                        'href': 'l/',
                                        },
                    'round_dates': {'tag': 'span',
                                    'attrs': {'data-timestamp': True},
                                    },
                    'round_creators': {'tag': 'span',
                                       'attrs': {'class': 'status-text',
                                                 'data-timestamp': False,
                                                 },
                                       'string': True,
                                       'remove': {'type': 'in',
                                                  'rems': ['Chosen by', 'Submitted by']},
                                       },
                    'player': {'tag': 'a',
                               'href': 'user/',
                               'multiple': {'name': {'title': True},
                                            'img': {'src': True},
                                            'url': {'href': True},
                                            }
                               },
                    }

    round_types = {'league': {'tag': 'span',
                              'attrs': {'class': 'league-title'},
                              'sublink': True,
                              },
                   'round': {'tag': 'span',
                             'attrs': {'class': 'round-title'},
                             },
                   'artist': {'tag': 'span',
                              'attrs': {'class': 'vcenter artist'},
                              'remove': {'type': 'start',
                                         'rems': ['By ']},
                              },
                   'track': {'tag': 'a',
                             'attrs': {'class': 'vcenter name'},
                             'multiple': {'title': {},
                                          'url': {'href': True},
                                          },
                             },
                   'submitter': {'tag': 'span',
                                 'attrs': {'class': 'vcenter submitter'},
                                 'sublink': True,
                                 },
                   'player': {'tag': 'div',
                              'attrs': {'class': 'col-xs-9 col-sm-8 col-md-9 voter text-left vcenter'},
                              },
                   'votes': {'tag': 'span',
                             'attrs': {'class': 'vote-count'},
                             'value': True,
                             },
                   'people': {'tag': 'span',
                              'attrs': {'class': 'voter-count'},
                              'remove': {'type': 'start',
                                         'rems': [f' {p} Voted On This' for p in ['People', 'Person']]},
                              'value': True,
                              },
                   }
                                 
    def __init__(self, main_url=''):
        self.result_types = {'home': Stripper.home_types,
                             'league': Stripper.league_types,
                             'round': Stripper.round_types,
                             }

        self.main_url = main_url

    def strain_soup(self, soup, rt):
        name = rt['tag']
        attributes = {}

        # find elements of soup that match attributes
        if rt.get('attrs') is not None:
            attributes['attrs'] =  rt['attrs']

        # look for href conditions
        if type(rt.get('href')) is bool:
            # any link exists
            attributes['href'] = rt['href']
        elif type(rt.get('href')) is str:
            # specific link exists
            href_like = rt['href']
            if (href_like[:len('http')] != 'http'):
                href_compile = f'{self.main_url}/{href_like}'
            else:
                href_compile = f'/{href_like}'
            attributes['href'] = re.compile(f'^{href_compile}') # must start with string
        if rt.get('string'):
            attributes['string'] = rt['string']

        noodles = soup.find_all(name=name, **attributes)

        return noodles

    def strip_noodle(self, noodle, **attrs):
        # get relevant contents

        if attrs.get('multiple') is not None:
            # dig deeper
            multiple_attrs = attrs['multiple']
            stripped = {m_attrs: self.strip_noodle(noodle, **multiple_attrs[m_attrs]) for m_attrs in multiple_attrs}
        else:
            # value to strip
            if attrs.get('sublink'): #, False):
                # link URL in another tag
                stripped = noodle.a.string
            elif attrs.get('href'): #, False) or (attrs.get('href_like') is not None):
                # link URL
                stripped = noodle['href']
            elif attrs.get('src'): #, False):
                # image location
                stripped = noodle.img['src']
            elif attrs.get('title'): #, False):
                # name
                stripped = noodle['title']
            elif attrs.get('attrs') and attrs['attrs'].get('data-timestamp'): #, False):
                # date
                stripped = parse(noodle['data-timestamp']).date()
                #stripped = datetime.strptime(noodle['data-timestamp'], '%Y-%m-%dT%H:%M:%SZ').date()
            else:
                # text
                stripped = noodle.string
    
            if attrs.get('remove'): # is not None:
                # extract what comes after
                if attrs['remove']['type'] == 'start':
                    for rem in attrs['remove']['rems']:
                        stripped = stripped.replace(rem, '', 1)
                # extract what comes between items
                elif attrs['remove']['type'] == 'in':
                    starts = attrs['remove']['rems']
                    pattern_starts = [start.replace('(','\(').replace(')','\)') for start in starts]
                    pattern_end = '[,.;() \-]'
                    pattern = '|'.join(f'({pattern_start}.+?{pattern_end})' for pattern_start in pattern_starts)
                    searched = re.search(pattern, stripped, flags=re.IGNORECASE)

                    if searched:
                        pattern = '|'.join(f'({pattern_start})' for pattern_start in pattern_starts)
                        matched = re.match(pattern, searched[0], flags=re.IGNORECASE)
                        stripped = searched[0][len(matched[0]):-1].strip()

                    else:
                        stripped = None
                    
            if attrs.get('value', False):
                # convert to number
                stripped = int(stripped)
       
        return stripped
  
    def get_results(self, html_text, page_type):
        # get HTML tags from URL

        soup = BeautifulSoup(html_text, 'html.parser') #response.text

        result_types = self.result_types[page_type]

        results = {rt: [self.strip_noodle(noodle, **result_types[rt]) \
            for noodle in self.strain_soup(soup, result_types[rt])] \
                                               for rt in result_types}

        return results

    def extract_results(self, html_text, page_type) -> tuple:
        # extract results from HTML tags
        print(f'\t...extracting results...')
        results = self.get_results(html_text, page_type)
        
        if page_type == 'home':
            returnable = self.extract_home(results)
        elif page_type == 'league':
            returnable = self.extract_league(results)
        elif page_type == 'round':
            returnable = self.extract_round(results)
        else:
            returnable = None

        return returnable

    def extract_home(self, results):
        league_titles = results['league']
        league_urls = results['league_urls']

        user_name = results['user_name'][0]
        league_creators = [user_name if creator == 'YOU' else creator for creator in results['league_creators']]

        league_dates = results['league_dates']

        return league_titles, league_urls, league_creators, league_dates

    def extract_league(self, results):
        league_title = results['league'][0]

        round_titles_all = [round['title'] for round in results['round']]
        viewable_urls = results['viewable_rounds']
        # only count URLs for rounds with results
        round_urls_all = [round['url'] if (round['url'] in viewable_urls) else None for round in results['round']]
        round_dates_all = results['round_dates']
        round_creators_all = results['round_creators'] #[creator for creator in results['round_creators'] if creator is not None] 

        # remove rounds titles and URLS that are duplicate (i.e. open)
        round_title_set = list(dict.fromkeys(round_titles_all))
        round_titles = [round_titles_all[round_titles_all.index(s)] for s in round_title_set]
        round_urls = [round_urls_all[round_titles_all.index(s)] for s in round_title_set]
        round_dates = [round_dates_all[round_titles_all.index(s)] for s in round_title_set]
        round_creators = [round_creators_all[round_titles_all.index(s)] for s in round_title_set]
        
        player_names = [player['name'] for player in results['player']]
        player_urls = [player['url'] for player in results['player']]
        player_imgs = [player['img'] for player in results['player']]

        return league_title, round_titles, \
            player_names, player_urls, player_imgs, \
            round_urls, round_dates, round_creators

    def extract_round(self, results):
        league_title = results['league'][0]
        round_title = results['round'][0]

        artists = results['artist'][0::2]
        titles = results['track']['title'][0::2]
        submitters = results['submitter'][0::2]
        track_urls = results['track']['url'][0::2]

        player_names = []
        vote_counts = []
        song_ids = []
        num = 0

        song_id = 0
        while num < len(results['player']):
            song_id += 1
            name = results['player'][num]
            first_repeat = results['player'].index(name, num+1)
            count = first_repeat - num

            #votes.loc[len(votes):len(votes)+count, 'song_id'] = [song_id]*count
            #for result_type in ['player', 'votes']:
            #    votes.loc[len(votes):len(votes)+count, result_type] = results[result_type][num:first_repeat]

            ## what about overall votes for an open round, where players are unknown?

            player_names.extend(results['player'][num:first_repeat])
            vote_counts.extend(results['votes'][num:first_repeat])
            song_ids.extend([song_id]*count)

            num += count*2

        return league_title, round_title, artists, titles, submitters, song_ids, player_names, vote_counts, track_urls



# people = results['people']
# [sum(p for p in people[0:i]) for i in range(len(people) + 1)] <- shortcut to avoid for loop
