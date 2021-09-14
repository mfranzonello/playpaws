import codecs
from os import listdir
from difflib import SequenceMatcher

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions
from bs4 import BeautifulSoup

class Simulator:
    def __init__(self, main_url=None, credentials=None,
                 chrome_directory=None, chrome_profile=None,
                 silent=True):
        self.main_url = main_url
        self.credentials = credentials
        
        self.chrome_directory = chrome_directory
        self.chrome_profile = chrome_profile
        self.silent = silent
        self.options = self.get_options()
        self.driver = None

        self.logged_in_url = f'{self.main_url}/user/'
        self.logged_in = False

    def get_options(self):
        options = Options()
        options.add_argument(f'user-data-dir={self.chrome_profile}')
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
                print(f'pre: {pre_url}, post: {post_url}')
            else:
                #self.turn_off()
                print(f'Log in failed! (attempt {attempt+1}/{attempts})')
                print(f'pre: {pre_url}, post: {post_url}')
                input()

            attempt += 1

    def authenticate(self):
        pre_url = self.driver.current_url
        for credential in self.credentials:
            self.driver.find_element_by_id(credential).clear()
            self.driver.find_element_by_id(credential).send_keys(self.credentials[credential])

        self.driver.find_element_by_id('login-button').click()

        print('waiting')
        print(self.main_url)
        print(self.logged_in_url)
        print(pre_url)
        self.driver.implicitly_wait(2)
        element = WebDriverWait(self.driver, 10).until(expected_conditions.url_contains(self.logged_in_url)) #url_changes(pre_url)) #
        print('worked')
        print(f'element: {element}')

    def get_html_text(self, url):
        if not self.logged_in:
            self.login()

        self.driver.get(url)

        html_text = self.driver.page_source

        return html_text

class Scraper:
    def __init__(self, simulator, stripper):
        self.simulator = simulator
        self.stripper = stripper

    def get_html_text(self, path):
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
                             'attr': {'class': 'league-title'},
                             }, # complete
                  'link': {'tag': 'a',
                           'attr': {'class': 'league-tile'},
                           'href': True,
                           },
                  }

    league_types = {'league': {'tag': 'span',
                               'attr': {'class': 'league-title'},
                               },
                    'round': {'tag': 'a',
                              'attr': {'class': 'round-title'},
                              },
                    'player': {'tag': 'span',
                               'attr': {'class': 'member-name'},
                               },
                    'link': {'tag': 'a',
                             'attr': {'title': 'round-title'},
                             'href': True,
                             }
                    }

    round_types = {'league': {'tag': 'span',
                              'attr': {'class': 'league-title'},
                              'sublink': True,
                              },
                   'round': {'tag': 'span',
                             'attr': {'class': 'round-title'},
                             },
                   'artist': {'tag': 'span',
                              'attr': {'class': 'vcenter artist'},
                              'remove': ['By '],
                              },
                   'title': {'tag': 'a',
                             'attr': {'class': 'vcenter name'},
                             },
                   'submitter': {'tag': 'span',
                                 'attr': {'class': 'vcenter submitter'},
                                 'sublink': True,
                                 },
                   'player': {'tag': 'div',
                              'attr': {'class': 'col-xs-9 col-sm-8 col-md-9 voter text-left vcenter'},
                              },
                   'votes': {'tag': 'span',
                             'attr': {'class': 'vote-count'},
                             'value': True,
                             },
                   'people': {'tag': 'span',
                              'attr': {'class': 'voter-count'},
                              'remove': [f' {p} Voted On This' for p in ['People', 'Person']],
                              'value': True,
                              },
                   'link': {'a': '',
                            'attr': {'class': 'vcenter name'},
                            'href': True,
                            }
                   }

    def __init__(self):
        self.result_types = {'round': Stripper.round_types,
                             'home': Stripper.home_types,
                             'league': Stripper.league_types,
                             }

    def strip_tag(self, tag, remove=None, value=False, sublink=False, href=False):
        if sublink:
            stripped = tag.a.string
        elif href:
            stripped = tag['href']
        else:
            stripped = tag.string
    
        if remove is not None:
            for rem in remove:
                stripped = stripped.replace(rem, '', 1)
        if value:
            stripped = int(stripped)
        return stripped
   
    def get_results(self, html_text, page_type):
        # get HTML tags from URL

        soup = BeautifulSoup(html_text, 'html.parser') #response.text

        result_types = self.result_types[page_type]
        results = {rt: [self.strip_tag(tag,
                                       result_types[rt].get('remove'),
                                       result_types[rt].get('value', False),
                                       result_types[rt].get('sublink', False)) for \
                                           tag in soup.find_all(name=result_types[rt]['tag'],
                                                                attrs=result_types[rt]['attr'])] for \
                                                                    rt in result_types} #['class'] instead of ['attr']

        return results

    def extract_results(self, html_text, page_type) -> tuple:
        # extract results from HTML tags
        print(f'Extracting results...')
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
        league_urls = results['link']
        return league_titles, league_urls

    def extract_league(self, results):
        league_title = results['league'][0]
        round_titles = results['round']
        round_urls = results['link']
        player_names = results['player']

        return league_title, round_titles, player_names, round_urls

    def extract_round(self, results):
        league_title = results['league'][0]
        round_title = results['round'][0]

        artists = results['artist'][0::2]
        titles = results['title'][0::2]
        submitters = results['submitter'][0::2]
        track_urls = results['link'][0::2]

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

            player_names.extend(results['player'][num:first_repeat])
            vote_counts.extend(results['votes'][num:first_repeat])
            song_ids.extend([song_id]*count)

            num += count*2

        return league_title, round_title, artists, titles, submitters, song_ids, player_names, vote_counts, track_urls



# people = results['people']
# [sum(p for p in people[0:i]) for i in range(len(people) + 1)] <- shortcut to avoid for loop

#website = 'http://musicleague.app'
#leagues = {'Play Paws Repeat': '5e7fac9b372046718c9778b42c2c6a47',
#          'Brad\'s Mixtape': 'b347861673e04b2cbc869c62309fc870'}

#for league in leagues:
#    league_num = leagues[league]
#    link = f'{website}/l/{league_num}/'
#    
#    payload = {'user': 'mfranzonello@gmail.com', #'1235496003'
#               'password': 'Italia1985!'}
#    with requests.Session() as s:
#        response = s.post('http://accounts.spotify.com/en/login', data={'username': 'mfranzonello@gmail.com', 'password': 'Italia1985!'})
#        print(response)
#        print(response.text)

#        response = s.get('http://accounts.spotify.com/en/status')
#        print(response)
#        print(response.text)
        
#        p = s.post(link, auth=HTTPBasicAuth(payload['user'], payload['password']))
#        print(p.text)
#        page = s.get(link)
#        response = s.get(link, auth=HTTPBasicAuth(payload['user'], payload['password']))
#        print(response.text)

#    #soup = BeautifulSoup(page.text, 'html.parser')
#    #print(soup.select)

#    input()
#    #print(soup.prettify())

##music_league_data = read_excel(path, sheetname)
##
##print(music_league_data)

#'https://accounts.spotify.com/authorize?client_id=96b3fbe9ebad42559cd306ea482b3085&response_type=code&redirect_uri=https%3A%2F%2Fmusicleague.app%2Flogin%2F&scope=user-read-email'
