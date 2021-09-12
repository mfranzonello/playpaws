import codecs
from os import listdir
from difflib import SequenceMatcher

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

class Selena:
    def __init__(self, local=False, chrome_directory=None, main_url=None, credentials=None):
        self.local = local
        self.main_url = main_url
        self.credentials = credentials[main_url]
        
        self.chrome_directory = chrome_directory
        self.options = Selena.get_options()
        self.driver = None
        self.logged_in = False

    def get_options():
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--window-size=%s" % '1920,1080')
        
        return options

    def turn_on(self):
        if not self.local:
            self.driver = webdriver.Chrome(f'{self.chrome_directory}/chromedriver.exe',
                                           options=self.options)
        
    def turn_off(self):
        if (not self.local) and (self.driver is not None):
            self.driver.close()
            self.driver = None

    def login(self, attempts=5):
        if not self.local:
            attempt = 0
            while (attempt < attempts) and (not self.logged_in):
                if self.driver is None:
                    self.turn_on()

                self.driver.get(self.main_url)

                pre_url = self.driver.current_url

                self.driver.find_element_by_link_text('Log In!').click()

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

    def get_html_text(self, url):
        if not self.local:
            if not self.logged_in:
                self.login()

            self.driver.get(url)

            html_text = self.driver.page_source

            return html_text

class HTMLScraper:
    def get_html_text(path, selena=None):
        if path[:len('http')] == 'http':
            html_text = HTMLScraper.get_from_web(path, selena)
            
        else:
            html_text = HTMLScraper.get_from_local(path)
            
        return html_text

    def get_from_web(url, selena):
        html_text = selena.get_html_text(url)
        return html_text

    def get_from_local(path):
        response = codecs.open(path, 'r', 'utf-8')
        html_text = response.read()
        return html_text

    def get_urls(directory, name=None, not_urls=[]):
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

    def check_url(url, league_title, round_title=None):
        # check if correct url
        page_type = 'round' if (round_title is not None) else 'league'
        html_text = HTMLScraper.get_html_text(url)
        results = HTMLStripper.get_results(html_text, page_type)
        match = (results['league'][0] == league_title)
        if round_title is not None:
            match = match & (results['round'][0] == round_title)
        return match

    def get_right_url(directory, league_title, round_title=None, not_urls=[]):
        # find the right url and search by order
        name = round_title if (round_title is not None) else league_title
        urls = HTMLStripper.get_urls(directory, name=name, not_urls=not_urls)
        match = False
        i = -1
        while (not match) & (i < len(urls) - 1):
            i += 1
            match = HTMLStripper.check_url(f'{directory}/{urls[i]}', league_title, round_title=round_title)

        if match:
            round_url = f'{directory}/{urls[i]}'
        else:
            round_url = None

        return round_url

class HTMLStripper:
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
                   }

    result_types = {'round': round_types,
                    'home': home_types,
                    'league': league_types,
                    }

    def strip_tag(tag, remove=None, value=False, sublink=False, href=False):
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
   
    def get_results(html_text, page_type):
        # get HTML tags from URL

        soup = BeautifulSoup(html_text, 'html.parser') #response.text

        result_types = HTMLStripper.result_types[page_type]
        results = {rt: [HTMLStripper.strip_tag(tag,
                                               result_types[rt].get('remove'),
                                               result_types[rt].get('value', False),
                                               result_types[rt].get('sublink', False)) for \
                                                tag in soup.find_all(name=result_types[rt]['tag'],
                                                                     attrs=result_types[rt]['attr'])] for \
                                                                        rt in result_types} #['class'] instead of ['attr']

        return results

    def extract_results(html_text, page_type):
        # extract results from HTML tags
        print(f'Extracting results...')
        results = HTMLStripper.get_results(html_text, page_type)
        
        if page_type == 'home':
            returnable = HTMLStripper.extract_home(results)
        elif page_type == 'league':
            returnable = HTMLStripper.extract_league(results)
        elif page_type == 'round':
            returnable = HTMLStripper.extract_round(results)
        else:
            returnable = None

        return returnable

    def extract_home(results):
        league_titles = results['league']
        league_urls = results['link']
        return league_titles, league_urls

    def extract_league(results):
        league_title = results['league'][0]
        round_titles = results['round']
        round_urls = results['link']
        player_names = results['player']

        return league_title, round_titles, round_urls, player_names

    def extract_round(results):
        league_title = results['league'][0]
        round_title = results['round'][0]

        artists = results['artist'][0::2]
        titles = results['title'][0::2]
        submitters = results['submitter'][0::2]

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

        return league_title, round_title, artists, titles, submitters, song_ids, player_names, vote_counts



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
