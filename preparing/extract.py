import re
from dateutil.parser import parse
from io import BytesIO
from zipfile import ZipFile

import requests
import browser_cookie3 as browsercookie
from bs4 import BeautifulSoup

from common.secret import get_secret, set_secret
from common.words import Texter
from display.streaming import Streamable

class Scraper(Streamable):
    headers_get = {'X-Requested-With': 'XMLHttpRequest'}
    headers_post = {'authority': 'musicleague.app'}

    def __init__(self, stripper):
        super().__init__()
        self.stripper = stripper
        self.main_url = stripper.main_url
        
        #self.cj = browsercookie.chrome(domain_name=self.main_url.replace('https://', ''))
        self.cj = {get_secret('ML_COOKIE_NAME'): get_secret('ML_COOKIE_VALUE')}
        
    def reset_cookie(self):
        domain_name = self.main_url.replace('https://', '')
        cj = browsercookie.chrome(domain_name=domain_name)
        cookie_name = list(cj._cookies[f'.{domain_name}']['/'].keys())[0]
        cookie_value = cj._cookies[f'.{domain_name}']['/'][cookie_name].value
        set_secret('ML_COOKIE_NAME', cookie_name)
        set_secret('ML_COOKIE_VALUE', cookie_value)

    def get_html_text(self, url):
        return self.get_content(url, 'text')

    def get_zip_file(self, url):
        return self.get_content(url, 'zip')
    
    def get_content(self, url, response_type):
        self.streamer.print(f'\t...requesting {response_type} for {url}')

        if url[0] == '/':
            url = f'{self.main_url}{url}'
        if response_type == 'zip':
            url = f'{url}/data'.replace('//', '/')

        if response_type == 'text':
            method = requests.get
            headers = self.headers_get
        elif response_type == 'zip':
            method = requests.post
            headers = self.headers_post

        response = method(url, cookies=self.cj, headers=headers, timeout=10)
        if response.ok:
            if response_type == 'text':
                item = response.text
            elif response_type == 'zip':
                item = ZipFile(BytesIO(response.content))
        else:
            item = None

        return item

class Stripper(Streamable):
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
                    'round_open': {'tag': 'span',
                                   'attrs': {'class': 'round-title'},
                                   },
                    'round': {'tag': 'a',
                              'attrs': {'class': 'round-title'},
                              'href': True,
                              'multiple': {'title': {},
                                           'url': {'href': True},
                                           },
                              },
                    'viewable_rounds': {'tag': 'a',
                                        'attrs': {'class': 'action-link', 
                                                  'title': 'Round Results',
                                                  },
                                        'href': True,
                                        },
                    'voteable_rounds': {'tag': 'a',
                                        'attrs': {'class': 'action-link', 
                                                  'title': 'Submit Votes',
                                                  },
                                        'href': True,
                                        },
                    'round_dates': {'tag': 'span',
                                    'attrs': {'data-timestamp': True},
                                    },
                    'description': {'tag': 'span',
                                    'attrs': {'class': 'status-text'},
                                    'self_only': True,
                                    },
                    'playlists': {'tag': 'a',
                                  'attrs': {'class': 'action-link',
                                            'title': 'Listen to Playlist'},
                                  'href': True,
                                  },
                    'player': {'tag': 'a',
                               'href': 'user/',
                               'multiple': {'name': {'title': True},
                                            'url': {'href': True,
                                                    'remove': {'type': 'in',
                                                               'rems': ['/user/']},
                                                    },
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
                   'user': {'tag': 'a',
                            'href': 'user/',
                            'multiple': {'name': {'title': {}},
                                         'url': {'href': True,
                                                 'remove': {'type': 'in',
                                                            'rems': ['/user/']},
                                                 },
                                         },
                            },
                   'votes': {'tag': 'span',
                             'attrs': {'class': 'vote-count'},
                             'value': True,
                             },
                   'points': {'tag': 'span',
                              'attrs': {'class': 'point-count'},
                              'value': True,
                              },
                   'people': {'tag': 'span',
                              'attrs': {'class': 'voter-count'},
                              'remove': {'type': 'start',
                                         'rems': [f' {p} Voted On This' for p in ['People', 'Person']]},
                              'value': True,
                              },
                   }
                                 
    def __init__(self, main_url):
        super().__init__()
        self.result_types = {'home': Stripper.home_types,
                             'league': Stripper.league_types,
                             'round': Stripper.round_types,
                             }

        self.main_url = main_url

        self.texter = Texter()

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
            slash = '/' if href_like[0] != '/' else ''

            if (href_like[:len('http')] != 'http'):
                href_compile = '|'.join(f'(^{m}{href_like})' for m in [f'{self.main_url}{slash}', slash])
            else:
                href_compile = f'{slash}{href_like}'
            
            attributes['href'] = re.compile(href_compile) # must start with string
        if rt.get('string'):
            attributes['string'] = rt['string']

        noodles = soup.find_all(name=name, **attributes)

        return noodles

    def strip_noodle(self, noodle, **attrs):
        # get relevant contents
        if attrs.get('multiple'):
            # dig deeper
            multiple_attrs = attrs['multiple']
            stripped = {m_attrs: self.strip_noodle(noodle, **multiple_attrs[m_attrs]) for m_attrs in multiple_attrs}

        elif attrs.get('self_only') and noodle.span:
            # cannot have children
            stripped = None

        else:
            # value to strip
            if attrs.get('sublink'):
                # link URL in another tag
                stripped = noodle.a.string
            elif attrs.get('href'):
                # link URL
                stripped = noodle['href']
            elif attrs.get('src'):
                # image location
                stripped = noodle.img['src']
            elif attrs.get('title'):
                # name
                stripped = noodle['title']
            elif attrs.get('attrs') and attrs['attrs'].get('data-timestamp'):
                # date
                stripped = parse(noodle['data-timestamp']).date()
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
                    ##_, stripped = self.texter.remove_parenthetical()
                 
                    starts = attrs['remove']['rems']
                    pattern_starts = [start.replace('(','\(').replace(')','\)') for start in starts]
                    pattern_ends = ['[,.;()/\- ]', '$']
                    pattern = '|'.join(f'({s}(.*?){e})' for s in pattern_starts for e in pattern_ends)
                    searched = re.search(pattern, stripped, flags=re.IGNORECASE)

                    if searched:
                        # return first match
                        stripped = next(s for s in searched.groups()[1::2] if s).strip()
                    else:
                        stripped = ''
                    
            if attrs.get('value', False):
                # convert to number
                stripped = int(stripped)
       
        return stripped
  
    def get_results(self, html_text, page_type):
        # get HTML tags from URL
        soup = BeautifulSoup(html_text, 'html.parser')

        result_types = self.result_types[page_type]

        results = {rt: [self.strip_noodle(noodle, **result_types[rt]) \
            for noodle in self.strain_soup(soup, result_types[rt])] \
                                               for rt in result_types}

        return results

    def extract_results(self, html_text, page_type) -> tuple:
        # extract results from HTML tags
        self.streamer.print(f'\t\t...extracting results')
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
        
        rounds_open = results['round_open']
        rounds_front, rounds_back = rounds_open[:(len(rounds_open)+1)//2], rounds_open[(len(rounds_open)+1)//2:]
        round_titles_all = rounds_front + [round['title'] for round in results['round']] + rounds_back

        voteable_urls = results['voteable_rounds']
        viewable_urls = results['viewable_rounds'] #results['voteable_rounds'] +
        # only count URLs for rounds with results
        round_urls_all = voteable_urls[:len(voteable_urls)//2] + viewable_urls + voteable_urls[len(voteable_urls)//2:] #[round['url'] if (round['url'] in viewable_urls) else None for round in results['round']]
        t2u = len(round_titles_all) - len(round_urls_all)
        round_urls_all = [None]*((t2u+1)//2) + round_urls_all + [None]*(t2u//2)
        
        round_dates_all = results['round_dates']
        round_descriptions_all = results['description'][::2]
        ##round_creators_all = [creator if len(creator) else None for creator in results['round_creators'] if creator is not None] #[creator for creator in results['round_creators'] if creator is not None]
        
        round_playlists_all = results['playlists']

        round_dates_all = round_dates_all[(len(round_dates_all)-len(round_titles_all)+1)//2:\
            (len(round_dates_all)-len(round_titles_all)+1)//2+len(round_titles_all)]
        round_descriptions_all = round_descriptions_all[(len(round_descriptions_all)-len(round_titles_all)+1)//2:\
            (len(round_descriptions_all)-len(round_titles_all)+1)//2+len(round_titles_all)]
        ##round_creators_all = round_creators_all[(len(round_creators_all)-len(round_titles_all)+1)//2:\
        ##    (len(round_creators_all)-len(round_titles_all)+1)//2+len(round_titles_all)]

        # remove rounds titles and URLS that are duplicate (i.e. open)
        round_title_set = list(dict.fromkeys(round_titles_all))
        round_titles = [round_titles_all[round_titles_all.index(s)] for s in round_title_set]
        round_urls = [round_urls_all[round_titles_all.index(s)] for s in round_title_set]
        round_dates = [round_dates_all[round_titles_all.index(s)] for s in round_title_set]
        round_descriptions = [round_descriptions_all[round_titles_all.index(s)] for s in round_title_set]
        ##round_creators = [round_creators_all[round_titles_all.index(s)] for s in round_title_set]

        t2p = len(round_titles_all) - len(round_playlists_all)
        round_playlists_all = [None]*((t2p+1)//2) + round_playlists_all + [None]*(t2p//2)
        round_playlists = [round_playlists_all[round_titles_all.index(s)] for s in round_title_set]
        
        player_names = [player['name'] for player in results['player']]
        player_urls = [player['url'] for player in results['player']]

        return league_title, round_titles, \
            player_names, player_urls, \
            round_urls, round_dates, round_descriptions, round_playlists #round_creators

    def extract_round(self, results):
        league_title = results['league'][0]
        round_title = results['round'][0]

        tracks = results['track'][0::2]
        track_urls = [track['url'] for track in tracks]

        submitters = results['submitter'][0::2]
        
        if len(results['user']):
            users_all = {user['url']: user['name'] for user in results['user']}
            users = {'username': users_all.keys(), 'player_names': users_all.values()}
        else:
            users = None

        voter_totals = results['people']
        point_totals = results['points']
        player_names = []
        vote_counts = []
        song_ids = []

        if len(track_urls):
            # round has votes visible
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

        return league_title, round_title, submitters, song_ids, player_names, vote_counts, point_totals, track_urls, users #voter_totals

