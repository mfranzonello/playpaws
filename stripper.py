import codecs
from os import listdir
import re
from dateutil.parser import parse

import requests
import browser_cookie3 as browsercookie
from bs4 import BeautifulSoup

from streaming import streamer

class Scraper:
    def __init__(self, stripper):
        self.stripper = stripper
        self.main_url = stripper.main_url

        self.cj = browsercookie.chrome(domain_name=self.main_url.replace('https://', ''))
        
    def get_html_text(self, url):
        streamer.print(f'\t...requesting {url}')

        if url[0] == '/':
            url = f'{self.main_url}{url}'

        response = requests.get(url, cookies=self.cj, timeout=10)
        if response.ok:
            html_text = response.text
        else:
            html_text = None
        
        return html_text

    def check_url(self, url, league_title, round_title=None):
        # check if correct url
        page_type = 'round' if (round_title is not None) else 'league'
        html_text = self.get_html_text(url)
        results = self.stripper.get_results(html_text, page_type)
        match = (results['league'][0] == league_title)
        if round_title is not None:
            match = match & (results['round'][0] == round_title)
        return match

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
                    'round_dates': {'tag': 'span',
                                    'attrs': {'data-timestamp': True},
                                    },
                    'round_creators': {'tag': 'span',
                                       'attrs': {'class': 'status-text'},
                                       'self_only': True,
                                       'remove': {'type': 'in',
                                                  'rems': [f'{b} by ' for b in ['Chosen', 'Submitted']]},
                                       },
                    'playlists': {'tag': 'a',
                                  'attrs': {'class': 'action-link',
                                            'title': 'Listen to Playlist'},
                                  'href': True,
                                  },
                    'player': {'tag': 'a',
                               'href': 'user/',
                               'multiple': {'name': {'title': True},
                                            'img': {'src': True},
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
        streamer.print(f'\t\t...extracting results')
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

        viewable_urls = results['viewable_rounds']
        # only count URLs for rounds with results
        round_urls_all = viewable_urls #[round['url'] if (round['url'] in viewable_urls) else None for round in results['round']]
        t2u = len(round_titles_all) - len(round_urls_all)
        round_urls_all = [None]*((t2u+1)//2) + round_urls_all + [None]*(t2u//2)
        
        round_dates_all = results['round_dates']
        round_creators_all = [creator if len(creator) else None for creator in results['round_creators'] if creator is not None] #[creator for creator in results['round_creators'] if creator is not None]
        
        round_playlists_all = results['playlists']

        ##input(f'{round_titles_all}, {viewable_urls}, {round_urls_all}, {round_dates_all}, {round_creators_all}, {round_playlists_all}')


        round_dates_all = round_dates_all[(len(round_dates_all)-len(round_titles_all)+1)//2:\
            (len(round_dates_all)-len(round_titles_all)+1)//2+len(round_titles_all)]
        round_creators_all = round_creators_all[(len(round_creators_all)-len(round_titles_all)+1)//2:\
            (len(round_creators_all)-len(round_titles_all)+1)//2+len(round_titles_all)]

        # remove rounds titles and URLS that are duplicate (i.e. open)
        round_title_set = list(dict.fromkeys(round_titles_all))
        round_titles = [round_titles_all[round_titles_all.index(s)] for s in round_title_set]
        round_urls = [round_urls_all[round_titles_all.index(s)] for s in round_title_set]
        round_dates = [round_dates_all[round_titles_all.index(s)] for s in round_title_set]
        round_creators = [round_creators_all[round_titles_all.index(s)] for s in round_title_set]

        t2p = len(round_titles_all) - len(round_playlists_all)
        round_playlists_all = [None]*((t2p+1)//2) + round_playlists_all + [None]*(t2p//2)
        round_playlists = [round_playlists_all[round_titles_all.index(s)] for s in round_title_set]
        
        player_names = [player['name'] for player in results['player']]
        player_urls = [player['url'] for player in results['player']]
        player_imgs = [player['img'] for player in results['player']]

        return league_title, round_titles, \
            player_names, player_urls, player_imgs, \
            round_urls, round_dates, round_creators, round_playlists

    def extract_round(self, results):
        league_title = results['league'][0]
        round_title = results['round'][0]

        artists = results['artist'][0::2]
        tracks = results['track'][0::2]
        titles = [track['title'] for track in tracks]
        track_urls = [track['url'] for track in tracks]
        submitters = results['submitter'][0::2]

        voter_totals = results['people']
        point_totals = results['points']
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

        return league_title, round_title, artists, titles, submitters, song_ids, player_names, vote_counts, point_totals, track_urls #voter_totals
