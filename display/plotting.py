''' Data visuals for Streamlit '''

from math import sin, cos, atan2, pi, nan, ceil
from os.path import dirname, realpath
from datetime import datetime
from random import choice as rand_choice
from collections import Counter

from pandas import DataFrame, isnull, to_datetime, Timestamp
from pandas.core.indexes.base import Index
import matplotlib.pyplot as plt
from matplotlib import rcParams, font_manager
from matplotlib.dates import date2num
from wordcloud import WordCloud
from numpy import unique, int64, float64, array, ndarray

from common.words import Texter
from common.locations import SPOTIFY_PLAY_URL
from display.librarian import Library
from display.artist import Canvas, Paintbrush
from display.storage import Boxer
from display.streaming import Streamable, Stab

class Plotter(Streamable):   
    marker_sizing = 50

    name_offset = 0.1
    font_size = 0.2
    
    likes_color = 'pink'
    liked_color = 'orange'
    like_arrow_width = 0.02
    like_arrow_length = 0.4
    like_arrow_split = 0.05

    figure_title = 'MusicLeague'

    subplot_aspects = {'golden': ((1 + 5**0.5) / 2, 1),
                       'top_songs': (3, 1),
                       }

    ranking_size = 0.75

    def __init__(self, database, streamer):
        super().__init__()
        self.texter = Texter()
        self.library = Library()
        self.paintbrush = Paintbrush()
        self.boxer = Boxer()

        self.streamer = streamer
        self.database = database

        self.library.add_emoji(*self.database.get_emojis())

        self.blank_league = '<select>'
        self.blank_player = ''
        self.god_player = self.database.get_god_id()
        
        # define fonts to use
        self.fonts = self.library.get_and_set_fonts(font_manager, rcParams,
                                                    dirname(dirname(realpath(__file__))),
                                                    plot_color=self.paintbrush.get_color('dark_grey', normalize=True))       
        self.highlight_color = self.paintbrush.get_color('blue', lighten=0.2)
        self.fail_color = self.paintbrush.get_color('grey')
        self.separate_colors = self.paintbrush.get_colors('silver', 'gold')
        
        lighten_pcts = [p/100 for p in range(-30, 30+10, 10)]
        self.pass_colors = [self.paintbrush.lighten_color(self.highlight_color, p) for p in lighten_pcts]
        self.fail_colors = [self.paintbrush.lighten_color(self.fail_color, p) for p in lighten_pcts]                                       

        plt.rcParams['hatch.linewidth'] = 3.0

        self.league_ids = None
        self.view_player = None
        self.view_league_ids = None
        self.plot_counts = len((self.plot_members,
                                self.plot_boards,
                                self.plot_rankings,
                                self.plot_features,
                                self.plot_tags,
                                self.plot_pie,
                                self.plot_top_songs,
                                self.plot_playlists,
                                self.plot_hoarding,
                                ))

        self.errors = []

    def print_error(self, segment, e):
        self.errors.append(segment)
        self.streamer.print(f'Error with {segment}: {e}')
    
    def add_canvas(self):
        canvas = Canvas(self.database, self.streamer)
        return canvas

    def add_data(self):
        self.streamer.print('Getting analyses')
        league_ids = self.database.get_league_ids()

        self.league_ids = [league_id for league_id in league_ids if self.database.check_data(league_id)]
        if len(self.league_ids):
            self.canvas = self.add_canvas()

    def prepare_dfs(self, key, function, *args, **kwargs):
        df, ok = self.streamer.get_session_state(key)
        if not ok:
            df = function(*args, **kwargs)
            self.streamer.store_session_state(key, df)

        return df

    def get_name(self, name_id, table=None, clean=False, full=False, display=False, feel=False, placeholder=None):
        if isinstance(name_id, (list, ndarray, Index)):
            name = [self.get_name(n_id, table, clean, full, display) for n_id in name_id]

        else:
            if (not name_id) or (isinstance(name_id, str) and ((len(name_id) == 0) or (name_id == placeholder))):
                name = name_id

            else:
                name = {'player': self.database.get_player_name,
                        'round': self.database.get_round_name,
                        'league': self.database.get_league_name,
                        'competition': self.database.get_competition_name,
                        }[table](name_id)

                if clean:
                    name = self.texter.clean_text(name)

                if full:
                    name = self.texter.get_display_name_full(name)

                if display:
                    name = self.texter.get_display_name(name)

                if feel:
                    name = self.library.feel_title(self.texter.clean_text(name))

        return name

    def get_player_name(self, player_id, full=False, display=False):
        return self.get_name(player_id, table='player', full=full, display=display)

    def get_round_title(self, round_id, clean=False):
        return self.get_name(round_id, table='round', clean=clean)

    def get_league_title(self, league_id, clean=False, feel=False):
        return self.get_name(league_id, table='league', clean=clean, feel=feel, placeholder=self.blank_league)    

    def get_competition_title(self, competition_id):
        return self.get_name(competition_id, table='competition')

    ##def get_stabs(self):
    ##    titles = {'awards': 'Awards',
    ##              }

    def plot_results(self):
        player_ids = self.database.get_player_ids()
        self.view_player = self.streamer.player_box.selectbox('Who are you?',
                                                              [self.god_player] + player_ids + [self.blank_player],
                                                              index=len(player_ids) + 1,
                                                              format_func=lambda x: self.get_player_name(x, full=True))

        if self.view_player != self.blank_player:           
            if self.view_player == self.god_player:
                viewable_league_ids = self.league_ids
            else:
                self.view_league_ids = self.database.get_player_leagues(self.view_player)
                viewable_league_ids = [l for l in self.view_league_ids if l in self.league_ids]

            if len(viewable_league_ids) == 1:
                league_id = viewable_league_ids[0]
            elif len(viewable_league_ids) > 1:
                league_id = self.streamer.selectbox.selectbox('Pick a league to view',
                                                                 [self.blank_league] + viewable_league_ids,
                                                                 format_func=lambda x: self.get_league_title(x, feel=True))
            else:
                league_id = self.blank_league

            if league_id == self.blank_league:
                try:
                    self.plot_viewer()
                except Exception as e:
                    self.print_error('plot_viewer', e)


                if self.view_player != self.god_player:
                    try:
                        self.plot_caption()
                    except Exception as e:
                        self.print_error('plot_caption', e)

            else:
                # set up stabs
                player_tab = Stab('player', {'awards': 'Awards', 'stats': 'Stats',
                                             'pulse': 'Pulse', 'wins': 'Wins'},
                                  header='Player Stats')

                boards_title = 'Finishers'
                scores_title = 'Scores'
                hoarding_title = 'Sharing'
                members_title = 'Pulse'
                audio_title = 'Audio Features'
                wordcloud_title = 'Genre Cloud'
                pieplot_title = 'Music Categories'
                league_tab = Stab('league', [boards_title, scores_title, hoarding_title, members_title,
                                                audio_title, wordcloud_title, pieplot_title], header='League Stats')

                songs_tab = Stab('songs', range(1, 1 + self.database.get_n_rounds(league_id)), header='Top Songs')


                # plot the viewer image
                if self.view_player == self.god_player:
                    badge = None
                    badge2 = None

                else:
                    boards_league_df = self.prepare_dfs(('boards_league', league_id),
                                                        self.database.get_boards_league, league_id)
                    boards_competition_df = self.prepare_dfs(('boards_competition', league_id),
                                                        self.database.get_boards_competition, league_id)

                    badge = boards_league_df.query('player_id == @self.view_player')['place'].iloc[0]
                    badge2 = boards_competition_df.query('player_id == @self.view_player')['place'].iloc[0]
                    n_players = len(boards_competition_df)

                playlists_df = self.prepare_dfs(('playlists_df', league_id),
                                                self.database.get_playlists, league_id)
                self.plot_viewer(badge=badge, badge2=badge2, playlists_df=playlists_df)

                # plot the league stats
                if self.view_player == self.god_player:
                    awards_df = self.prepare_dfs(('awards_df', league_id, self.view_player),
                                                 self.database.get_awards, league_id, self.view_player)
                    try:
                        self.plot_caption(league_id=league_id, god_mode=True, awards_df=awards_df, title='League Stats')
                    except Exception as e:
                        self.print_error('plot_songs', e)

                # plot the viewer stats
                else:
                    self.streamer.tab(player_tab, caption=True)
                    relations_df = self.prepare_dfs(('relations_df', league_id),
                                                 self.database.get_relationships, league_id)
                    wins_df = self.prepare_dfs(('player_wins_df', league_id, self.view_player),
                                               self.database.get_round_wins, league_id, self.view_player)
                    awards_df = self.prepare_dfs(('awards_df', league_id, self.view_player),
                                                 self.database.get_awards, league_id, self.view_player)
                    stats_df = self.prepare_dfs(('stats_df', league_id),
                                                self.database.get_stats, league_id)

                    competition_id = self.database.get_current_competition(league_id)
                    competition_title = self.get_competition_title(competition_id)
                    competition_wins = self.prepare_dfs(('competitions_wins', league_id, self.view_player),
                                                       self.database.get_competition_wins, league_id,
                                                       self.view_player)
                    try:
                        self.plot_caption(league_id=league_id, relations_df=relations_df,
                                          wins_df=wins_df, awards_df=awards_df, stats_df=stats_df,
                                          competition_title=competition_title,
                                          badge2=badge2, n_players=n_players,
                                          competition_wins=competition_wins, tab=player_tab)
                    except Exception as e:
                        self.print_error('plot_caption', e)

                self.streamer.print(f'Preparing plot for {self.get_league_title(league_id)}')
                self.streamer.status(0, base=True)

                # plot results title
                league_creator = self.prepare_dfs(('league_creator', league_id),
                                                  self.database.get_league_creator, league_id)
                self.plot_title(league_id, league_creator)
           

                # plot results tab
                self.streamer.tab(league_tab)

                # plot round finishers
                boards_df = self.prepare_dfs(('boards_df', league_id),
                                             self.database.get_boards, league_id)
                creators_winners_df = self.prepare_dfs(('creators_winners_df', league_id),
                                                       self.database.get_creators_and_winners, league_id)
                competitions_df = self.prepare_dfs(('competitions_df', league_id),
                                                    self.database.get_competitions, league_id)
                self.plot_boards(league_id, boards_df, creators_winners_df, competitions_df,
                                 title=boards_title, tab=league_tab)

                # plot player scores
                rankings_df = self.prepare_dfs(('rankings_df', league_id),
                                               self.database.get_rankings, league_id)
                awards_round_df = self.prepare_dfs(('awards_df', league_id),
                                             self.database.get_round_awards, league_id)
                try:
                    self.plot_rankings(league_id, rankings_df, awards_round_df,
                                       title=scores_title, tab=league_tab)
                except Exception as e:
                    self.print_error('plot_rankings', e)
                    
                # plot vote hoarding
                awards_league_df = self.prepare_dfs(('awards_df', league_id),
                                self.database.get_league_awards, league_id)
                self.plot_hoarding(league_id, awards_round_df, awards_league_df,
                                   title=hoarding_title, tab=league_tab)

                # plot league pulse
                members_df = self.prepare_dfs(('members_df', league_id),
                                              self.database.get_members, league_id)
                try:
                    self.plot_members(league_id, members_df, title=members_title, tab=league_tab)
                except Exception as e:
                    self.print_error('plot_members', e)
                    
                # plot audio features
                features_df = self.prepare_dfs(('features_df', league_id),
                                               self.database.get_audio_features, league_id)
                try:
                    self.plot_features(league_id, features_df, title=audio_title, tab=league_tab)
                except Exception as e:
                    self.print_error('plot_features', e)

                # plot wordcloud and pie chart
                genres_df, categories_df = self.prepare_dfs(('genres_df', league_id),
                                                            self.database.get_genre_counts,
                                                            league_id, tags=True, remove_default=True)
                exclusives_df = self.prepare_dfs(('exclusives_df', league_id),
                                                 self.database.get_exclusive_genres, league_id)
                if self.view_player == self.god_player:
                    tags_df = None
                else:
                    tags_df = self.prepare_dfs(('tags_df', league_id, self.view_player),
                                                self.database.get_player_tags, league_id,
                                                player_id=self.view_player)
                masks = self.prepare_dfs(('masks', league_id),
                                         self.boxer.get_mask, league_id)

                try:
                    self.plot_tags(league_id, genres_df, exclusives_df, tags_df, masks,
                                   title=wordcloud_title, tab=league_tab)
                except Exception as e:
                    self.print_error('plot_tags', e)
                try:
                    self.plot_pie(league_id, categories_df, title=pieplot_title, tab=league_tab)
                except Exception as e:
                    self.print_error('plot_pie', e)

                # plot top songs in tabs
                self.streamer.tab(songs_tab)

                results_df = self.prepare_dfs(('results_df', league_id),
                                              self.database.get_song_results, league_id)
                descriptions_df = self.prepare_dfs(('descriptions_df', league_id),
                                                   self.database.get_round_descriptions, league_id)
                try:
                    self.plot_top_songs_summary(league_id, results_df, descriptions_df)
                    self.plot_top_songs(league_id, tab=songs_tab)
                except Exception as e:
                    self.print_error('plot_songs', e)

                # plot complete playlist
                playlists_df = self.prepare_dfs(('playlists_df', league_id),
                                                self.database.get_playlists, league_id)
                track_count, track_durations = self.prepare_dfs(('durations', league_id),
                                                self.database.get_track_count_and_durations, league_id)
                try:
                    self.plot_playlists(league_id, playlists_df, track_count, track_durations)
                except Exception as e:
                    self.print_error('plot_playlists', e)
            
                exc = ' except ' + ', '.join(self.errors) if len(self.errors) else ''
                self.streamer.print(f'Everything loaded{exc}! Close this sidebar to view.')

    def place_image(self, ax, x, y, player_id=None, color=None, size=0.5,
                    image_size=(0, 0), padding=0, text=None,
                    aspect=(1, 1), flipped=False, zorder=0,
                    border_color=None, border_padding=0.2):
        
        flip = -1 if flipped else 1

        if player_id:
            image = self.canvas.get_player_image(player_id)
        elif color:
            image = self.canvas.get_color_image(color, image_size)
        else:
            image = None
            imgs = None

        if image and text:
            image = self.canvas.add_text(image, text, self.fonts['image_sans'])

        if image and border_color:
            image = self.canvas.add_border(image, color=border_color, padding=border_padding)

        if image:
            scaling = [a / max(aspect) for a in aspect]

            x_ex = (size + padding)/2 * scaling[0]
            y_ex = (size + padding)/2 * scaling[1]

            extent = [x - x_ex, x + x_ex,
                      y - flip*y_ex, y + flip*y_ex]
            imgs = ax.imshow(image, extent=extent, zorder=zorder)

        return image, imgs

    def rotate_labels(self, n_rounds):
        if n_rounds < 5:
            rotation = 0
        elif n_rounds < 10:
            rotation = 45
        else:
            rotation = 90

        return rotation

    def split_labels(self, labels, n_rounds):
        if n_rounds >= 10:
            labels = [self.texter.split_long_text(l, limit=30) for l in labels]

        return labels

    def translate(self, x:float, y:float, theta:float, rotate:float, shift_distance:float=0):
        x_shifted = x + shift_distance*cos(theta + rotate*pi/2)
        y_shifted = y + shift_distance*sin(theta + rotate*pi/2)
        return x_shifted, y_shifted

    def plot_viewer(self, badge=None, badge2=None, playlists_df=None):
        image = self.canvas.get_player_image(self.view_player)
        palette = self.paintbrush.get_palette(image)
        image = self.canvas.add_border(image, color=palette[0], padding=0.2)

        medal_metals = ['gold', 'silver', 'bronze', 'gunmetal_grey']
        for b, pct, position, label in zip([badge, badge2], [0.4, 0.25], ['LR', 'UL'], ['OVERALL', 'COMPETITION']):
            if isinstance(b, (int, float, int64, float64)) and b >= 1:
                image = self.place_badge(image, int(b), medal_metals, pct=pct, position=position, label=label)
            
        html = None
        height = None
        if playlists_df is not None:
            theme = f'favorite - {self.view_player}'
            playlist_uri = playlists_df.query('theme == @theme')['uri'].squeeze()
            if len(playlist_uri):
                playlist_uri = playlist_uri.replace('spotify:playlist:', '')

                width = 380
                height = 80
                html = (f'<iframe src="{SPOTIFY_PLAY_URL}/embed/playlist/{playlist_uri}" '
                        f'width="{width}" height="{height}" '
                        f'frameborder="0" allowtransparency="true" allow="encrypted-media"></iframe>'
                        )

        self.streamer.viewer(image, footer=html, footer_height=height)

    def place_badge(self, image, badge, medal_metals, pct, position, label=None):
        place = max(1, min(len(medal_metals), badge))
        border_color = [self.paintbrush.get_color(c) for c in medal_metals][place - 1]

        image = self.canvas.add_badge(image, self.texter.get_ordinal(badge), self.fonts['image_sans'],
                                      pct=pct, color=self.paintbrush.lighten_color(border_color, 0.1),
                                      border_color=border_color, padding=0.3, position=position, label=label)

        return image

    def plot_caption(self, league_id=None, relations_df=None, wins_df=None, awards_df=None, stats_df=None,
                     competition_title=None, badge2=None, n_players=None, competition_wins=None,
                     god_mode=False, title=None, tab=None):
        keys = {'relations': ['likes', 'liked', 'nearest'],
                'awards': ['chatty', 'quiet', 'popular', 'discoverer',
                           'dirtiest', 'clean', 'generous', 'stingy', 
                           'fast_submit', 'slow_submit', 'fast_vote', 'slow_vote'],
                'stats': ['win_rate', 'play_rate'],
                }

        relations_s = relations_df.set_index('player_id')[player_id]
        awards_s = awards_df.set_index('player_id')['player_id']
        stats_s = stats_df.set_index('player_id')[player_id]
        wins_s = winds_df.set_index('player_id')['player_id']

        if god_mode:
            parameters = {'god': True}
            keys = award_keys

            parameters.update({k: self.get_player_name(awards_df[k]) \
                for k in keys if k in awards_df})

        else:
            parameters = {}
            print('passed 0')
            print(relations_df)
            print(stats_df)
            #if (relations_df is not None) and len(relations_df):
            #    parameters.update({k: self.get_player_name(relations_df[f'{k}_id']) for k in keys['relations'] if f'{k}_id' in relations_df})
            #print('passed 1')
            if (awards_df is not None) and len(awards_df):
                    parameters.update({k: awards_df[k] for k in keys['awards'] if k in awards_df})
            print('passed 2')
            if (stats_df is not None) and len(stats_df):
                    parameters.update({k: stats_df[k] for k in keys['stats'] if k in stats_df})
            print('passed 3')
            if (wins_df is not None) and len(wins_df):
                parameters['wins'] = self.get_round_title(wins_df)
            print('passed 4')
            if badge2:
                parameters.update({'current_competition': competition_title,
                                   'badge2': badge2,
                                   'n_players': n_players})
            parameters['leagues'] = [self.get_league_title(l, feel=True) for l in self.view_league_ids if l != league_id]
            parameters['other_leagues'] = (league_id is not None)
            parameters['competition_wins'] = self.get_competition_title(competition_wins)

        if tab:
            for tk, t in (tab.get_titles_and_keys() if tab else [[title]*2]):
                self.streamer.caption(self.library.get_column(parameters, subsection=tk), header=t, tab=tab)
        else:
            self.streamer.caption(self.library.get_column(parameters), header=title)

    def plot_title(self, league_id, creator_id):
        league_title = self.get_league_title(league_id, feel=True)
        creator = self.get_player_name(creator_id)
        viewer = self.get_player_name(self.view_player)

        parameters = {'title': league_title,
                      'creator': creator,
                      'viewer': viewer,
                      }
        self.streamer.title(league_title,
                            tooltip=self.library.get_tooltip('title', parameters=parameters))

        self.streamer.sidebar_image(self.boxer.get_cover(league_title))

    def plot_members(self, league_id, members_df, title=None, tab=None):
        plot_key = (league_id, 'members_ax')
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            ax, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])

            # plot nodes for players
            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...relationships', base=False)
            x = members_df['x']
            y = members_df['y']
            player_ids = members_df['player_id']

            # plot center
            x_center, y_center = self.get_center(members_df)
            ax.scatter(x_center, y_center, marker='1', zorder=2*len(player_ids))

            sizes = self.get_scatter_sizes(members_df)
            colors = self.get_node_colors(members_df)
            colors_scatter = self.paintbrush.get_scatter_colors(colors)
       
            for x_p, y_p, p_name, s_p, c_p, c_s, z in zip(x, y, player_ids, sizes, colors, colors_scatter, player_ids.index):
                self.place_member_nodes(ax, x_p, y_p, p_name, s_p, c_p, c_s, z)
            self.streamer.status(1/self.plot_counts * (1/3))

            # split if likes is liked
            split = members_df.set_index('player_id')
            split = split['likes_id'] == split['liked_id']

            for i in members_df.index:
                self.place_member_relationships(ax, player_ids[i], members_df, split,
                                                zorder=2*len(player_ids))
            self.streamer.status(1/self.plot_counts * (1/3))

            ax.axis('equal')
            ax.set_ylim(members_df['y'].min() - self.name_offset - self.font_size,
                        members_df['y'].max() + self.name_offset + self.font_size)
            ax.axis('off')

            parameters = {'leader': self.get_player_name(members_df[members_df['wins'] == members_df['wins'].max()]['player_id'].to_list()),
                          'closest_dfc': self.get_player_name(members_df[members_df['dfc'] == members_df['dfc'].min()]['player_id'].to_list()),
                          }
            self.streamer.store_session_state(plot_key, (ax, parameters))
            
        self.streamer.pyplot(ax.figure, header=title, #in_expander=fig.get_size_inches()[1] > 6,
                             tooltip=self.library.get_tooltip('members', parameters=parameters), tab=tab)

        plt.close()

    def place_member_nodes(self, ax, x_p, y_p, p_id, s_p, c_p, c_s, z):
        if not(isnull(s_p)):
            # plot if there is a size
            plot_size = (s_p/2)**0.5/pi/10
            image, _ = self.place_image(ax, x_p, y_p, player_id=p_id, size=plot_size, flipped=False,
                                        zorder=2*z+1)
            if image:
                _, _ = self.place_image(ax, x_p, y_p, color=c_p, size=plot_size, image_size=image.size, padding=0.05,
                                        zorder=2*z)
            else:
                ax.plot(x_p, y_p, s=s_p, c=c_s)

    def get_node_colors(self, members_df):
        max_dfc = members_df['dfc'].max()
        min_dfc = members_df['dfc'].min()

        rgb_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('green', 'blue'))
        colors = [self.paintbrush.get_rgb(rgb_df, 1-(dfc - min_dfc)/(max_dfc - min_dfc),
                               fail_color=self.paintbrush.get_color('grey')) for dfc in members_df['dfc'].to_list()]
        
        return colors

    def place_member_relationships(self, ax, me, members_df, split, zorder):
        # get location
        x_me, y_me, theta_me = self.where_am_i(members_df, me)

        # plot names
        self.place_member_names(ax, me, x_me, y_me, theta_me, zorder=zorder+2)

        # split if liked
        split_distance = split[me] * self.like_arrow_split

        # find likes
        self.place_member_likers(ax, members_df, me, x_me, y_me, split_distance, direction='likes',
                                 color=self.likes_color, zorder=zorder+1)
        self.place_member_likers(ax, members_df, me, x_me, y_me, split_distance, direction='liked',
                                 color=self.liked_color, zorder=zorder+1)

    def place_member_names(self, ax, me, x_me, y_me, theta_me, zorder=0):
        x_1, y_1 = self.translate(x_me, y_me, theta_me, 0, shift_distance=-self.name_offset) ## <- name offset should be based on node size

        h_align = 'right' if (theta_me > -pi/2) & (theta_me < pi/2) else 'left'
        v_align = 'top' if (theta_me > 0) else 'bottom'
        display_name = self.get_player_name(me, display=True)
            
        ax.text(x_1, y_1, display_name, horizontalalignment=h_align, verticalalignment=v_align,
                zorder=zorder)
        
    def place_member_likers(self, ax, members_df, me, x_me, y_me, split_distance, direction, color, zorder=0):
        x_like, y_like, theta_us = self.who_likes_whom(members_df, me, direction, self.like_arrow_length)

        if theta_us:
            side = {'likes': -1,
                    'liked': 1}[direction]

            x_1, y_1 = self.translate(x_me, y_me, theta_us, side, shift_distance=split_distance)
            x_2, y_2 = self.translate(x_like, y_like, theta_us, side, shift_distance=split_distance)

            xy = {'likes': [x_1, y_1, x_2-x_1, y_2-y_1],
                  'liked': [x_2, y_2, x_1-x_2, y_1-y_2]}[direction]

            ax.arrow(*xy,
                     width=self.like_arrow_width, facecolor=color,
                     edgecolor='none', length_includes_head=True,
                     zorder=zorder)

    def get_center(self, members_df):
        x_center = members_df['x'].mean()
        y_center = members_df['y'].mean()
        
        return x_center, y_center

    def where_am_i(self, members_df, player_id):
        x_me = members_df['x'][members_df['player_id'] == player_id].to_list()[0]
        y_me = members_df['y'][members_df['player_id'] == player_id].to_list()[0]
        x_center, y_center = self.get_center(members_df)
        theta_me = atan2(y_center - y_me, x_center - x_me)

        return x_me, y_me, theta_me

    def who_likes_whom(self, members_df, player_id, direction, line_dist):
        likes_me = members_df[f'{direction}_id'][members_df['player_id'] == player_id].to_list()[0]

        x_me, y_me, _ = self.where_am_i(members_df, player_id)
        them = members_df[['x', 'y']][members_df['player_id'] == likes_me]
        if len(them):
            x_them, y_them = them.values[0]
        
            theta_us = atan2(y_them - y_me, x_them - x_me)

            x_likes = x_me + line_dist * cos(theta_us)
            y_likes = y_me + line_dist * sin(theta_us)

        else:
            theta_us = None
            x_likes = None
            y_likes = None

        return x_likes, y_likes, theta_us

    def get_scatter_sizes(self, members_df):
        sizes = (members_df['wins'] + 1) * self.marker_sizing
        return sizes

    def plot_boards(self, league_id, board, creators_winners_df, competitions_df,
                    title=None, tab=None):
        plot_key = (league_id, 'boards_ax', self.view_player)
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            ax, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure() ###figsize=(8,8), dpi=100)
            ax = fig.add_axes([1, 1, 1, 1])

            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...rankings', base=False)

            n_rounds = len(board.columns)
            n_players = len(board.index)
            aspect = (max(1, n_rounds - 1), max(1, n_players - 1))
            scaling = [a / b * aspect[1] for a, b in zip(self.subplot_aspects['golden'], aspect)]

            xs = [x * scaling[0] for x in range(1, n_rounds + 1)]

            has_dnf = board.where(board < 0, 0).sum().sum() < 0
        
            lowest_rank = int(board.where(board > 0, 0).max().max())
            highest_dnf = int(board.where(board < 0, 0).min().min())

            ties_df = DataFrame(0, columns=unique(board.values), index=xs)


            icon_scale = min(ax.figure.get_figwidth()/n_rounds,
                             ax.figure.get_figheight()/(n_players + has_dnf)) ** 0.5

            for player_id in board.index:
                self.place_board_player(ax, xs, player_id, board, lowest_rank, ties_df, icon_scale)
            self.streamer.status(1/self.plot_counts * (1/3))

            round_ids = board.columns.tolist()
        
            x_min = min(xs)
            x_max = max(xs)

            if has_dnf:
                # plot DNF line
                ax.plot([x_min - scaling[0]/2, x_max + scaling[0]/2], [lowest_rank + 1] * 2, '--', color='0.5',
                        zorder=0)
            self.streamer.status(1/self.plot_counts * (1/3))

            ax.set_xlim(x_min - scaling[0]/2, x_max + scaling[0]/2)
            ax.set_xticks(xs)
            ax.set_xticklabels(self.split_labels(self.get_round_title(round_ids, clean=True), n_rounds),
                               rotation=self.rotate_labels(n_rounds))

            y_min = lowest_rank - highest_dnf + has_dnf
            y_max = 0
            yticks = range(y_min, y_max, -1)

            ax.set_ylim(y_min + 0.5, y_max + 0.5)
            ax.set_yticks(yticks)
            ax.set_yticklabels([int(y) if y <= lowest_rank else 'DNF' if y == lowest_rank + 2 else '' for y in yticks])

            if len(competitions_df):
                self.add_backgrounds(ax, round_ids, competitions_df, scaling=scaling)

            parameters = {'round_titles': self.get_round_title(creators_winners_df['round_id'].to_list()),
                          'choosers': self.get_player_name(creators_winners_df['creator_id'].to_list()),
                          'winners': self.get_player_name(creators_winners_df['winner_ids'].to_list()),
                          }
            self.streamer.store_session_state(plot_key, (ax, parameters))

        self.streamer.pyplot(ax.figure, header=title, #in_expander=fig.get_size_inches()[1] > 6,
                             tooltip=self.library.get_tooltip('boards', parameters=parameters), tab=tab)

        plt.close()

    def place_board_player(self, ax, xs, player_id, board, lowest_rank, ties_df, icon_scale):
        player_name = self.get_player_name(player_id)
        ys = board.where(board > 0).loc[player_id]
        ds = [lowest_rank - d + 1 for d in board.where(board < 0).loc[player_id]]

        ties = board.eq(board.loc[player_id]).sum()

        display_name = self.texter.get_display_name(player_name)

        # plot finishers
        if player_id == self.view_player:
            border_color = self.highlight_color
            color = self.paintbrush.normalize_color(border_color)
            linewidth = 2.25
        else:
            border_color = None
            color = f'C{board.index.get_loc(player_id)}'
            linewidth = None
            
        ax.plot(xs, ys, marker='.', color=color, zorder=1 if player_id==self.view_player else 0, linewidth=linewidth)
        ax.scatter(xs, ds, marker='.', color=color, zorder=0)

        size = icon_scale * self.ranking_size

        for x, y, d, t in zip(xs, ys, ds, ties):

            if y > 0:
                x_plot, y_plot = self.adjust_ties(x, y, t, ties_df.loc[x, y], size)
                ties_df.loc[x, y] += 1

                image, _ = self.place_image(ax, x_plot, y_plot, player_id=player_id, size=size, flipped=True,
                                            zorder=3 if player_id==self.view_player else 2, border_color=border_color)

                if not image:
                    ax.text(x, y, display_name)
                

            if d > lowest_rank + 1:
                # plot DNFs
                image, _ = self.place_image(ax, x, d, player_id=player_id, size=size, flipped=True,
                                            zorder=1)
                if not image:
                    ax.text(x, d, display_name)

    def adjust_ties(self, x, y, t, i, size, overlap=0.95):
        if t == 1:
            x_plot = x
            y_plot = y

        else:
            angle = 2*pi / t

            adj = pi / 4 if t == 2 else 0
            R = size/2 * overlap
            x_plot = R * cos(angle * i + adj) + x
            y_plot = R * sin(angle * i + adj) + y
            
        return x_plot, y_plot

    def add_backgrounds(self, ax, round_ids, competitions_df, scaling=[1, 1], x_offset=0):
        competition_colors = self.paintbrush.get_scatter_colors(self.separate_colors)
        n_colors = len(competition_colors)

        competitions_explode = competitions_df['round_ids'].explode()
        
        y0, y1 = ax.get_ylim()

        for r_i, round_id in enumerate(round_ids):
            if r_i in competitions_explode.to_list():
                x_ = r_i + 1 + x_offset
                x0 = x_ - 1/2
                x1 = x_ + 1/2

                c_i = competitions_explode[competitions_explode == round_id].index.to_list()[0]
                color = competition_colors[c_i % n_colors]
                facecolor = tuple([c for c in color] + [0.4])
                edgecolor = tuple([c for c in color] + [0.3])
                ax.fill_between([scaling[0]*x0, scaling[0]*x1], y0, y1,
                                facecolor=facecolor, edgecolor=edgecolor,
                                hatch='///', zorder=-1)

    def plot_rankings(self, league_id, rankings, awards_round_df, title=None, tab=None):
        plot_key = (league_id, 'rankings_ax', self.view_player)
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            ax, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...scores', base=False)
            rankings_df = rankings.reset_index().pivot(index='player_id', columns='round_id', values='score').div(100)\
                .reindex(columns=rankings.index.get_level_values(0).drop_duplicates()).sort_index(ascending=False)

            player_ids = rankings_df.index
            n_rounds = len(rankings_df.columns)
                        
            fig_w, _ = fig.get_size_inches()
            fig.set_size_inches([fig_w, fig_w / (n_rounds + 4) * len(player_ids)])

            max_score = rankings_df.max().max()
            rgb_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('red', 'yellow', 'green', 'blue'))

            scores_df = awards_round_df.set_index('player_id')
        
            max_dirty = max(0.5, scores_df['dirtiness'].max())
            rgb_dirty_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('purple', 'peach'))

            max_discovery = 1
            rgb_discovery_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('grey', 'dark_blue'))

            xs = range(n_rounds)
            x_min = -1.5
            x_max = len(xs) + 2 + 0.5
            self.streamer.status(1/self.plot_counts * (1/3))
            for player_id in player_ids:
                y = player_ids.get_loc(player_id)
                marker_size = 0.9
                image_size = self.place_player_scores(ax, player_id, xs, y, rankings_df, max_score, rgb_df, marker_size)

                # plot area behind view player
                if player_id == self.view_player:
                    color = self.paintbrush.normalize_color(self.highlight_color)
                    ax.fill_between([x_min, x_max], y-0.5, y+0.5, color=color, zorder=0)

                # plot dirtiness
                self.place_player_score(ax, len(xs), y, scores_df['dirtiness'][player_id], max_dirty, rgb_dirty_df,
                                        marker_size, image_size, percent=True)

                # plot discovery
                self.place_player_score(ax, len(xs)+1, y, scores_df['discovery'][player_id], max_discovery, rgb_discovery_df,
                                        marker_size, image_size, percent=True)
                self.place_player_score(ax, len(xs)+2, y, scores_df['popularity'][player_id], max_discovery, rgb_discovery_df,
                                        marker_size, image_size, percent=True)
            self.streamer.status(1/self.plot_counts * (1/3))
            
            ax.axis('equal')
            ax.spines['left'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.set_yticklabels([])
            ax.set_yticks([])

            ax.set_xticks([(n_rounds-1)/2] + [n_rounds + i for i in range(3)])
            ax.set_xticklabels(['scores', 'dirtiness', 'discovery', 'popularity'],
                               rotation=self.rotate_labels(n_rounds))
            ax.set_xlim([x_min, x_max])
            
            parameters = {'dirty': self.get_player_name(scores_df[scores_df['dirtiness'] == scores_df['dirtiness'].max()].index.to_list()),
                          'discovery': self.get_player_name(scores_df[scores_df['discovery'] == scores_df['discovery'].max()].index.to_list()),
                          'popular': self.get_player_name(scores_df[scores_df['popularity'] == scores_df['popularity'].max()].index.to_list()),
                          }
            self.streamer.store_session_state(plot_key, (ax, parameters))

        self.streamer.pyplot(ax.figure, header=title, #in_expander=fig.get_size_inches()[1] > 6,
                             tooltip=self.library.get_tooltip('rankings', parameters=parameters), tab=tab)

        plt.close()

    def place_player_scores(self, ax, player_id, xs, y, rankings_df, max_score, rgb_df, marker_size):
        ys = [y] * len(xs)
        
        scores = rankings_df.loc[player_id]
        colors = [self.paintbrush.get_rgb(rgb_df, score/max_score, fail_color=self.paintbrush.get_color('grey')) \
            for score in scores]
        colors_scatter = self.paintbrush.get_scatter_colors(colors)

        image, _ = self.place_image(ax, -1, y, player_id=player_id, size=marker_size)
        if image:
            image_size = image.size
            for x, c, score in zip(xs, colors, scores):
                self.place_image(ax, x, y, color=c, image_size=image_size, size=marker_size,
                                text=round(score*100) if not isnull(score) else 'DNF')
        else:
            image_size = None
            ax.scatter(xs, ys, s=20**2, c=colors_scatter) 
            for x, score in zip(xs, scores):
                ax.text(x, y, round(score*100) if not isnull(score) else 'DNF',
                        horizontalalignment='center', verticalalignment='center', color='white')

        return image_size

    def place_player_score(self, ax, x, y, score, max_score, rgb_df, marker_size, image_size=None, percent=None):
        if percent:
            text = f'{score:.0%}'
        elif isnull(score):
            text = 'DNF'
        else:
            text = score

        color = self.paintbrush.get_rgb(rgb_df, score/max_score)
        if image_size:
            image, imgs = self.place_image(ax, x, y, color=color, image_size=image_size, size=marker_size, text=text)
        else:
            image, imgs = None
        return image, imgs
            
    def plot_features(self, league_id, features_df, title=None, tab=None):
        plot_key = (league_id, 'features_ax')
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            ax, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...features', base=False)
            features_solo = {f: self.library.feel(f) for f in ['tempo']}                             
            features_like = {f: self.library.feel(f) for f in ['danceability', 'energy',
                                                               'liveness', 'valence',
                                                               'speechiness', 'acousticness',
                                                               'instrumentalness']}
            available_colors = self.paintbrush.get_colors('red', 'blue', 'purple', 'peach', 'dark_blue', 'orange', 'aqua', 'copper', 'pink')

            features_colors = self.paintbrush.get_scatter_colors(available_colors)

            n_rounds = len(features_df)
        
            # ['loudness', 'key', 'mode']
            features_all = list(features_solo.keys()) + list(features_like.keys())
            mapper = {f'avg_{f}': f'{f}' for f in features_all}

            features_df.loc[:, 'round'] = features_df['round_id'].apply(lambda x: self.get_round_title(x, clean=True))
        
            features_df = features_df.set_index('round').drop(columns='round_id').rename(columns=mapper)[features_all]
            features_df[list(features_solo.keys())] = features_df[list(features_solo.keys())].abs().div(features_df[list(features_solo.keys())].max())
        
            features_df.plot(use_index=True, y=list(features_like.keys()), color=features_colors[:len(features_like)],
                             kind='bar', legend=False, xlabel=None, rot=45, ax=ax)

            padding = 5
            font_size = 'medium'

            for c in range(len(features_like)):
                #c = ax.containers.index(container)
                ax.bar_label(ax.containers[c], color=features_colors[c % len(features_colors)], labels=[list(features_like.values())[c]]*n_rounds,
                             fontfamily=self.fonts['plot_emoji'], horizontalalignment='center', padding=padding, size=font_size)
            
                    
            self.streamer.status(1/self.plot_counts * (1/3))

            padding = 0.05
            for solo, f in zip(features_solo, range(len(features_solo))):
                color = features_colors[(len(features_like) + f) % len(features_colors)]
                features_df.plot(y=solo, color=color, #secondary_y=solo, 
                                 kind='line', legend=False, ax=ax)
            
                for i in range(n_rounds-1):
                    y = (features_df[solo][i] + features_df[solo][i+1])/2
                    padding_multiplier = -1 if y > 0.5 else 1
                    ax.text(x=i + 0.5, y=y + padding_multiplier * padding, s=features_solo[solo], # y=self.convert_axes(ax, (features_df[solo][i] + features_df[solo][i+1])/2)
                            size=font_size, color=color, fontfamily=self.fonts['plot_emoji'], horizontalalignment='center') #font=self.emoji_font
                
                        
            self.streamer.status(1/self.plot_counts * (1/3))

            for position in ['top', 'left', 'right']:
                ax.spines[position].set_visible(False)

            ax.set_xticklabels(self.split_labels([t.get_text() for t in ax.get_xticklabels()], n_rounds),
                               rotation=self.rotate_labels(n_rounds))
            ax.set_xlabel('') ## None?
            ax.set_yticklabels([])
            ax.set_yticks([])
            
            parameters = {}
            self.streamer.store_session_state(plot_key, (ax, parameters))

        self.streamer.pyplot(ax.figure, header=title, #in_expander=fig.get_size_inches()[1] > 6,
                             tooltip=self.library.get_tooltip('features', parameters=parameters), tab=tab)

        plt.close()

    def convert_axes(self, ax, z, y=True):
        if y:
            z1_0, z1_1 = ax.get_ylim()
            z2_0, z2_1 = ax.right_ax.get_ylim()
        else:
            z1_0, z1_1 = ax.get_xlim()
            z2_0, z2_1 = ax.right_ax.get_xlim()

        z_ = (z - z2_0) / (z2_1 - z2_0) * (z1_1 - z1_0) + z1_0

        return z_

    def plot_tags(self, league_id, genres_df, exclusives_df, tags_df, mask_bytes, title=None, tab=None):
        # word cloud
        plot_key = (league_id, 'tags_ax', self.view_player)
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            wordcloud_image, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:          
            self.streamer.status(1/self.plot_counts * (1/2))
            self.streamer.print('\t...genres', base=False)

            text, text_ex = self.get_wordcloud_text(genres_df, tags_df, exclusives_df)
            wordcloud_image = self.draw_wordcloud(text, mask_bytes)
        
            self.streamer.status(1/self.plot_counts * (1/2))

            parameters = {'top_tags': [t[0] for t in text.most_common(3)],                          
                          'highlight': self.paintbrush.hex_color(self.highlight_color),
                          }
            if self.view_player != self.god_player:
                parameters['exlcusives'] = [t[0] for t in text_ex.most_common(3)]
            self.streamer.store_session_state(plot_key, (wordcloud_image, parameters))

        self.streamer.image(wordcloud_image, header=title,
                            tooltip=self.library.get_tooltip('tags', parameters=parameters), tab=tab)

    def get_wordcloud_text(self, genres_df, tags_df, exclusives_df):
        text = Counter(genres_df.groupby('genre')['occurances'].sum().to_dict())
        self.player_tags = tags_df if self.view_player != self.god_player else []

        if self.view_player != self.god_player:
            text_ex = text.copy()
            for k in text.keys():
                if k not in exclusives_df.to_list():
                    del text_ex[k]
        else:
            text_ex = None

        return text, text_ex

    def draw_wordcloud(self, text, mask_bytes):
        mask = self.canvas.get_mask_array(mask_bytes)

        wordcloud = WordCloud(mode='RGBA', background_color=None, mask=mask)\
            .generate_from_frequencies(text)\
            .recolor(color_func=self.word_color)
        wordcloud_image = wordcloud.to_array()

        return wordcloud_image
    
    def word_color(self, word, font_size, position, orientation, random_state=None, **kwargs):
        color = rand_choice(self.pass_colors if (word in self.player_tags) else self.fail_colors)
        
        return color

    def plot_pie(self, league_id, categories_df, title=None, tab=None):
        # pie chart
        plot_key = (league_id, 'categories_ax', self.view_player)
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            ax, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:
            self.streamer.status(1/self.plot_counts * (1/2))
            self.streamer.print('\t...categories', base=False)
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1]) 

            ax.pie(categories_df['occurances'], labels=categories_df['category'],
               colors=self.paintbrush.get_plot_colors()) #autopct='%1f%%', 
            ax.axis('equal')
            
            self.streamer.status(1/self.plot_counts * (1/2))

            parameters = None
            self.streamer.store_session_state(plot_key, (ax, parameters))

        self.streamer.pyplot(ax.figure, header=title,
                             tooltip=self.library.get_tooltip('pie', parameters=parameters), tab=tab)

        plt.close()
    
    def plot_top_songs_summary(self, league_id, results_df, descriptions, max_years=10, base=500):
        self.streamer.print('\t...songs', base=False)
        plot_key = (league_id, 'top_songs_ax')
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            round_ids, n_rounds, n_years, years_range, max_date, \
                text_df, W, H, x0, x1, _, _, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:
            self.streamer.status(1/self.plot_counts * (1/4))
            
            round_ids = list(results_df['round_id'].unique())

            results_df.index = range(len(results_df))

            results_df['text'] = results_df.apply(lambda x: ' + '.join(x['artist']) + ' "' + x['title'] + '"', axis=1)
            results_df['text_top'] = results_df.apply(lambda x: ' + '.join(x['artist']), axis=1)
            results_df['text_bottom'] = results_df.apply(lambda x: '"' + x['title'] + '"', axis=1)

            rdfgb = results_df.reset_index().groupby('round_id').rank()['index']
            rdfgb2 = results_df.groupby('round_id').count()['song_id']
            divisor = results_df['round_id'].map(rdfgb2.to_dict()).apply(self.sum_num)

            results_df['size'] = rdfgb.add(1).pow(-1).div(divisor)
            results_df['y'] = rdfgb.sub(1).apply(self.sum_num).div(divisor)
       
            self.streamer.status(1/self.plot_counts * (1/4))
    
            max_date = results_df['release_date'].max()
            dates = to_datetime(results_df['release_date'])
            outlier_date = dates.where(dates > dates.mean() - dates.std()).min()
            min_date = max_date.replace(year=outlier_date.year)

            rgb_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('purple', 'red', 'orange', 'yellow',
                                                                             'green', 'blue', 'dark_blue'))

            results_df['x'] = results_df.apply(lambda x: date2num(max(min_date, x['release_date'])), axis=1)
            results_df['font_name'] = results_df.apply(lambda x: self.fonts['image_bold'] if x['closed'] else self.fonts['image_sans'], axis=1)
            results_df['color'] = results_df.apply(lambda x: self.paintbrush.get_rgb(rgb_df, x['points'] / results_df[results_df['round_id'] == x['round_id']]['points'].max() \
                                                if results_df[results_df['round_id'] == x['round_id']]['points'].max() else nan, self.paintbrush.get_color('grey')), axis=1)
            results_df['highlight'] = results_df['submitter_id'] == self.view_player

            self.streamer.status(1/self.plot_counts * (1/4))
        
            n_years = max_date.year - min_date.year
            years_range = range(0, n_years, max(1, ceil(n_years/max_years)))

            average_year = results_df.groupby('round_id').first()['release_date'].apply(Timestamp).mean().year

            text_image_results = self.canvas.get_time_parameters(results_df,
                                                        self.subplot_aspects['top_songs'],
                                                        base)
            text_df, W, H, x0, x1 = text_image_results
            
            n_rounds = len(round_ids)

            parameters = {'max_year': max_date.year,
                          'min_year': min_date.year,
                          'average_age': datetime.today().year - average_year,
                          'oldest_year': results_df['release_date'].min().year,
                          }
            self.streamer.store_session_state(plot_key, (round_ids, n_rounds, n_years, years_range, max_date,
                                                         text_df, W, H, x0, x1, base, descriptions, parameters))
        self.streamer.wrapper(header=None,
                                tooltip=self.library.get_tooltip('top_songs', parameters=parameters))
                
    def plot_top_songs(self, league_id, tab=None):
        plot_key = (league_id, 'top_songs_ax')
        stored, _ = self.streamer.get_session_state(plot_key)
        round_ids, n_rounds, n_years, years_range, max_date, \
            text_df, W, H, x0, x1, base, descriptions, _ = stored

        for r in round_ids:
            plot_key_i = (league_id, 'top_songs_ax', r)
            stored, ok = self.streamer.get_session_state(plot_key_i)
            if ok:
                ax, parameters_i = stored
                self.streamer.status(1/self.plot_counts * (1/n_rounds))

            else:
                fig = plt.figure()
                ax = fig.add_axes([1, 1, 1, 1])
            
                image = self.canvas.get_timeline_image(text_df[text_df['round_id'] == r], W, H, x0, x1,
                                                        base, self.highlight_color)

                ax.imshow(image)
                
                ax.set_yticks([])
                ax.set_yticklabels([])

                ax.set_xticks([x0 + (W - x0 - x1) / n_years * y for y in years_range] + [W - x1])
                ax.set_xticklabels([max_date.year - i for i in years_range] + [f'<{max_date.year - max(years_range)}'])
                
                self.streamer.status(1/self.plot_counts * (1/n_rounds))

                parameters_i = {'description': descriptions[descriptions['round_id'] == r]['description'].iloc[0],
                                }
                self.streamer.store_session_state(plot_key_i, (ax, parameters_i))
                    
            self.streamer.pyplot(ax.figure, header=round_ids.index(r) + 1, header2=self.get_round_title(r),
                                    tooltip=self.library.get_tooltip('top_songs_round', parameters=parameters_i), tab=tab)
                
    def sum_num(self, num):
        return sum(1/(n+2) for n in range(int(num)))

    def plot_playlists(self, league_id, playlists_df, track_count, duration, width=400, height=80):#600):
        self.streamer.status(1/self.plot_counts)

        playlist_uri = playlists_df.query('theme == "complete"')['uri'].iloc[0].replace('spotify:playlist:', '')

        html = (f'<iframe src="{SPOTIFY_PLAY_URL}/embed/playlist/{playlist_uri}" '
                f'width="{width}" height="{height}" '
                f'frameborder="0" allowtransparency="true" allow="encrypted-media"></iframe>'
                )

        parameters = {'count': track_count,
                      'duration': duration,
                      }
        self.streamer.embed(html, height=height, header='League Playlist',
                            tooltip=self.library.get_tooltip('playlist', parameters=parameters))

        plt.close()

    def plot_hoarding(self, league_id, awards_round_df, awards_league_df, title=None, tab=None):
        ''' plot votes shares '''
        plot_key = (league_id, 'hoarding_ax', self.view_player)
        stored, ok = self.streamer.get_session_state(plot_key)
        if ok:
            ax, parameters = stored
            self.streamer.status(1/self.plot_counts)
            
        else:
            self.streamer.status(1/self.plot_counts)
            print(awards_round_df)
            hoarding_df = awards_round_df.pivot(index='player_id', columns='round_id', values='generosity')
            most_generous = awards_league_df[awards_league_df['generous']==awards_league_df['generous'].min()]['player_id'].to_list()
            least_generous = awards_league_df[awards_league_df['generous']==awards_league_df['generous'].max()]['player_id'].to_list()

            player_ids = hoarding_df.index
            round_ids = hoarding_df.columns

            angles = [i*2*pi/len(round_ids) for i in range(len(round_ids))]
            angles = [self.reduce_angle(self.flip_angle(a) + sum(angles)/len(angles) - pi/2) for a in angles]

            fig, ax = plt.subplots(nrows=1, ncols=1, subplot_kw={'polar': True})

            adj = DataFrame([[(hoarding_df.loc[:i, c] == hoarding_df.loc[i, c]).sum() - 1 for c in hoarding_df.columns] \
                for i in hoarding_df.index], columns=hoarding_df.columns, index=hoarding_df.index).mul(3*pi/180)

            if len(round_ids) > 2:
                adj[adj.columns[0]] = 0
                adj[adj.columns[-1]] = 0

            for player_id in player_ids:
                color = self.paintbrush.normalize_color(self.highlight_color) if player_id == self.view_player else None
                zorder = 1 if player_id == self.view_player else 0

                angles_adj = (array(angles) + array(adj.loc[player_id])).tolist()

                ax_method = ax.plot if (len(round_ids) > 2) else ax.scatter
                ax_method(angles_adj, hoarding_df.loc[player_id],
                        color=color, label=self.texter.get_display_name(self.get_player_name(player_id)), zorder=zorder)

                ax.fill([angles_adj[0]] + angles_adj + [angles_adj[-1]],
                        [0] + hoarding_df.loc[player_id].tolist() + [0],
                        color=color, alpha=0.2)

            ax.set_xticks(angles)
            ax.set_xticklabels(self.get_round_title(round_ids, clean=True))
            ax.legend()

            parameters = {'generous': self.get_player_name(most_generous),
                          'hoarder': self.get_player_name(least_generous),
                          }

            self.streamer.store_session_state(plot_key, (ax, parameters))

        self.streamer.pyplot(ax.figure, header=title, #in_expander=fig.get_size_inches()[1] > 6,
                             tooltip=self.library.get_tooltip('hoarding', parameters=parameters), tab=tab)

        plt.close()

    def flip_angle(self, angle):
        if 0 <= angle <= pi:
            flipped = pi - angle
        else:
            flipped = 3*pi - angle
        return flipped

    def reduce_angle(self, angle):
        return angle % (2*pi)