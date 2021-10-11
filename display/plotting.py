from math import sin, cos, atan2, pi, nan, isnan, ceil
from collections import Counter
from os.path import dirname, realpath
from datetime import datetime

from pandas import DataFrame, isnull, to_datetime, Timestamp
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib import font_manager
from matplotlib.dates import date2num#, num2date
from wordcloud import WordCloud#, ImageColorGenerator
from numpy import unique

from common.words import Texter
from display.library import Librarian
from display.artist import Canvas, Paintbrush
from display.storage import Boxer
from display.streaming import Streamable

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
        self.librarian = Librarian()
        self.paintbrush = Paintbrush()
        self.boxer = Boxer()

        self.streamer = streamer
        self.database = database
        
        # define fonts to use
        self.fonts = self.librarian.get_and_set_fonts(font_manager, rcParams,
                                                    dirname(dirname(realpath(__file__))))       
        self.league_titles = None
        self.view_player = None
        self.plot_counts = len((self.plot_members,
                                self.plot_boards,
                                self.plot_rankings,
                                self.plot_features,
                                self.plot_tags,
                                self.plot_top_songs,
                                self.plot_playlists,
                                ))
    
    def add_canvas(self):
        canvas = Canvas(self.database, self.streamer)
        return canvas

    def add_analyses(self):
        self.streamer.print('Getting analyses')
        analyses_df = self.database.get_analyses()

        if len(analyses_df):
            self.league_titles = analyses_df['league']
            self.canvas = self.add_canvas()
        else:
            self.league_titles = []

    def prepare_dfs(self, key, function, *args):
        df, ok = self.streamer.get_session_state(key)
        if not ok:
            df = function(*args)
            self.streamer.store_session_state(key, df)

        return df

    def plot_results(self):
        player_names = self.database.get_player_names()
        self.view_player = self.streamer.player_box.selectbox('Who are you?', player_names + [''],
                                                              index=len(player_names))

        if self.view_player != '':
            self.plot_viewer()

            league_titles = self.database.get_player_leagues(self.view_player)
            viewable_league_titles = [l for l in league_titles if l in self.league_titles.to_list()]
            if len(viewable_league_titles) == 1:
                league_title = viewable_league_titles[0]
            elif len(viewable_league_titles) > 1:
                league_title = self.streamer.selectbox.selectbox('Pick a league to view',
                                                                 ['<select>'] + viewable_league_titles)
            else:
                league_title = '<select>'

            if league_title == '<select>':
                pass

            else:
                self.streamer.print(f'Preparing plot for {league_title}')
                self.streamer.status(0, base=True)

                league_creator = self.prepare_dfs('league_creator', self.database.get_league_creator, league_title)
                self.plot_title(league_title, league_creator)
           
                members_df = self.prepare_dfs('members_df', self.database.get_members, league_title)
                self.plot_members(league_title, members_df)

                boards_df = self.prepare_dfs('boards_df', self.database.get_boards, league_title)
                creators_and_winners_df = self.prepare_dfs('creators_and_winners_df', self.database.get_creators_and_winners, league_title)
                self.plot_boards(league_title, boards_df, creators_and_winners_df)

                rankings_df = self.prepare_dfs('rankings_df', self.database.get_rankings, league_title)
                dirtiness_df = self.prepare_dfs('dirtiness_df', self.database.get_dirtiness, league_title)
                discovery_df = self.prepare_dfs('discovery_df', self.database.get_discovery_scores, league_title)
                self.plot_rankings(league_title, rankings_df, dirtiness_df, discovery_df)
                                   
                features_df = self.prepare_dfs('features_df', self.database.get_audio_features, league_title)
                self.plot_features(league_title, features_df)
                
                tags_df = self.prepare_dfs('tags_df', self.database.get_genres_and_tags, league_title)
                exclusives_df = self.prepare_dfs('exclusives_df', self.database.get_exclusive_genres, league_title)
                masks = self.prepare_dfs('masks', self.boxer.get_mask, league_title)
                self.plot_tags(league_title, tags_df, exclusives_df, masks)

                results_df = self.prepare_dfs('results_df', self.database.get_song_results, league_title)
                descriptions_df = self.prepare_dfs('descriptions_df', self.database.get_round_descriptions, league_title)
                self.plot_top_songs(league_title, results_df, descriptions_df)

                playlists_df = self.prepare_dfs('playlists_df', self.database.get_playlists, league_title)
                track_count = self.prepare_dfs('track_count', self.database.get_track_count, league_title)
                track_durations = self.prepare_dfs('track_durations', self.database.get_track_durations, league_title)
                self.plot_playlists(league_title, playlists_df, track_count, track_durations)
            
                self.streamer.print('Everything loaded! Close this sidebar to view.')

    def plot_image(self, ax, x, y, player_name=None, color=None, size=0.5,
                   image_size=(0, 0), padding=0, text=None,
                   aspect=(1, 1), flipped=False, zorder=0):
        flip = -1 if flipped else 1

        if player_name:
            image = self.canvas.get_player_image(player_name)
        elif color:
            image = self.canvas.get_color_image(color, image_size)
        else:
            image = None
            imgs = None

        if image and text:
            image = self.canvas.add_text(image, text, self.fonts['image_sans'])

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

    def plot_viewer(self):
        image = self.canvas.get_player_image(self.view_player)
        #image = self.boxer.get_welcome()

        self.streamer.image(image, in_expander=False, right_column=[self.view_player])
        ##self.streamer.wrapper(None, tooltip=self.librarian.get_tooltip('welcome'))

    def plot_title(self, league_title, creator):
        parameters = {'title': league_title,
                      'creator': creator,
                      'viewer': self.view_player,
                      }
        self.streamer.title(league_title,
                            tooltip=self.librarian.get_tooltip('title', parameters=parameters))

        self.streamer.sidebar_image(self.boxer.get_cover(league_title))

    def plot_members(self, league_title, members_df):
        plot_key = (league_title, 'members_ax')
        ax, ok = self.streamer.get_session_state(plot_key)
        if ok:
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])

            # plot nodes for players
            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...relationships', base=False)
            x = members_df['x']
            y = members_df['y']
            player_names = members_df['player']

            # plot center
            x_center, y_center = self.get_center(members_df)
            ax.scatter(x_center, y_center, marker='1', zorder=2*len(player_names))

            sizes = self.get_scatter_sizes(members_df)
            colors = self.get_node_colors(members_df)
            colors_scatter = self.paintbrush.get_scatter_colors(colors)
       
            for x_p, y_p, p_name, s_p, c_p, c_s, z in zip(x, y, player_names, sizes, colors, colors_scatter, player_names.index):
                self.plot_member_nodes(ax, x_p, y_p, p_name, s_p, c_p, c_s, z)
            self.streamer.status(1/self.plot_counts * (1/3))

            # split if likes is liked
            split = members_df.set_index('player')
            split = split['likes'] == split['liked']

            for i in members_df.index:
                self.plot_member_relationships(ax, player_names[i], members_df, split,
                                               zorder=2*len(player_names))
            self.streamer.status(1/self.plot_counts * (1/3))

            ax.axis('equal')
            ax.set_ylim(members_df['y'].min() - self.name_offset - self.font_size,
                        members_df['y'].max() + self.name_offset + self.font_size)
            ax.axis('off')

            self.streamer.store_session_state(plot_key, ax)

        parameters = {'leader': members_df[members_df['wins'] == members_df['wins'].max()]['player'].values,
                      'closest_dfc': members_df[members_df['dfc'] == members_df['dfc'].min()]['player'].values,
                      }

        self.streamer.pyplot(ax.figure, header='League Pulse',
                             tooltip=self.librarian.get_tooltip('members', parameters=parameters))

    def plot_member_nodes(self, ax, x_p, y_p, p_name, s_p, c_p, c_s, z):
        plot_size = (s_p/2)**0.5/pi/10
        image, _ = self.plot_image(ax, x_p, y_p, player_name=p_name, size=plot_size, flipped=False,
                                   zorder=2*z+1)
        if image:
            _, _ = self.plot_image(ax, x_p, y_p, color=c_p, size=plot_size, image_size=image.size, padding=0.05,
                                   zorder=2*z)
        else:
            ax.plot(x_p, y_p, s=s_p, c=c_s)

    def get_node_colors(self, members_df):
        max_dfc = members_df['dfc'].max()
        min_dfc = members_df['dfc'].min()

        rgb_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('green', 'blue'))
        colors = [self.paintbrush.get_rgb(rgb_df, 1-(dfc - min_dfc)/(max_dfc - min_dfc),
                               fail_color=self.paintbrush.get_color('grey')) for dfc in members_df['dfc'].values]
        
        return colors

    def plot_member_relationships(self, ax, me, members_df, split, zorder):
        # get location
        x_me, y_me, theta_me = self.where_am_i(members_df, me)

        # plot names
        self.plot_member_names(ax, me, x_me, y_me, theta_me, zorder=zorder+2)

        # split if liked
        split_distance = split[me] * self.like_arrow_split

        # find likes
        self.plot_member_likers(ax, members_df, me, x_me, y_me, split_distance, direction='likes',
                                color=self.likes_color, zorder=zorder+1)
        self.plot_member_likers(ax, members_df, me, x_me, y_me, split_distance, direction='liked',
                                color=self.liked_color, zorder=zorder+1)

    def plot_member_names(self, ax, me, x_me, y_me, theta_me, zorder=0):
        x_1, y_1 = self.translate(x_me, y_me, theta_me, 0, shift_distance=-self.name_offset) ## <- name offset should be based on node size

        h_align = 'right' if (theta_me > -pi/2) & (theta_me < pi/2) else 'left'
        v_align = 'top' if (theta_me > 0) else 'bottom'
        display_name = self.texter.get_display_name(me)
            
        ax.text(x_1, y_1, display_name, horizontalalignment=h_align, verticalalignment=v_align,
                zorder=zorder)
        
    def plot_member_likers(self, ax, members_df, me, x_me, y_me, split_distance, direction, color, zorder=0):
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

    def where_am_i(self, members_df, player_name):
        x_me = members_df['x'][members_df['player'] == player_name].values[0]
        y_me = members_df['y'][members_df['player'] == player_name].values[0]
        x_center, y_center = self.get_center(members_df)
        theta_me = atan2(y_center - y_me, x_center - x_me)

        return x_me, y_me, theta_me

    def who_likes_whom(self, members_df, player_name, direction, line_dist):
        likes_me = members_df[direction][members_df['player'] == player_name].values[0]

        x_me, y_me, _ = self.where_am_i(members_df, player_name)
        them = members_df[['x', 'y']][members_df['player'] == likes_me]
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

    def plot_boards(self, league_title, board, creators_winners_df):
        plot_key = (league_title, 'boards_ax')
        ax, ok = self.streamer.get_session_state(plot_key)
        if ok:
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])

            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...rankings', base=False)
            n_rounds = len(board.columns)
            n_players = len(board.index)
            aspect = (n_rounds - 1, n_players - 1)
            scaling = [a / b * aspect[1] for a, b in zip(self.subplot_aspects['golden'], aspect)]

            xs = [x * scaling[0] for x in range(1, n_rounds + 1)]

            has_dnf = board.where(board < 0, 0).sum().sum() < 0
        
            lowest_rank = int(board.where(board > 0, 0).max().max())
            highest_dnf = int(board.where(board < 0, 0).min().min())

            ties_df = DataFrame(0, columns=unique(board.values), index=xs)

            for player in board.index:
                self.plot_board_player(ax, xs, player, board, lowest_rank, ties_df) #ties_df =
            self.streamer.status(1/self.plot_counts * (1/3))

            round_titles = [self.texter.clean_text(c) for c in board.columns]
        
            x_min = min(xs)
            x_max = max(xs)

            if has_dnf:
                # plot DNF line
                ax.plot([x_min - scaling[0]/2, x_max + scaling[0]/2], [lowest_rank + 1] * 2, '--', color='0.5',
                        zorder=0)
            self.streamer.status(1/self.plot_counts * (1/3))

            ax.set_xlim(x_min - scaling[0]/2, x_max + scaling[0]/2)
            ax.set_xticks(xs)
            ax.set_xticklabels(self.split_labels(round_titles, n_rounds),
                               rotation=self.rotate_labels(n_rounds))

            y_min = lowest_rank - highest_dnf + has_dnf
            y_max = 0
            yticks = range(y_min, y_max, -1)

            ax.set_ylim(y_min + 0.5, y_max + 0.5)
            ax.set_yticks(yticks)
            ax.set_yticklabels([int(y) if y <= lowest_rank else 'DNF' if y == lowest_rank + 2 else '' for y in yticks])

            self.streamer.store_session_state(plot_key, ax)

        parameters = {'round_titles': creators_winners_df['round'].values,
                      'choosers': creators_winners_df['creator'].values,
                      'winners': creators_winners_df['winner'].values,
                      }
        self.streamer.pyplot(ax.figure, header='Round Finishers',
                             tooltip=self.librarian.get_tooltip('boards', parameters=parameters))

    def plot_board_player(self, ax, xs, player, board, lowest_rank, ties_df):
        ys = board.where(board > 0).loc[player]
        ds = [lowest_rank - d + 1 for d in board.where(board < 0).loc[player]]

        ties = board.eq(board.loc[player]).sum()

        display_name = self.texter.get_display_name(player)

        color = f'C{board.index.get_loc(player)}'
        ax.plot(xs, ys, marker='.', color=color, zorder=0)
        ax.scatter(xs, ds, marker='.', color=color, zorder=0)

        for x, y, d, t in zip(xs, ys, ds, ties):
            size = self.ranking_size# * t**0.5 / t
            if y > 0:
                x_plot, y_plot = self.adjust_ties(x, y, t, ties_df.loc[x, y])
                ties_df.loc[x, y] += 1

                # plot finishers
                image, _ = self.plot_image(ax, x_plot, y_plot, player_name=player, size=size, flipped=True,
                                           zorder=1)
                if not image:
                    ax.text(x, y, display_name)
                

            if d > lowest_rank + 1:
                # plot DNFs
                image, _ = self.plot_image(ax, x, d, player_name=player, size=size, flipped=True,
                                           zorder=1)
                if not image:
                    ax.text(x, d, display_name)

    def adjust_ties(self, x, y, t, i, overlap=0.95):
        if t == 1:
            x_plot = x
            y_plot = y

        else:
            angle = 2*pi / t

            adj = pi / 4 if t == 2 else 0
            R = self.ranking_size/2 * overlap
            x_plot = R * cos(angle * i + adj) + x
            y_plot = R * sin(angle * i + adj) + y
            
        return x_plot, y_plot

    def plot_rankings(self, league_title, rankings, dirty_df, discovery_df):
        plot_key = (league_title, 'rankings_ax')
        ax, ok = self.streamer.get_session_state(plot_key)
        if ok:
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...scores', base=False)
            rankings_df = rankings.reset_index().pivot(index='player', columns='round', values='score').div(100)\
                .reindex(columns=rankings.index.get_level_values(0).drop_duplicates()).sort_index(ascending=False)

            player_names = rankings_df.index
            n_rounds = len(rankings_df.columns)
                        
            fig_w, _ = fig.get_size_inches()
            fig.set_size_inches([fig_w, fig_w / (n_rounds + 4) * len(player_names)])

            max_score = rankings_df.max().max()
            rgb_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('red', 'yellow', 'green', 'blue'))
        
            max_dirty = max(0.5, dirty_df.max())
            rgb_dirty_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('purple', 'peach'))

            max_discovery = 1
            rgb_discovery_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('grey', 'dark_blue'))

            xs = range(n_rounds)
            self.streamer.status(1/self.plot_counts * (1/3))
            for player in player_names:
                y = player_names.get_loc(player)
                marker_size = 0.9
                image_size = self.plot_player_scores(ax, player, xs, y, rankings_df, max_score, rgb_df, marker_size)

                # plot dirtiness
                self.plot_player_score(ax, len(xs), y, dirty_df[player], max_dirty, rgb_dirty_df,
                                       marker_size, image_size, percent=True)

                # plot discovery
                self.plot_player_score(ax, len(xs)+1, y, discovery_df['discovery'][player], max_discovery, rgb_discovery_df,
                                       marker_size, image_size, percent=True)
                self.plot_player_score(ax, len(xs)+2, y, discovery_df['popularity'][player], max_discovery, rgb_discovery_df,
                                       marker_size, image_size, percent=True)
            self.streamer.status(1/self.plot_counts * (1/3))
            
            ax.axis('equal')
            #ax.axis('off')
            ax.spines['left'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.set_yticklabels([])
            ax.set_yticks([])

            ax.set_xticks([(n_rounds-1)/2] + [n_rounds + i for i in range(3)])
            ax.set_xticklabels(['scores', 'dirtiness', 'discovery', 'popularity'],
                               rotation=self.rotate_labels(n_rounds))

            self.streamer.store_session_state(plot_key, ax)

        parameters = {'dirty': dirty_df[dirty_df == dirty_df.max()].index.values,
                      'discovery': discovery_df[discovery_df['discovery'] == discovery_df['discovery'].max()].index.values,
                      'popular': discovery_df[discovery_df['popularity'] == discovery_df['popularity'].max()].index.values,
                      }
        self.streamer.pyplot(ax.figure, header='Player Scores',
                             tooltip=self.librarian.get_tooltip('rankings', parameters=parameters))

    def plot_player_scores(self, ax, player, xs, y, rankings_df, max_score, rgb_df, marker_size):
        ys = [y] * len(xs)
        
        scores = rankings_df.loc[player]
        colors = [self.paintbrush.get_rgb(rgb_df, score/max_score, fail_color=self.paintbrush.get_color('grey')) \
            for score in scores]
        colors_scatter = self.paintbrush.get_scatter_colors(colors)

        image, _ = self.plot_image(ax, -1, y, player_name=player, size=marker_size)
        if image:
            image_size = image.size
            for x, c, score in zip(xs, colors, scores):
                self.plot_image(ax, x, y, color=c, image_size=image_size, size=marker_size,
                                text=round(score*100) if not isnull(score) else 'DNF')
        else:
            image_size = None
            ax.scatter(xs, ys, s=20**2, c=colors_scatter) 
            for x, score in zip(xs, scores):
                ax.text(x, y, round(score*100) if not isnull(score) else 'DNF',
                        horizontalalignment='center', verticalalignment='center', color='white')

        return image_size

    def plot_player_score(self, ax, x, y, score, max_score, rgb_df, marker_size, image_size=None, percent=None):
        if percent:
            text = f'{score:.0%}'
        elif isnull(score):
            text = 'DNF'
        else:
            text = score

        color = self.paintbrush.get_rgb(rgb_df, score/max_score)
        if image_size:
            image, imgs = self.plot_image(ax, x, y, color=color, image_size=image_size, size=marker_size, text=text)
        else:
            image, imgs = None
        return image, imgs
            
    def plot_features(self, league_title, features_df):
        plot_key = (league_title, 'features_ax')
        ax, ok = self.streamer.get_session_state(plot_key)
        if ok:
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            self.streamer.status(1/self.plot_counts * (1/3))
            self.streamer.print('\t...features', base=False)
            features_solo = {#'duration': '⏲',
                             'tempo': '🥁',
                             }
            features_like = {'danceability': '💃',
                             'energy': '⚡',
                             'liveness': '🏟',
                             'valence': '💖',
                             'speechiness': '💬',
                             'acousticness': '🎸',
                             'instrumentalness': '🎹',
                             }
            available_colors = self.paintbrush.get_colors('red', 'blue', 'purple', 'peach', 'dark_blue', 'orange', 'aqua', 'gold', 'pink')

            features_colors = self.paintbrush.get_scatter_colors(available_colors)

            n_rounds = len(features_df)
        
            # ['loudness', 'key', 'mode']
            features_all = list(features_solo.keys()) + list(features_like.keys())
            mapper = {f'avg_{f}': f'{f}' for f in features_all}

            features_df['round'] = features_df['round'].apply(self.texter.clean_text)
        
            features_df = features_df.set_index('round').rename(columns=mapper)[features_all]
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
        
            self.streamer.store_session_state(plot_key, ax)

        self.streamer.pyplot(ax.figure, header='Audio Features',
                             tooltip=self.librarian.get_tooltip('features', parameters={}))

    def convert_axes(self, ax, z, y=True):
        if y:
            z1_0, z1_1 = ax.get_ylim()
            z2_0, z2_1 = ax.right_ax.get_ylim()
        else:
            z1_0, z1_1 = ax.get_xlim()
            z2_0, z2_1 = ax.right_ax.get_xlim()

        z_ = (z - z2_0) / (z2_1 - z2_0) * (z1_1 - z1_0) + z1_0

        return z_

    def plot_tags(self, league_title, tags_df, exclusives, mask_bytes):
        plot_key = (league_title, 'tags_ax')
        ax, ok = self.streamer.get_session_state(plot_key)
        if ok:
            wordcloud_image, parameters = ax
            self.streamer.status(1/self.plot_counts)
            
        else:          
            self.streamer.status(1/self.plot_counts * (1/2))
            self.streamer.print('\t...genres', base=False)
            mask = self.canvas.get_mask_array(mask_bytes)

            text = Counter(tags_df.dropna().sum().sum())
            
            wordcloud = WordCloud(background_color='white', mask=mask).generate_from_frequencies(text)
            wordcloud_image = wordcloud.to_array()
  
            text_ex = text.copy()
            for k in text.keys():
               if k not in exclusives.values:
                   del text_ex[k]

            self.streamer.status(1/self.plot_counts * (1/2))

            parameters = {'top_tags': [t[0] for t in text.most_common(3)],
                          'exclusives': [t[0] for t in text_ex.most_common(3)]
                          }
            self.streamer.store_session_state(plot_key, (wordcloud_image, parameters))
        self.streamer.image(wordcloud_image, header='Genre Cloud',
                            tooltip=self.librarian.get_tooltip('tags', parameters=parameters))

    def sum_num(self, num):
        return sum(1/(n+2) for n in range(int(num)))

    def plot_top_songs(self, league_title, results_df, descriptions, max_years=10):
        plot_key = (league_title, 'top_songs_ax')
        results, ok = self.streamer.get_session_state(plot_key)
        if False:#ok:
            self.streamer.status(1/self.plot_counts)
            
        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            self.streamer.status(1/self.plot_counts * (1/4))
            self.streamer.print('\t...songs', base=False)
            round_titles = list(results_df['round'].unique())

            results_df.index = range(len(results_df))

            results_df['text'] = results_df.apply(lambda x: ' + '.join(x['artist']) + ' "' + x['title'] + '"', axis=1)
            results_df['text_top'] = results_df.apply(lambda x: ' + '.join(x['artist']), axis=1)
            results_df['text_bottom'] = results_df.apply(lambda x: '"' + x['title'] + '"', axis=1)

            divisor = results_df['round'].map(results_df.groupby('round').count()['song_id'].to_dict()).apply(self.sum_num)
            results_df['size'] = results_df.reset_index().groupby('round').rank()['index'].add(1).pow(-1)\
                .div(divisor)
            results_df['y'] = results_df.reset_index().groupby('round').rank()['index'].sub(1).apply(self.sum_num)\
                .div(divisor)
       
            self.streamer.status(1/self.plot_counts * (1/4))
    
            max_date = results_df['release_date'].max()
            dates = to_datetime(results_df['release_date'])
            outlier_date = dates.where(dates > dates.mean() - dates.std()).min()
            min_date = max_date.replace(year=outlier_date.year)

            rgb_df = self.paintbrush.grade_colors(self.paintbrush.get_colors('purple', 'red', 'orange', 'yellow',
                                                                             'green', 'blue', 'dark_blue'))

            results_df['x'] = results_df.apply(lambda x: max(min_date, x['release_date']), axis=1)
            results_df['font_name'] = results_df.apply(lambda x: self.fonts['image_bold'] if x['closed'] else self.fonts['image_sans'], axis=1)
            results_df['color'] = results_df.apply(lambda x: self.paintbrush.get_rgb(rgb_df, x['points'] / results_df[results_df['round'] == x['round']]['points'].max() \
                                                if results_df[results_df['round'] == x['round']]['points'].max() else nan, self.paintbrush.get_color('grey')), axis=1)
        
            self.streamer.status(1/self.plot_counts * (1/4))
        
            n_years = max_date.year - min_date.year
            years_range = range(0, n_years, max(1, ceil(n_years/max_years)))

            average_year = results_df.groupby('round').first()['release_date'].apply(Timestamp).mean().year
            parameters = {'max_year': max_date.year,
                          'min_year': min_date.year,
                          'average_age': datetime.today().year - average_year,
                          'oldest_year': results_df['release_date'].min().year,
                          }
            self.streamer.wrapper(header='Top Songs',
                                  tooltip=self.librarian.get_tooltip('top_songs', parameters=parameters))
                
            base = 500
            text_image_results = self.canvas.get_time_parameters(results_df,
                                                                   self.subplot_aspects['top_songs'],
                                                                   base)
            text_df, D, max_x, x_ratio, W, H, base = text_image_results

            n_rounds = len(round_titles)
            for i, r in enumerate(round_titles):
                image = self.canvas.get_timeline_image(text_df[text_df['round'] == r], max_x, x_ratio, W, H, base)

                ax.imshow(image)
                W, H = image.size
            
                ax.set_yticks([])
                ax.set_yticklabels([])

                ax.set_xticks([D / n_years * d for d in years_range] + [(W + D) / 2])
                ax.set_xticklabels([max_date.year - i for i in years_range] + [f'< {max_date.year - max(years_range)}'])
        
                ##ax.set_title(self.texter.clean_text(r), loc='left', fontweight='bold') #, horizontalalignment='left')
                
                self.streamer.status(1/self.plot_counts * (1/n_rounds))

                parameters = {'description': descriptions[descriptions['round'] == r]['description'].iloc[0],
                              }
                self.streamer.pyplot(ax.figure, header2=self.texter.clean_text(r),
                                     tooltip=self.librarian.get_tooltip('top_songs_round', parameters=parameters))
                
            results = None
            self.streamer.store_session_state(plot_key, results)

    def plot_playlists(self, league_title, playlists_df, track_count, duration,
                       width=400, height=600):
        self.streamer.status(1/self.plot_counts)

        playlist_uri = playlists_df.query('theme == "complete"')['uri'].iloc[0].replace('spotify:playlist:', '')

        html = (f'<iframe src="https://open.spotify.com/embed/playlist/{playlist_uri}" '
                f'width="{width}" height="{height}" '
                f'frameborder="0" allowtransparency="true" allow="encrypted-media"></iframe>'
                )

        parameters = {'count': track_count,
                      'duration': duration,
                      }
        self.streamer.embed(html, height=height, header='League Playlist',
                            tooltip=self.librarian.get_tooltip('playlist', parameters=parameters))