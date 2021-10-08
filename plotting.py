from math import sin, cos, atan2, pi, nan, isnan, ceil
from collections import Counter
import os

from PIL import Image, ImageDraw, ImageFont, UnidentifiedImageError
from pandas import DataFrame, isnull, to_datetime
import matplotlib.pyplot as plt
from matplotlib import rcParams
from matplotlib import font_manager
from matplotlib.dates import date2num, num2date
from wordcloud import WordCloud#, ImageColorGenerator
from numpy import asarray
import streamlit as st

from words import Texter
from media import Imager, Gallery
from storage import Boxer
from streaming import streamer

class Pictures(Imager):
    def __init__(self, database):
        super().__init__()

        self.gallery = Gallery(database, crop=True)
        self.boxer = Boxer()
        self.mobis = {}
        
    def get_player_image(self, player_name):
        image = self.gallery.get_image(player_name)

        if not image:
            # no Spotify profile exists
            if player_name not in self.mobis:
                # get a random image of Mobi that hasn't been used already
                mobi_byte = self.boxer.get_mobi()
                mobi_image = Image.open(mobi_byte) if mobi_byte else None
                self.mobis[player_name] = self.crop_image(mobi_image)

            image = self.mobis[player_name]

        return image

    def store_player_image(self, player_name, image):
        self.gallery.store_image(player_name, image)

    def add_text(self, image, text, font, color=(255, 255, 255), boundary=[0.75, 0.8]):
        draw = ImageDraw.Draw(image)

        text_str = str(text)
        
        bw, bh = boundary
        W, H = image.size
        font_size = round(H * 0.75 * bh)
        font_length = ImageFont.truetype(font, font_size).getmask(text_str).getbbox()[2]
        true_font_size = int(min(1, bw * W / font_length) * font_size)

        font = ImageFont.truetype(font, true_font_size)
        
        x0, y0, x1, y1 = draw.textbbox((0, 0), text_str, font=font)
        w = x1 - x0
        h = y1 + y0

        position = ((W-w)/2,(H-h)/2)
       
        draw.text(position, text_str, color, font=font)

        return image

    def get_mask_array(self, image_bytes):
        #fp = urlopen(src)
        try:
            image = Image.open(image_bytes) #fp)
            mask = asarray(image)
        except UnidentifiedImageError:
            streamer.print(f'can\'t open image') # at {src}')
            mask = None

        return mask

    def get_text_image(self, text_df, aspect, base):
        H = int((text_df['y_round'].max() + 1) * base)
        W = int(aspect[0] * H / aspect[1])

        ppi = 72

        max_x = text_df['x'].max()

        text_df['image_font'] = text_df.apply(lambda x: ImageFont.truetype(x['font_name'], int(x['font_size'] * ppi)), axis=1)
        text_df['length'] = text_df.apply(lambda x: x['image_font'].getmask(x['text']).getbbox()[2], axis=1)
        
        text_df['total_length'] = text_df['length'].add(date2num(max_x) - date2num(text_df['x']))
        max_x_i = text_df[text_df['total_length'] == text_df['total_length'].max()].index[0]

        X = (date2num(max_x) - date2num(text_df['x'][max_x_i]))
        x_ratio = (W - text_df['length'][max_x_i]) / X
        D = x_ratio * X
        
        image = Image.new('RGBA', (W, H), (0, 0, 0, 0))
        draw = ImageDraw.Draw(image)

        text_df['new_x'] = text_df.apply(lambda x: x_ratio * (date2num(max_x) - date2num(x['x'])) + x['total_length'], axis=1)

        for i in text_df.index:
            x_text = x_ratio * (date2num(max_x) - date2num(text_df['x'][i]))
            y_text = text_df[['y_round', 'y_song']].sum(1)[i] * base

            draw.text((x_text, y_text), text_df['text'][i],
                      fill=text_df['font_color'][i], font=text_df['image_font'][i])

        return image, D
    
class Plotter:
    color_wheel = 255
    
    dfc_colors = {'grey': (172, 172, 172),
                  'blue': (44, 165, 235),
                  'green': (86, 225, 132),
                  'red': (189, 43, 43),
                  'yellow': (255, 242, 119),
                  'purple': (192, 157, 224),
                  'peach': (224, 157, 204),
                  'dark_blue': (31, 78, 148),
                  'orange': (245, 170, 66),
                  'aqua': (85, 230, 203),
                  'pink': (225, 138, 227),
                  'gold': (145, 110, 45),
                  }

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
                       'top_songs': (2, 1),
                       }

    ranking_size = 0.75

    def __init__(self, database):
        self.texter = Texter()
        self.boxer = Boxer()
        
        # define fonts to use
        self.sans_fonts = list(self.texter.sans_fonts.keys())
        self.emoji_fonts = list(self.texter.emoji_fonts.keys())

        self.sans_font = list(self.texter.sans_fonts.values())[0]
        self.bold_font = list(self.texter.bold_fonts.values())[0]
        self.emoji_font = list(self.texter.emoji_fonts.values())[0]

        self.image_sans_font = f'fonts/{self.sans_font}'
        self.image_bold_font = f'fonts/{self.bold_font}'

        # set plotting fonts
        dir_path = os.path.dirname(os.path.realpath(__file__))
        #font_files = font_manager.list_fonts(directory=f'{dir_path}/fonts', extensions=['ttf'])
        font_dir = f'{dir_path}/fonts'
        font_files = font_manager.findSystemFonts(fontpaths=[font_dir])

        for font_file in font_files:
            font_manager.fontManager.addfont(font_file)

        self.plot_sans_font = font_manager.get_font(f'{font_dir}/{self.sans_font}').family_name
        self.plot_emoji_font = font_manager.get_font(f'{font_dir}/{self.emoji_font}').family_name

        rcParams['font.family'] = 'sans-serif'
        rcParams['font.sans-serif'] = self.sans_fonts
        
        self.league_titles = None
        self.members_list = None
        self.rankings_list = None
        self.boards_list = None
        self.pictures = None

        self.database = database

    def translate(self, x:float, y:float, theta:float, rotate:float, shift_distance:float=0):
        x_shifted = x + shift_distance*cos(theta + rotate*pi/2)
        y_shifted = y + shift_distance*sin(theta + rotate*pi/2)
        return x_shifted, y_shifted

    def get_dfc_colors(self, *color_names):
        colors = [self.dfc_colors[name] for name in color_names]

        if len(color_names) == 1:
            colors = colors[0]

        return colors

    def grade_colors(self, colors:list, precision:int=2):
        # create color gradient
        rgb_df = DataFrame(colors, columns=['R', 'G', 'B'],
                           index=[round(x/(len(colors)-1), 2) for x in range(len(colors))])\
                               .reindex([x/10**precision for x in range(10**precision+1)]).interpolate()
        return rgb_df

    def get_rgb(self, rgb_df:DataFrame, percent:float, fail_color=(0, 0, 0), astype=int):
        # get color based on interpolation of a list of colors
        if isnan(percent):
            rgb = fail_color
        else:
            rgb = tuple(rgb_df.iloc[rgb_df.index.get_loc(percent, 'nearest')].astype(astype))

        return rgb
    
    ##@st.cache(hash_funcs=)
    def add_pictures(self):
        pictures = Pictures(self.database)
        return pictures

    def add_analyses(self):
        streamer.print('Getting analyses')
        analyses_df = self.database.get_analyses()

        if len(analyses_df):
            self.league_titles = analyses_df['league']
            self.pictures = self.add_pictures()
        else:
            self.league_titles = []

    def plot_results(self):
        league_title = streamer.selectbox.selectbox('Pick a league to view', ['<select>'] + self.league_titles.to_list())

        if league_title != '<select>':
            streamer.print(f'Preparing plot for {league_title}')
            streamer.status(0, base=True)
            
            streamer.title(league_title)

            self.plot_members(league_title, self.database.get_members(league_title))
            self.plot_boards(league_title, self.database.get_boards(league_title))
            self.plot_rankings(league_title, self.database.get_rankings(league_title), self.database.get_dirtiness(league_title), self.database.get_discovery_scores(league_title))
            self.plot_features(league_title, self.database.get_audio_features(league_title))
            self.plot_tags(league_title, self.database.get_genres_and_tags(league_title), self.boxer.get_mask(league_title))
            self.plot_top_songs(league_title, self.database.get_song_results(league_title))

            streamer.clear_printer()

    def plot_image(self, ax, x, y, player_name=None, color=None, size=0.5,
                   image_size=(0, 0), padding=0, text=None,
                   aspect=(1, 1), flipped=False, zorder=0):
        flip = -1 if flipped else 1

        if player_name:
            image = self.pictures.get_player_image(player_name)
        elif color:
            image = self.pictures.get_color_image(color, image_size)
        else:
            image = None
            imgs = None

        if image and text:
            image = self.pictures.add_text(image, text, self.image_sans_font)

        if image:
            scaling = [a / max(aspect) for a in aspect]

            x_ex = (size + padding)/2 * scaling[0]
            y_ex = (size + padding)/2 * scaling[1]
            extent = [x - x_ex, x + x_ex,
                      y - flip*y_ex, y + flip*y_ex]
            imgs = ax.imshow(image, extent=extent, zorder=zorder)

        return image, imgs

    def plot_members(self, league_title, members_df):
        if f'members_ax:{league_title}' in st.session_state:
            ax = st.session_state[f'members_ax:{league_title}']
            streamer.status(1/6)

        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])

            # plot nodes for players
            streamer.status(1/6 * (1/3))
            streamer.print('\t...relationships', base=False)
            x = members_df['x']
            y = members_df['y']
            player_names = members_df['player']

            # plot center
            x_center, y_center = self.get_center(members_df)
            ax.scatter(x_center, y_center, marker='1', zorder=3)

            sizes = self.get_scatter_sizes(members_df)
            colors = self.get_node_colors(members_df)
            colors_scatter = self.get_scatter_colors(colors, divisor=self.color_wheel)
       
            for x_p, y_p, p_name, s_p, c_p, c_s in zip(x, y, player_names, sizes, colors, colors_scatter):
                self.plot_member_nodes(ax, x_p, y_p, p_name, s_p, c_p, c_s)
            streamer.status(1/6 * (1/3))

            # split if likes is liked
            split = members_df.set_index('player')
            split = split['likes'] == split['liked']

            for i in members_df.index:
                self.plot_member_relationships(ax, player_names[i], members_df, split)
            streamer.status(1/6 * (1/3))

            ax.axis('equal')
            ax.set_ylim(members_df['y'].min() - self.name_offset - self.font_size,
                        members_df['y'].max() + self.name_offset + self.font_size)
            ax.axis('off')

            st.session_state[f'members_ax:{league_title}'] = ax

        streamer.pyplot(ax.figure, header='League Pulse', tooltip=self.get_tooltip('members'))

    def plot_member_nodes(self, ax, x_p, y_p, p_name, s_p, c_p, c_s):
        plot_size = (s_p/2)**0.5/pi/10
        image, _ = self.plot_image(ax, x_p, y_p, player_name=p_name, size=plot_size, flipped=False, zorder=1)
        if image:
            _, _ = self.plot_image(ax, x_p, y_p, color=c_p, size=plot_size, image_size=image.size, padding=0.05, zorder=0)
        else:
            ax.plot(x_p, y_p, s=s_p, c=c_s)

    def get_node_colors(self, members_df):
        max_dfc = members_df['dfc'].max()
        min_dfc = members_df['dfc'].min()

        rgb_df = self.grade_colors(self.get_dfc_colors('green', 'blue'))
        colors = [self.get_rgb(rgb_df, 1-(dfc - min_dfc)/(max_dfc - min_dfc),
                               fail_color=self.get_dfc_colors('grey')) for dfc in members_df['dfc'].values]
        
        return colors

    def plot_member_relationships(self, ax, me, members_df, split):
        # get location
        x_me, y_me, theta_me = self.where_am_i(members_df, me)

        # plot names
        self.plot_member_names(ax, me, x_me, y_me, theta_me)

        # split if liked
        split_distance = split[me] * self.like_arrow_split

        # find likes
        self.plot_member_likers(ax, members_df, me, x_me, y_me, split_distance, direction='likes', color=self.likes_color)
        self.plot_member_likers(ax, members_df, me, x_me, y_me, split_distance, direction='liked', color=self.liked_color)

    def plot_member_names(self, ax, me, x_me, y_me, theta_me):
        x_1, y_1 = self.translate(x_me, y_me, theta_me, 0, shift_distance=-self.name_offset) ## <- name offset should be based on node size

        h_align = 'right' if (theta_me > -pi/2) & (theta_me < pi/2) else 'left'
        v_align = 'top' if (theta_me > 0) else 'bottom'
        display_name = self.texter.get_display_name(me)
            
        ax.text(x_1, y_1, display_name, horizontalalignment=h_align, verticalalignment=v_align, zorder=4)
        
    def plot_member_likers(self, ax, members_df, me, x_me, y_me, split_distance, direction, color):
        x_like, y_like, theta_us = self.who_likes_whom(members_df, me, direction, self.like_arrow_length)

        side = {'likes': -1,
                'liked': 1}[direction]

        x_1, y_1 = self.translate(x_me, y_me, theta_us, side, shift_distance=split_distance)
        x_2, y_2 = self.translate(x_like, y_like, theta_us, side, shift_distance=split_distance)

        xy = {'likes': [x_1, y_1, x_2-x_1, y_2-y_1],
              'liked': [x_2, y_2, x_1-x_2, y_1-y_2]}[direction]

        ax.arrow(*xy,
                 width=self.like_arrow_width, facecolor=color,
                 edgecolor='none', length_includes_head=True, zorder=2)

    def plot_boards(self, league_title, board):
        if f'boards_ax:{league_title}' in st.session_state:
            ax = st.session_state[f'boards_ax:{league_title}']
            streamer.status(1/6)

        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])

            streamer.status(1/6 * (1/3))
            streamer.print('\t...rankings', base=False)
            n_rounds = len(board.columns)
            n_players = len(board.index)
            aspect = (n_rounds - 1, n_players - 1)
            scaling = [a / b * aspect[1] for a, b in zip(self.subplot_aspects['golden'], aspect)]

            xs = [x * scaling[0] for x in range(1, n_rounds + 1)]

            has_dnf = board.where(board < 0, 0).sum().sum() < 0
        
            lowest_rank = int(board.where(board > 0, 0).max().max())
            highest_dnf = int(board.where(board < 0, 0).min().min())

            for player in board.index:
                self.plot_board_player(ax, xs, player, board, lowest_rank)
            streamer.status(1/6 * (1/3))

            round_titles = [self.texter.clean_text(c) for c in board.columns]
        
            x_min = min(xs)
            x_max = max(xs)

            if has_dnf:
                # plot DNF line
                ax.plot([x_min - scaling[0]/2, x_max + scaling[0]/2], [lowest_rank + 1] * 2, '--', color='0.5')
            streamer.status(1/6 * (1/3))

            ax.set_xlim(x_min - scaling[0]/2, x_max + scaling[0]/2)
            ax.set_xticks(xs)
            ax.set_xticklabels(round_titles, rotation=45)

            y_min = lowest_rank - highest_dnf + has_dnf
            y_max = 0
            yticks = range(y_min, y_max, -1)

            ax.set_ylim(y_min + 0.5, y_max + 0.5)
            ax.set_yticks(yticks)
            ax.set_yticklabels([int(y) if y <= lowest_rank else 'DNF' if y == lowest_rank + 2 else '' for y in yticks])

            st.session_state[f'boards_ax:{league_title}'] = ax

        streamer.pyplot(ax.figure, header='Round Finishers')

    def plot_board_player(self, ax, xs, player, board, lowest_rank):
        ys = board.where(board > 0).loc[player]
        ds = [lowest_rank - d + 1 for d in board.where(board < 0).loc[player]]

        display_name = self.texter.get_display_name(player)

        color = f'C{board.index.get_loc(player)}'
        ax.plot(xs, ys, marker='.', color=color)
        ax.scatter(xs, ds, marker='.', color=color)

        for x, y, d in zip(xs, ys, ds):
            if y > 0:
                # plot finishers
                image, _ = self.plot_image(ax, x, y, player_name=player, size=self.ranking_size, flipped=True, zorder=100)#, aspect=aspect)
                if not image:
                    ax.text(x, y, display_name)

            if d > lowest_rank + 1:
                # plot DNFs
                image, _ = self.plot_image(ax, x, d, player_name=player, size=self.ranking_size, flipped=True, zorder=100)#, aspect=aspect)
                if not image:
                    ax.text(x, d, display_name)

    def plot_rankings(self, league_title, rankings, dirty_df, discovery_df):
        if f'rankings_ax:{league_title}' in st.session_state:
            ax = st.session_state[f'rankings_ax:{league_title}']
            streamer.status(1/6)

        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            streamer.status(1/6 * (1/3))
            streamer.print('\t...scores', base=False)
            rankings_df = rankings.reset_index().pivot(index='player', columns='round', values='score').div(100)\
                .reindex(columns=rankings.index.get_level_values(0).drop_duplicates()).sort_index(ascending=False)

            player_names = rankings_df.index
            n_rounds = len(rankings_df.columns)

            max_score = rankings_df.max().max()
            rgb_df = self.grade_colors(self.get_dfc_colors('red', 'yellow', 'green', 'blue'))
        
            max_dirty = max(0.5, dirty_df.max())
            rgb_dirty_df = self.grade_colors(self.get_dfc_colors('purple', 'peach'))

            max_discovery = 1
            rgb_discovery_df = self.grade_colors(self.get_dfc_colors('grey', 'dark_blue'))

            xs = range(n_rounds)
            streamer.status(1/6 * (1/3))
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
            streamer.status(1/6 * (1/3))
            
            ax.axis('equal')
            #ax.axis('off')
            ax.spines['left'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.set_yticklabels([])
            ax.set_yticks([])

            ax.set_xticks([(n_rounds-1)/2] + [n_rounds + i for i in range(3)])
            ax.set_xticklabels(['scores', 'dirtiness', 'discovery', 'popularity'], rotation=45)

            st.session_state['rankings_ax:{league_title}'] = ax

        streamer.pyplot(ax.figure, header='Player Scores')

    def plot_player_scores(self, ax, player, xs, y, rankings_df, max_score, rgb_df, marker_size):
        ys = [y] * len(xs)
        
        scores = rankings_df.loc[player]
        colors = [self.get_rgb(rgb_df, score/max_score, fail_color=self.get_dfc_colors('grey')) \
            for score in scores]
        colors_scatter = self.get_scatter_colors(colors, divisor=self.color_wheel)

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

        color = self.get_rgb(rgb_df, score/max_score)
        if image_size:
            image, imgs = self.plot_image(ax, x, y, color=color, image_size=image_size, size=marker_size, text=text)
        else:
            image, imgs = None
        return image, imgs
            
    def plot_features(self, league_title, features_df):
        if f'features_ax:{league_title}' in st.session_state:
            ax = st.session_state[f'features_ax:{league_title}']
            streamer.status(1/6)

        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            streamer.status(1/6 * (1/3))
            streamer.print('\t...features', base=False)
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
            available_colors = self.get_dfc_colors('red', 'blue', 'purple', 'peach', 'dark_blue', 'orange', 'aqua', 'gold', 'pink')

            features_colors = self.get_scatter_colors(available_colors, divisor=self.color_wheel)

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
                             fontfamily=self.plot_emoji_font, horizontalalignment='center', padding=padding, size=font_size)
            
                    
            streamer.status(1/6 * (1/3))

            padding = 0.05
            for solo, f in zip(features_solo, range(len(features_solo))):
                color = features_colors[(len(features_like) + f) % len(features_colors)]
                features_df.plot(y=solo, color=color, #secondary_y=solo, 
                                 kind='line', legend=False, ax=ax)
            
                for i in range(n_rounds-1):
                    y = (features_df[solo][i] + features_df[solo][i+1])/2
                    padding_multiplier = -1 if y > 0.5 else 1
                    ax.text(x=i + 0.5, y=y + padding_multiplier * padding, s=features_solo[solo], # y=self.convert_axes(ax, (features_df[solo][i] + features_df[solo][i+1])/2)
                            size=font_size, color=color, fontfamily=self.plot_emoji_font, horizontalalignment='center') #font=self.emoji_font
                
                        
            streamer.status(1/6 * (1/3))

            for position in ['top', 'left', 'right']:
                ax.spines[position].set_visible(False)

            ax.set_yticklabels([])
            ax.set_yticks([])
        
            st.session_state[f'features_ax:{league_title}'] = ax

        streamer.pyplot(ax.figure, header='Audio Features')

    def convert_axes(self, ax, z, y=True):
        if y:
            z1_0, z1_1 = ax.get_ylim()
            z2_0, z2_1 = ax.right_ax.get_ylim()
        else:
            z1_0, z1_1 = ax.get_xlim()
            z2_0, z2_1 = ax.right_ax.get_xlim()

        z_ = (z - z2_0) / (z2_1 - z2_0) * (z1_1 - z1_0) + z1_0

        return z_

    def plot_tags(self, league_title, tags_df, mask_bytes): #_src):
        if f'tags_ax:{league_title}' in st.session_state:
            ax = st.session_state[f'tags_ax:{league_title}']
            streamer.status(1/6)

        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            streamer.status(1/6 * (1/2))
            streamer.print('\t...genres', base=False)
            mask = self.pictures.get_mask_array(mask_bytes) #_src)

            text = Counter(tags_df.dropna().sum().sum())
            wordcloud = WordCloud(background_color='white', mask=mask).generate_from_frequencies(text)
            ax.imshow(wordcloud, interpolation="bilinear")
            ax.axis('off')
        
            streamer.status(1/6 * (1/2))

            st.session_state[f'tags_ax:{league_title}'] = ax

        streamer.pyplot(ax.figure, header='Genre Cloud')

    def plot_top_songs(self, league_title, results_df, max_years=10):
        if f'top_songs_ax:{league_title}' in st.session_state:
            ax = st.session_state[f'top_songs_ax:{league_title}']
            streamer.status(1/6)

        else:
            fig = plt.figure()
            ax = fig.add_axes([1, 1, 1, 1])
            
            streamer.status(1/6 * (1/4))
            streamer.print('\t...songs', base=False)
            rounds = list(results_df['round'].unique())
            n_rounds = len(rounds)

            results_df['text'] = results_df.apply(lambda x: ' + '.join(x['artist']) + ' "' + x['title'] + '"', axis=1)
            results_df['y_round'] = results_df['round'].map({d: rounds.index(d) for d in results_df['round'].unique()})
            results_df['y_song'] = (1-1/(2**results_df['song_id'].map({d: list(results_df[results_df['round']==r]['song_id'].unique()).index(d) \
                for r in rounds for d in results_df[results_df['round']==r]['song_id'].unique()})))
            results_df['y'] = results_df[['y_round', 'y_song']].sum(1)
        
            streamer.status(1/6 * (1/4))
    
            max_date = results_df['release_date'].max()
            dates = to_datetime(results_df['release_date'])
            outlier_date = dates.where(dates > dates.mean() - dates.std()).min()
            min_date = max_date.replace(year=outlier_date.year)

            rgb_df = self.grade_colors(self.get_dfc_colors('purple', 'red', 'orange', 'yellow',
                                                            'green', 'blue', 'dark_blue'))

            results_df['x'] = results_df.apply(lambda x: max(min_date, x['release_date']), axis=1)
            results_df['font_size'] = (1 - results_df['y_song']) / 2
            results_df['font_name'] = results_df.apply(lambda x: self.image_bold_font if x['closed'] else self.image_sans_font, axis=1)
            results_df['font_color'] = results_df.apply(lambda x: self.get_rgb(rgb_df, x['points'] / results_df[results_df['round'] == x['round']]['points'].max() \
                                                if results_df[results_df['round'] == x['round']]['points'].max() else nan, self.get_dfc_colors('grey')), axis=1)
        
            streamer.status(1/6 * (1/4))
        
            base = 100
            image, D = self.pictures.get_text_image(results_df, self.subplot_aspects['top_songs'], base)
            
            streamer.status(1/6 * (1/4))
        
            ax.imshow(image)
            W, H = image.size
            
            ax.set_yticks([H / n_rounds * (n + 0.5) for n in range(n_rounds)])
            ax.set_yticklabels([self.texter.clean_text(r) for r in rounds])

            ax.yaxis.tick_right()

            n_years = max_date.year - min_date.year
            years_range = range(0, n_years, max(1, ceil(n_years/max_years)))
            ax.set_xticks([D / n_years * d for d in years_range] + [(W + D) / 2])
            ax.set_xticklabels([max_date.year - i for i in years_range] + [f'< {max_date.year - max(years_range)}'])
        
            st.session_state[f'top_songs_ax:{league_title}'] = ax

        streamer.pyplot(ax.figure, header='Top Songs')

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
        
        x_them = members_df['x'][members_df['player'] == likes_me].values[0]
        y_them = members_df['y'][members_df['player'] == likes_me].values[0]

        theta_us = atan2(y_them - y_me, x_them - x_me)

        x_likes = x_me + line_dist * cos(theta_us)
        y_likes = y_me + line_dist * sin(theta_us)

        return x_likes, y_likes, theta_us

    def get_scatter_sizes(self, members_df):
        sizes = (members_df['wins'] + 1) * self.marker_sizing
        return sizes

    def get_scatter_colors(self, colors_rgb, divisor=1):
        colors = [self.normalize_color(rgb, divisor) for rgb in colors_rgb]
        return colors

    def normalize_color(self, color, divisor):
        color = tuple(c / divisor for c in color)
        return color

    def get_tooltip(self, plot_name):
        members_text = (f'This shows the relationships between\n'
                        f'the league players. Players with similar\n'
                        f'music tastes are closer together. The\n'
                        f'arrows indicate who likes whom the most\n'
                        f'(the pink arrow shows who a player gives\n'
                        f'most of their votes to, and the orange\n'
                        f'arrow shows who they get the most votes\n'
                        f'from).'
                        )

        tooltips = {'members': members_text,
                    }

        tooltip = tooltips.get(plot_name)

        return tooltip
