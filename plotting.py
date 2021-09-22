from math import sin, cos, atan2, pi, isnan
from re import compile, UNICODE
from urllib.request import urlopen

from PIL import Image, ImageDraw, ImageFont, ImageOps, UnidentifiedImageError
from pandas import set_option, DataFrame, isnull
import matplotlib.pyplot as plt
from matplotlib import rcParams
##import mpld3

class Printer:
    def __init__(self, *options):
        self.options = [*options]

    #def set_display_options(self):
        for option in self.options:
            set_option(option, None)

class Texter:
    emoji_pattern = compile('['
                            u'\U0001F600-\U0001F64F' # emoticons
                            u'\U0001F300-\U0001F5FF' # symbols & pictographs
                            u'\U0001F680-\U0001F6FF' # transport & maps
                            u'\U0001F1E0-\U0001F1FF' # flags
                            u'\U00002500-\U00002BEF' # chinese char
                            u'\U00002702-\U000027B0'
                            u'\U00002702-\U000027B0'
                            u'\U000024C2-\U0001F251'
                            u'\U0001F926-\U0001F937'
                            u'\U00010000-\U0010FFFF'
                            u'\U000023E9-\U000023F3' # play, pause
                            ']+', flags=UNICODE)

    fonts = {'Segoe UI': 'segoeui.ttf',
             'Tahoma': 'tahoma.ttf'}

    emoji_fonts = {'Segoe UI Emoji': 'seguiemj.ttf'}

    def __init__(self):
        pass

    def clean_text(self, text:str) -> str:
        cleaned_text = self.emoji_pattern.sub(r'', text).strip()
        return cleaned_text

    def get_display_name(self, name:str) -> str:
        if ' ' in name:
            display_name = name[:name.index(' ')]
        elif '.' in name:
            display_name = name[:name.index('.')]
        else:
            display_name = name
        return display_name.title()

class Pictures:
    def __init__(self, database):
        self.database = database
        self.players_df = self.database.get_players()
        self.images = self.download_images()
        self.crop_player_images()
        
    def download_images(self):
        images = {}

        print('Downloading profile images...')
        for i in self.players_df.index:
            player_name = self.players_df['player'][i]
            print(f'\t...{player_name}')

            # download image
            src = self.players_df['src'][i]
            if src[:len('http')] != 'http':
                src = f'https://{src}'
            fp = urlopen(src)

            try:
                # see if image can load
                image = Image.open(fp)
            except UnidentifiedImageError:
                # image is unloadable
                print(f'unable to read image for {player_name}')
                image = None

            # store in images dictionary
            images[player_name] = image

        return images

    def get_player_image(self, player_name):
        image = self.images[player_name]

        return image

    def get_color_image(self, color, size):
        image = self.crop_image(Image.new('RGB', size, color))

        return image
    
    def crop_image(self, image):
        size = image.size
        mask = Image.new('L', size, 0)
        drawing = ImageDraw.Draw(mask)
        drawing.ellipse((0, 0) + size, fill=255)
        cropped = ImageOps.fit(image, mask.size, centering=(0.5, 0.5))
        cropped.putalpha(mask)

        return cropped

    def crop_player_images(self):
        for player in self.images:
            image = self.images[player]
            if image:
                self.images[player] = self.crop_image(image)

    def add_text(self, image, text, font, color=(255, 255, 255), padding=0.05):
        draw = ImageDraw.Draw(image)

        text_str = str(text)
        
        W, H = image.size
        font_size = round(H * 0.75 * (1-padding))
        font = ImageFont.truetype(font, font_size)
        
        x0, y0, x1, y1 = draw.textbbox((0, 0), text_str, font=font)
        w = x1 - x0
        h = y1 + y0

        position = ((W-w)/2,(H-h)/2)
       
        draw.text(position, text_str, color, font=font)

        return image

class Plotter:
    color_wheel = 255
    
    dfc_grey = (172, 172, 172)
    dfc_blue = (44, 165, 235)
    dfc_green = (86, 225, 132)
    dfc_red = (189, 43, 43)
    dfc_yellow = (255, 242, 119)
    dfc_purple = (192, 157, 224)
    dfc_peach = (224, 157, 204)
    dfc_dark_blue = (31, 78, 148)
    dfc_orange = (245, 170, 66)
    dfc_aqua = (85, 230, 203)

    marker_sizing = 50

    name_offset = 0.1
    font_size = 0.2
    
    likes_color = 'pink'
    liked_color = 'orange'
    like_arrow_width = 0.02
    like_arrow_length = 0.4
    like_arrow_split = 0.05

    #figure_size = (16, 10)
    figure_title = 'MusicLeague'

    subplot_aspect = ((1 + 5**0.5) / 2, 1)
    ranking_size = 0.75

    def __init__(self, database):
        self.texter = Texter()

        self.fonts = list(self.texter.fonts.keys())
        self.emoji_fonts = list(self.texter.emoji_fonts.keys())

        self.image_font = list(self.texter.fonts.values())[0]
        self.emoji_font = self.emoji_fonts[0]

        rcParams['font.family'] = 'sans-serif'
        rcParams['font.sans-serif'] = self.fonts
        ##rcParams['font.emoji'] = self.texter.emoji_fonts
        
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

    def grade_colors(self, colors:list, precision:int=2):
        # create color gradient
        rgb_df = DataFrame(colors, columns=['R', 'G', 'B'],
                           index=[round(x/(len(colors)-1), 2) for x in range(len(colors))])\
                               .reindex([x/10**precision for x in range(10**precision+1)]).interpolate()
        return rgb_df

    def get_rgb(self, rgb_df:DataFrame, percent:float, fail_color=(0, 0, 0)):
        # get color based on interpolation of a list of colors
        if isnan(percent):
            rgb = fail_color
        else:
            rgb = tuple(rgb_df.iloc[rgb_df.index.get_loc(percent, 'nearest')].astype(int))

        return rgb

    def add_anaylses(self):
        analyses_df = self.database.get_analyses()

        if len(analyses_df):
            league_titles = analyses_df['league']
            self.league_titles = league_titles

            db_calls = [self.database.get_members,
                        self.database.get_rankings,
                        self.database.get_boards,
                        self.database.get_dirtiness,
                        self.database.get_discovery_scores,
                        self.database.get_audio_features,
                        self.database.get_genres_pie]

            db_lists = [[db_call(league_title) for league_title in league_titles] for db_call in db_calls]
            (self.members_list,
             self.rankings_list,
             self.boards_list,
             self.dirty_list,
             self.discoveries_list,
             self.features_list,
             self.genres_list) = db_lists
             
            self.pictures = Pictures(self.database)

    def plot_results(self):
        nrows = 2
        ncols = 3
        n_leagues = len(self.members_list)

        for n in range(n_leagues):
            fig, axs = plt.subplots(nrows, ncols)

            # set league title
            league_title = self.league_titles[n]
            self.plot_title(fig, league_title)
        
            print(f'Preparing plot for {league_title}...')
            self.plot_members(axs[0][0], self.members_list[n])
            self.plot_boards(axs[1][1], self.boards_list[n])
            self.plot_rankings(axs[1][0], self.rankings_list[n], self.dirty_list[n], self.discoveries_list[n])
            self.plot_features(axs[0][1], self.features_list[n])
            self.plot_genres(axs[0][2], self.genres_list[n])
            # self.plot_top_songs(axs[0][1])

        #mpld3.show()

        print('Generating plot...')
        plt.show()

    def plot_title(self, fig, title):
        fig.suptitle(self.texter.clean_text(title))
        plt.get_current_fig_manager().set_window_title(f'{self.figure_title} - {self.texter.clean_text(title)}')
        # set figure size
        #fig.set_size_inches(*self.figure_size)

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
            image = self.pictures.add_text(image, text, self.image_font)

        if image:
            scaling = [a / max(aspect) for a in aspect]

            x_ex = (size + padding)/2 * scaling[0]
            y_ex = (size + padding)/2 * scaling[1]
            extent = [x - x_ex, x + x_ex,
                      y - flip*y_ex, y + flip*y_ex]
            imgs = ax.imshow(image, extent=extent, zorder=zorder)

        return image, imgs

    def plot_members(self, ax, members_df):
        # plot nodes for players
        print('\t...relationships')
        x = members_df['x']
        y = members_df['y']
        player_names = members_df['player']

        # plot center
        x_center, y_center = self.get_center(members_df)
        ax.scatter(x_center, y_center, marker='1', zorder=3)

        sizes = self.get_scatter_sizes(members_df)
        colors = self.get_colors(members_df)
        colors_scatter = self.get_scatter_colors(colors, divisor=self.color_wheel)
        
        for x_p, y_p, p_name, s_p, c_p, c_s in zip(x, y, player_names, sizes, colors, colors_scatter):
            self.plot_member_nodes(ax, x_p, y_p, p_name, s_p, c_p, c_s)

        # split if likes is liked
        split = members_df.set_index('player')
        split = split['likes'] == split['liked']

        for i in members_df.index:
            self.plot_member_relationships(ax, player_names[i], members_df, split)

        ax.axis('equal')
        ax.set_ylim(members_df['y'].min() - self.name_offset - self.font_size,
                    members_df['y'].max() + self.name_offset + self.font_size)
        ax.axis('off')

    def plot_member_nodes(self, ax, x_p, y_p, p_name, s_p, c_p, c_s):
        plot_size = size=(s_p/2)**0.5/pi/20
        image, imgs_1 = self.plot_image(ax, x_p, y_p, player_name=p_name, size=plot_size, flipped=False, zorder=1)
        if image:
            _, imgs_2 = self.plot_image(ax, x_p, y_p, color=c_p, size=plot_size, image_size=image.size, padding=0.05, zorder=0)
        else:
            ax.plot(x_p, y_p, s=s_p, c=c_s)

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
        x_1, y_1 = self.translate(x_me, y_me, theta_me, 0, shift_distance=-self.name_offset)

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

    def plot_boards(self, ax, board):
        print('\t...rankings')
        n_rounds = len(board.columns)
        n_players = len(board.index)
        aspect = (n_rounds - 1, n_players - 1)
        scaling = [a / b * aspect[1] for a, b in zip(self.subplot_aspect, aspect)]

        xs = [x * scaling[0] for x in range(1, n_rounds + 1)]
        ## self.xs = xs <- store? or just return

        has_dnf = board.where(board < 0, 0).sum().sum() < 0
        
        lowest_rank = int(board.where(board > 0, 0).max().max())
        highest_dnf = int(board.where(board < 0, 0).min().min())

        for player in board.index:
            self.plot_board_player(ax, xs, player, board, lowest_rank)

        round_titles = [self.texter.clean_text(c) for c in board.columns]
        
        x_min = min(xs)
        x_max = max(xs)

        if has_dnf:
            # plot DNF line
            ax.plot([x_min - 0.5, x_max + 0.5], [lowest_rank + 1] * 2, '--', color='0.5')

        ax.set_xlim(x_min - 0.5, x_max + 0.5)
        ax.set_xticks(xs)
        ax.set_xticklabels(round_titles, rotation=45)

        y_min = lowest_rank - highest_dnf + has_dnf
        y_max = 0
        yticks = range(y_min, y_max, -1)

        ax.set_ylim(y_min + 0.5, y_max + 0.5)
        ax.set_yticks(yticks)
        ax.set_yticklabels([int(y) if y <= lowest_rank else 'DNF' if y == lowest_rank + 2 else '' for y in yticks])

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
                image, imgs = self.plot_image(ax, x, y, player_name=player, size=self.ranking_size, flipped=True, zorder=100)#, aspect=aspect)
                if not image:
                    ax.text(x, y, display_name)

            if d > lowest_rank + 1:
                # plot DNFs
                image, imgs = self.plot_image(ax, x, d, player_name=player, size=self.ranking_size, flipped=True, zorder=100)#, aspect=aspect)
                if not image:
                    ax.text(x, d, display_name)

    def plot_rankings(self, ax, rankings, dirty_df, discovery_df):
        print('\t...scores')
        rankings_df = rankings.reset_index().pivot(index='player', columns='round', values='score').div(100)\
            .reindex(columns=rankings.index.get_level_values(0).drop_duplicates()).sort_index(ascending=False)

        player_names = rankings_df.index
        n_rounds = len(rankings_df.columns)

        max_score = rankings_df.max().max()
        rgb_df = self.grade_colors([self.dfc_red, self.dfc_yellow, self.dfc_green, self.dfc_blue])
        
        max_dirty = max(0.5, dirty_df.max())
        rgb_dirty_df = self.grade_colors([self.dfc_purple, self.dfc_peach])

        max_discovery = 1
        rgb_discovery_df = self.grade_colors([self.dfc_grey, self.dfc_dark_blue])

        xs = range(n_rounds)
        for player in player_names:
            y = player_names.get_loc(player)
            marker_size = 0.9
            image_size = self.plot_player_scores(ax, player, xs, y, rankings_df, max_score, rgb_df, marker_size)

            # plot dirtiness
            self.plot_player_score(ax, len(xs), y, dirty_df[player], max_dirty, rgb_dirty_df,
                                   marker_size, image_size, percent=True)

            # plot discovery
            self.plot_player_score(ax, len(xs)+1, y, discovery_df[player], max_discovery, rgb_discovery_df,
                                   marker_size, image_size, percent=True)
            
        ax.axis('equal')
        ax.tick_params(axis='both', which='both',
                       bottom='off', top='off', left='off', right='off') # get rid of ticks?

    def plot_player_scores(self, ax, player, xs, y, rankings_df, max_score, rgb_df, marker_size):
        ys = [y] * len(xs)
        
        scores = rankings_df.loc[player]
        colors = [self.get_rgb(rgb_df, score/max_score, fail_color=self.dfc_grey) \
            for score in scores]
        colors_scatter = self.get_scatter_colors(colors, divisor=self.color_wheel)

        image, imgs = self.plot_image(ax, -1, y, player_name=player, size=marker_size)
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
            
    def plot_features(self, ax, features_df):
        features_solo = ['duration', 'tempo']
        features_like = ['danceability', 'energy', 'liveness', 'valence',
                         'speechiness', 'acousticness', 'instrumentalness']
        features_mapping = {'danceability': '💃',
                            'energy': '⚡',
                            'liveness': '🏟',
                            'valence': '💖',
                            'speechiness': '💬',
                            'acousticness': '🎸',
                            'instrumentalness': '🎹',
                            }
        features_like_colors = self.get_scatter_colors([self.dfc_red, self.dfc_blue, self.dfc_purple, self.dfc_yellow,
                                                        self.dfc_dark_blue, self.dfc_orange, self.dfc_aqua],
                                                       divisor=self.color_wheel)

        n_rounds = len(features_df)
        
        # ['loudness', 'key', 'mode']
        features_all = features_solo + features_like
        mapper = {f'avg_{f}': f'{f}' for f in features_all}
        
        features_df = features_df.set_index('round').rename(columns=mapper)[features_all]
        features_df[features_solo] = features_df[features_solo].abs().div(features_df[features_solo].max())
        
        features_df.plot(use_index=True, y=features_like, color=features_like_colors,
                         kind='bar', legend=False, rot=45, ax=ax)

        for c in range(len(features_like)):
            #c = ax.containers.index(container)
            ax.bar_label(ax.containers[c], color=features_like_colors[c], labels=[features_mapping[features_like[c]]]*n_rounds,
                         font=self.emoji_font, horizontalalignment='center')

        features_df.plot(y='tempo', secondary_y='tempo',
                         kind='line', legend=False, ax=ax)

    def plot_genres(self, ax, genres_df):
        genres_df.groupby('genre').sum().plot(y='representation', kind='pie', legend=False, labeldistance=None,
                                              ax=ax)
        
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

    def get_colors(self, members_df):
        max_dfc = members_df['dfc'].max()
        min_dfc = members_df['dfc'].min()

        rgb_df = self.grade_colors([self.dfc_green, self.dfc_blue])
        colors = [self.get_rgb(rgb_df, 1-(dfc - min_dfc)/(max_dfc - min_dfc),
                               fail_color=self.dfc_grey) for dfc in members_df['dfc'].values]
        
        return colors

    def get_scatter_colors(self, colors_rgb, divisor=1):
        colors = [self.normalize_color(rgb, divisor) for rgb in colors_rgb]
        return colors

    def normalize_color(self, color, divisor):
        color = tuple(c / divisor for c in color)
        return color