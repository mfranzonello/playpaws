from math import sin, cos, atan2, pi, isnan
from re import compile, UNICODE
from urllib.request import urlopen
from PIL import Image, ImageDraw, ImageOps, UnidentifiedImageError # <- is this built-in?

from pandas import set_option

import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Segoe UI', 'Tahoma']

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

    def clean_text(text:str) -> str:
        cleaned_text = Texter.emoji_pattern.sub(r'', text).strip()
        return cleaned_text

    def get_display_name(name:str) -> str:
        if ' ' in name:
            display_name = name[:name.index(' ')]
        elif '.' in name:
            display_name = name[:name.index('.')]
        else:
            display_name = name
        return display_name.title()

class Pictures:
    def __init__(self, players):
        self.players = players
        self.images = self.download_images()
        self.crop_player_images()

    def download_images(self):
        images = {}
        for i in self.players.df.index:
            player_name = self.players.df['player'][i]
            
            src = self.players.df['src'][i]
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

            images[player_name] = image

        return images

    def get_player_image(self, player_name):
        image = self.images[player_name]
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

class Plotter:
    dfc_blue = (44, 165, 235)
    dfc_green = (86, 225, 132)
    dfc_grey = (172, 172, 172)
    color_wheel = 255

    marker_sizing = 50

    name_offset = 0.1
    font_size = 0.2
    
    likes_color = 'pink'
    liked_color = 'orange'
    like_arrow_width = 0.02
    like_arrow_length = 0.4
    like_arrow_split = 0.05

    figure_size = (16, 10)
    figure_title = 'MusicLeague'

    subplot_aspect = ((1 + 5**0.5) / 2, 1)
    ranking_size = 0.75

    def __init__(self):
        self.league_titles = []
        self.members_list = []
        self.rankings_list = []
        self.pictures = None


    def translate(self, x:float, y:float, theta:float, rotate:float, shift_distance:float=0):
        x_shifted = x + shift_distance*cos(theta + rotate*pi/2)
        y_shifted = y + shift_distance*sin(theta + rotate*pi/2)
        return x_shifted, y_shifted

    def dfc_color(self, percent_blue:float):
        if isnan(percent_blue):
            rgb = tuple(g/self.color_wheel for g in self.dfc_grey)
        else:
            rgb = tuple((percent_blue*b + (1-percent_blue)*g)/self.color_wheel \
                for b,g in zip(self.dfc_blue, self.dfc_green))
            
        return rgb

    def add_anaylses(self, analyses, players):
        for analysis in analyses:
            self.add_league(analysis['league_title'], analysis['members'], analysis['rankings'])
        self.pictures = Pictures(players)

    def add_league(self, league_title, members, rankings):
        self.league_titles.append(league_title)
        self.members_list.append(members)
        self.rankings_list.append(rankings)

    def plot_results(self):
        # set league title
        nrows = 2
        ncols = len(self.members_list)
        fig, axs = plt.subplots(nrows, ncols)

        plt.get_current_fig_manager().set_window_title(self.figure_title)
        fig.set_size_inches(*self.figure_size)
            
        for ax0, members, league_title in zip(axs[0], self.members_list, self.league_titles):
            self.plot_players(ax0, members, league_title)

        for ax1, rankings in zip(axs[1], self.rankings_list):
            self.plot_rankings(ax1, rankings)

        plt.show()

    def plot_image(self, ax, player_name, x, y, size=0.5, aspect=(1, 1)):
        image = self.pictures.get_player_image(player_name)
        if image:
            scaling = [a / max(aspect) for a in aspect]
            extent = [x + size/2 * scaling[0], x - size/2 * scaling[0],
                      y + size/2 * scaling[1], y - size/2 * scaling[1]]
            ax.imshow(image, extent=extent)
            success = True
        else:
            success = False

        return success

    def plot_players(self, ax, members, league_title):
        # set subplot title
        ax.set_title(Texter.clean_text(league_title))

        # plot nodes for players
        x = members.df['x']
        y = members.df['y']
        sizes = self.get_scatter_sizes(members)
        colors = self.get_scatter_colors(members)

        ax.scatter(x, y, s=sizes, c=colors)

        # split if likes is liked
        split = members.df.set_index('player')
        split = split['likes'] == split['liked']

        for i in members.df.index:
            # plot center
            x_center, y_center = self.get_center(members)
            ax.scatter(x_center, y_center, marker='1')

            # get name
            me = members.df['player'][i]
            x_me, y_me, theta_me = self.where_am_i(members, me)

            # plot names
            x_1, y_1 = self.translate(x_me, y_me, theta_me, 0, shift_distance=-self.name_offset)

            h_align = 'right' if (theta_me > -pi/2) & (theta_me < pi/2) else 'left'
            v_align = 'top' if (theta_me > 0) else 'bottom'
            display_name = Texter.get_display_name(me)
            
            ax.text(x_1, y_1, display_name, horizontalalignment=h_align, verticalalignment=v_align)

            # split if liked
            split_distance = split[me] * self.like_arrow_split

            # find likes
            x_likes, y_likes, theta_us = self.who_likes_whom(members, me, 'likes', self.like_arrow_length)
            x_1, y_1 = self.translate(x_me, y_me, theta_us, -1, shift_distance=split_distance)
            x_2, y_2 = self.translate(x_likes, y_likes, theta_us, -1, shift_distance=split_distance)
            ax.arrow(x_1, y_1, x_2-x_1, y_2-y_1,
                        width=self.like_arrow_width, facecolor=self.likes_color,
                        edgecolor='none', length_includes_head=True)

            # find liked
            x_liked, y_liked, theta_us = self.who_likes_whom(members, me, 'liked', line_dist=self.like_arrow_length)
            x_1, y_1 = self.translate(x_me, y_me, theta_us, 1, shift_distance=split_distance)
            x_2, y_2 = self.translate(x_liked, y_liked, theta_us, 1, shift_distance=split_distance)
            ax.arrow(x_2, y_2, x_1-x_2, y_1-y_2,
                        width=self.like_arrow_width, facecolor=self.liked_color,
                        edgecolor='none', length_includes_head=True)

        ax.axis('equal')
        ax.set_ylim(members.df['y'].min() - self.name_offset - self.font_size,
                    members.df['y'].max() + self.name_offset + self.font_size)
        ax.axis('off')

    def plot_rankings(self, ax, rankings):
        leaderboard, dnf = rankings.get_leaderboard()

        n_rounds = len(leaderboard.columns)
        n_players = len(leaderboard.index)
        aspect = (n_rounds - 1, n_players - 1) # (+ 1, + 1)?
        scaling = [a / b * aspect[1] for a, b in zip(self.subplot_aspect, aspect)]

        xs = [x * scaling[0] for x in range(1, n_rounds + 1)]

        has_dnf = dnf.notna().sum().sum() > 0
        
        lowest_rank = int(leaderboard.max().max())
        highest_dnf = int(dnf.fillna(0).min().min())

        if has_dnf:
            ax.plot([1, max(xs)], [lowest_rank + 1] * 2, '--', color='0.5')

        for player in leaderboard.index:
            ys = leaderboard.loc[player]
            ds = [lowest_rank - d + 1 for d in dnf.loc[player]]
            display_name = Texter.get_display_name(player)

            color = f'C{leaderboard.index.get_loc(player)}'
            ax.plot(xs, ys, marker='.', color=color)
            ax.scatter(xs, ds, marker='.', color=color)

            for x, y, d in zip(xs, ys, ds):
                if y > 0:
                    # plot finishers
                    image_plotted = self.plot_image(ax, player, x, y, size=self.ranking_size)#, aspect=aspect)
                    if not image_plotted:
                        ax.text(x, y, display_name)

                if d > lowest_rank + 1:
                    # plot DNFs
                    image_plotted = self.plot_image(ax, player, x, d, size=self.ranking_size)#, aspect=aspect)
                    if not image_plotted:
                        ax.text(x, d, display_name)

        round_titles = [Texter.clean_text(c) for c in leaderboard.columns]
        
        x_min = min(xs)
        x_max = max(xs)

        ax.set_xlim(x_min - 0.5, x_max + 0.5)
        ax.set_xticks(xs)
        ax.set_xticklabels(round_titles, rotation=45)

        y_min = lowest_rank - highest_dnf + has_dnf
        y_max = 0
        yticks = range(y_min, y_max, -1)

        ax.set_ylim(y_min + 0.5, y_max + 0.5)
        ax.set_yticks(yticks)
        ax.set_yticklabels([int(y) if y <= lowest_rank else 'DNF' if y == lowest_rank + 2 else '' for y in yticks])

    def get_center(self, members):
        x_center = members.df['x'].mean()
        y_center = members.df['y'].mean()
        
        return x_center, y_center

    def where_am_i(self, members, player_name):
        x_me = members.df['x'][members.df['player'] == player_name].values[0]
        y_me = members.df['y'][members.df['player'] == player_name].values[0]
        x_center, y_center = self.get_center(members)
        theta_me = atan2(y_center - y_me, x_center - x_me)

        return x_me, y_me, theta_me

    def who_likes_whom(self, members, player_name, like, line_dist):
        likes_me = members.df[like][members.df['player'] == player_name].values[0]

        x_me, y_me, _ = self.where_am_i(members, player_name)
        
        x_them = members.df['x'][members.df['player'] == likes_me].values[0]
        y_them = members.df['y'][members.df['player'] == likes_me].values[0]

        theta_us = atan2(y_them - y_me, x_them - x_me)

        x_likes = x_me + line_dist * cos(theta_us)
        y_likes = y_me + line_dist * sin(theta_us)

        return x_likes, y_likes, theta_us

    def get_scatter_sizes(self, members):
        sizes = (members.df['wins'] + 1) * self.marker_sizing
        return sizes

    def get_scatter_colors(self, members):
        max_dfc = members.df['dfc'].max()
        min_dfc = members.df['dfc'].min()
        colors = [self.dfc_color(1-(dfc - min_dfc)/(max_dfc - min_dfc)) for dfc in members.df['dfc'].values]
        return colors