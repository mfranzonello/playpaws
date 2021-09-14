from math import sin, cos, atan2, pi, isnan
from re import compile, UNICODE

from pandas import set_option

import matplotlib.pyplot as plt
from matplotlib import rcParams

rcParams['font.family'] = 'sans-serif'
rcParams['font.sans-serif'] = ['Segoe UI', 'Tahoma']

class Printer:
    def __init__(self, *options):
        self.options = [*options]

    def set_display_options():
        for option in options:
            set_option(options, None)

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

    def translate(x:float, y:float, theta:float, rotate:float, shift_distance:float=0):
        x_shifted = x + shift_distance*cos(theta + rotate*pi/2)
        y_shifted = y + shift_distance*sin(theta + rotate*pi/2)
        return x_shifted, y_shifted

    def dfc_color(percent_blue:float):
        if isnan(percent_blue):
            rgb = tuple(g/Plotter.color_wheel for g in Plotter.dfc_grey)
        else:
            rgb = tuple((percent_blue*b + (1-percent_blue)*g)/Plotter.color_wheel \
                for b,g in zip(Plotter.dfc_blue, Plotter.dfc_green))
            
        return rgb

    def __init__(self):
        self.league_titles = []
        self.players_list = []
        self.rankings_list = []

    def add_anaylses(self, anaylses):
        for analysis in analyses:
            self.add_league(analysis['league_title'], analysis['players'], analysis['rankings'])

    def add_league(self, league_title, players, rankings):
        self.league_titles.append(league_title)
        self.players_list.append(players)
        self.rankings_list.append(rankings)

    def plot_results(self):
        # set league title
        nrows = 2
        ncols = len(self.players_list)
        fig, axs = plt.subplots(nrows, ncols)

        plt.get_current_fig_manager().set_window_title(Plotter.figure_title)
        fig.set_size_inches(*Plotter.figure_size)
            
        for ax0, players, league_title in zip(axs[0], self.players_list, self.league_titles):
            Plotter.plot_players(ax0, players, league_title)

        for ax1, rankings in zip(axs[1], self.rankings_list):
            Plotter.plot_rankings(ax1, rankings)

        plt.show()

    def plot_players(ax, players, league_title):
        # set subplot title
        ax.set_title(Texter.clean_text(league_title))

        # plot nodes for players
        x = players.df['x']
        y = players.df['y']
        sizes = Plotter.get_scatter_sizes(players)
        colors = Plotter.get_scatter_colors(players)

        ax.scatter(x, y, s=sizes, c=colors)

        # split if likes is liked
        split = players.df.set_index('player')
        split = split['likes'] == split['liked']

        for i in players.df.index:
            # plot center
            x_center, y_center = Plotter.get_center(players)
            ax.scatter(x_center, y_center, marker='1')

            # get name
            me = players.df['player'][i]
            x_me, y_me, theta_me = Plotter.where_am_i(players, me)

            # plot names
            x_1, y_1 = Plotter.translate(x_me, y_me, theta_me, 0, shift_distance=-Plotter.name_offset)

            h_align = 'right' if (theta_me > -pi/2) & (theta_me < pi/2) else 'left'
            v_align = 'top' if (theta_me > 0) else 'bottom'
            display_name = Texter.get_display_name(me)
            
            ax.text(x_1, y_1, display_name, horizontalalignment=h_align, verticalalignment=v_align)

            # split if liked
            split_distance = split[me] * Plotter.like_arrow_split

            # find likes
            x_likes, y_likes, theta_us = Plotter.who_likes_whom(players, me, 'likes', Plotter.like_arrow_length)
            x_1, y_1 = Plotter.translate(x_me, y_me, theta_us, -1, shift_distance=split_distance)
            x_2, y_2 = Plotter.translate(x_likes, y_likes, theta_us, -1, shift_distance=split_distance)
            ax.arrow(x_1, y_1, x_2-x_1, y_2-y_1,
                        width=Plotter.like_arrow_width, facecolor=Plotter.likes_color,
                        edgecolor='none', length_includes_head=True)

            # find liked
            x_liked, y_liked, theta_us = Plotter.who_likes_whom(players, me, 'liked', line_dist=Plotter.like_arrow_length)
            x_1, y_1 = Plotter.translate(x_me, y_me, theta_us, 1, shift_distance=split_distance)
            x_2, y_2 = Plotter.translate(x_liked, y_liked, theta_us, 1, shift_distance=split_distance)
            ax.arrow(x_2, y_2, x_1-x_2, y_1-y_2,
                        width=Plotter.like_arrow_width, facecolor=Plotter.liked_color,
                        edgecolor='none', length_includes_head=True)

        ax.axis('equal')
        ax.set_ylim(players.df['y'].min() - Plotter.name_offset - Plotter.font_size,
                    players.df['y'].max() + Plotter.name_offset + Plotter.font_size)
        ax.axis('off')

    def plot_rankings(ax, rankings):
        leaderboard, dnf = rankings.get_leaderboard()

        xs = range(1, len(leaderboard.columns) + 1)

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
                    ax.text(x, y, display_name)
                if d > lowest_rank + 1:
                    ax.text(x, d, display_name)

        round_titles = [Texter.clean_text(c) for c in leaderboard.columns]
        
        ax.set_xticks(xs)
        ax.set_xticklabels(round_titles, rotation=45)

        max_y = 0
        min_y = lowest_rank - highest_dnf + has_dnf
        yticks = range(min_y, max_y, -1)

        ax.set_ylim(min_y + 0.5, max_y + 0.5)
        ax.set_yticks(yticks)
        ax.set_yticklabels([int(y) if y <= lowest_rank else 'DNF' if y == lowest_rank + 2 else '' for y in yticks])

    def get_center(players):
        x_center = players.df['x'].mean()
        y_center = players.df['y'].mean()
        
        return x_center, y_center

    def where_am_i(players, player_name):
        x_me = players.df['x'][players.df['player'] == player_name].values[0]
        y_me = players.df['y'][players.df['player'] == player_name].values[0]
        x_center, y_center = Plotter.get_center(players)
        theta_me = atan2(y_center - y_me, x_center - x_me)

        return x_me, y_me, theta_me

    def who_likes_whom(players, player_name, like, line_dist):
        likes_me = players.df[like][players.df['player'] == player_name].values[0]

        x_me, y_me, _ = Plotter.where_am_i(players, player_name)
        
        x_them = players.df['x'][players.df['player'] == likes_me].values[0]
        y_them = players.df['y'][players.df['player'] == likes_me].values[0]

        theta_us = atan2(y_them - y_me, x_them - x_me)

        x_likes = x_me + line_dist * cos(theta_us)
        y_likes = y_me + line_dist * sin(theta_us)

        return x_likes, y_likes, theta_us

    def get_scatter_sizes(players):
        sizes = (players.df['wins'] + 1) * Plotter.marker_sizing
        return sizes

    def get_scatter_colors(players):
        max_dfc = players.df['dfc'].max()
        min_dfc = players.df['dfc'].min()
        colors = [Plotter.dfc_color(1-(dfc - min_dfc)/(max_dfc - min_dfc)) for dfc in players.df['dfc'].values]
        return colors