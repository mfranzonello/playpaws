from pandas import isnull
from pandas.core.dtypes import missing

from common.words import Texter

class Library:
    # one-to-one emoji
    emoji = {'dirtiest': '🙊',
             'clean': '🧼',
             'discoverer': '🔎',
             'popular': '🕶',
             'generous': '🗳',
             'hoarder': '🦍',
             'participant': '🤗',
             'likes': '💞',
             'liked': '💕',
             'closest': '👯',
             'win_rate': '🔥',
             'play_rate': '⚾',
             'wins': '🏅',
             'competitions': '🥊',
             'viewer': '👋',
             'creator': '🤓',
             'leader': '🥇',
             'closest_dfc': '🎯',
             'tbd': '❔',
             'winner': '🏆',
             
             'tag': '🗨️',
             'tag_ex': '💬',
             'release_date': '📅',
             'release_age': '🎂',
             'release_old': '⌛',
             'track_count': '🎶',
             'track_duration': '🕓',

             'tempo': '🥁',
             'danceability': '💃',
             'energy': '⚡',
             'liveness': '🏟',
             'valence': '💖',
             'speechiness': '🗣',
             'acousticness': '🎸',
             'instrumentalness': '🎹',
             'duration': '⏲',
             
             'round_title': '🎧',
             }
                  
    # one-to-many emoji
    emojis = {'🐾': ['paws', 'dog', 'animal'],
              '💿': ['mixtape', 'CD'],
              '🧭': ['compass'],
              '👬': ['brothers', 'men'],
              }

    def __init__(self):
        self.texter = Texter()

    def get_and_set_fonts(self, font_manager, rcParams, dir_path, plot_color):
        sans_fonts = list(self.texter.sans_fonts.keys())
        sans_font = list(self.texter.sans_fonts.values())[0]
        bold_font = list(self.texter.bold_fonts.values())[0]
        emoji_font = list(self.texter.emoji_fonts.values())[0]

        image_sans_font = f'fonts/{sans_font}'
        image_bold_font = f'fonts/{bold_font}'

        # set plotting fonts
        font_dir = f'{dir_path}/fonts'
        font_files = font_manager.findSystemFonts(fontpaths=[font_dir])

        for font_file in font_files:
            font_manager.fontManager.addfont(font_file)

        plot_sans_font = font_manager.get_font(f'{font_dir}/{sans_font}').family_name
        plot_emoji_font = font_manager.get_font(f'{font_dir}/{emoji_font}').family_name

        rcParams['font.family'] = 'sans-serif'
        rcParams['font.sans-serif'] = sans_fonts

        rcParams['text.color'] = plot_color
        rcParams['axes.labelcolor'] = plot_color
        rcParams['xtick.color'] = plot_color
        rcParams['ytick.color'] = plot_color

        fonts = {'image_sans': image_sans_font,
                 'image_bold': image_bold_font,
                 'plot_sans': plot_sans_font,
                 'plot_emoji': plot_emoji_font}

        return fonts  

    def newline(self, num=2):
        return '  \n'*num

    def indent(self, num=10):
        return '&nbsp;'*num

    def bar(self):
        return f'{self.newline(1)}---{self.newline(1)}'

    def get_tooltip(self, plot_name, parameters={}):
        text = None

        # pick expander label
        if plot_name == 'welcome':
            label = '<< Open the sidebar'

        elif plot_name == 'title':
            label = 'Explain the rules.'

        elif plot_name == 'top_songs_round':
            label = 'Read the description'

        else:
            label = 'What am I looking at?'

        # build expander contents
        if plot_name == 'welcome':
            text = ('This is a visual analysis tool for MusicLeague.app. Open the left '
                    'sidebar to pick a league to view.')

        elif plot_name == 'title':
            title = self.feel_title(parameters.get('title'), markdown='**')
            viewer = parameters.get('viewer')
            creator = parameters.get('creator')
            if creator == viewer:
                creator = 'YOU'
            text = (f'Welcome to the MöbiMusic league analyzer, {self.feel("viewer")}**{viewer}**{self.feel("viewer")}! These are the nerb '
                    f'results of all the current rounds for the {title} league, '
                    f'created by {self.feel("creator")}**{creator}**{self.feel("creator")}. Keep scrolling to see how players have '
                    f'placed and what it all sounds like.'
                    )

        elif plot_name == 'members':
            leaders = self.texter.get_plurals(parameters.get('leader'), markdown=f'{self.feel("leader")}**')
            closest_dfc = self.texter.get_plurals(parameters.get('closest_dfc'), markdown=f'{self.feel("closest_dfc")}**')                         
            text = (f'This shows the relationships between the league players. '
                    f'Players with similar music tastes are closer together. The '
                    f'arrows indicate who likes whom the most (a pink arrow shows '
                    f'whom a player gives most of their votes to, and the orange '
                    f'arrow shows whom they get the most votes from).'
                    f'{self.newline()}'
                    f'The size of a player node is relative to how well they\'ve '
                    f'performed in the league, the bigger the better. The color around '
                    f'a player indicates how close they are to the center of the '
                    f'league\'s music taste (a blue circle is the closest to the center '
                    f'while a green circle is farthest).'
                    f'{self.newline()}'
                    f'Currently, the player{leaders.get("s")} with the highest rank '
                    f'{leaders.get("be")} {leaders.get("text")} and the '
                    f'player{closest_dfc.get("s")} closest to the center {closest_dfc.get("be")} '
                    f'{closest_dfc.get("text")}.'
                    )
        
        elif plot_name == 'boards':
            round_titles = parameters.get('round_titles')
            choosers = parameters.get('choosers')
            winners = parameters.get('winners')
            placements = f'{self.newline(num=1)}'.join(f'{round_title} (chosen by {chooser}):'
                                                       f'{self.newline(num=1)}{self.indent(20)}'
                                                       f'{self.feel("tbd") if isnull(winner) else self.feel("winner")}'
                                                       f'**{"TBD" if isnull(winner) else winner}**'
                                                       f'{self.feel("tbd") if isnull(winner) else self.feel("winner")}' \
                for round_title, chooser, winner in zip(round_titles, choosers, winners))
            text = (f'This chart shows how players finished in each round. '
                    f'{self.newline()}{placements}')

        elif plot_name == 'rankings':
            dirty = self.texter.get_plurals(parameters.get('dirty'), markdown=f'self.feel("dirtiest")**')
            discovery = self.texter.get_plurals(parameters.get('discovery'), markdown=f'self.feel("discoverer")**')
            popular = self.texter.get_plurals(parameters.get('popular'), markdown=f'self.feel("popular")**')
            text = (f'This shows each player in alphabetical order '
                    f'and how the scored in each round. A higher number '
                    f'indicates a better score, that is, this player got '
                    f'more points than one with a lower score. "DNF" means '
                    f'this player didn\'t submit a song or didn\'t vote.'
                    f'{self.newline()}'
                    f'"Dirtiness" is a percentage of how many songs a player '
                    f'submitted are explicit. The dirtiest player{dirty.get("s")} '
                    f'{dirty.get("be")} {dirty.get("text")}.'
                    f'{self.newline()}'
                    f'"Discovery" and "Popularity" have to do with how well '
                    f'known a song is and who is listening to it. Songs with '
                    f'a smaller listening base score high on the discovery '
                    f'dimension, and a player with a higher score is '
                    f'introducing undiscovered songs to the group. The best '
                    f'discoverer{discovery.get("s")} {discovery.get("be")} {discovery.get("text")}. '
                    f'Meanwhile, players with higher popularity scores are sharing songs '
                    f'that listeners are playing more often. The player{popular.get("s")} '
                    f'sharing the most popular tracks {popular.get("be")} {popular.get("text")}.'                    
                    )

        elif plot_name == 'features':
            text = (f'This shows the evolution of how each round sounds.{self.newline()}'
                    f'{self.indent()}{self.feel("tempo")}: Tempo (beats per minute){self.newline(num=1)}'
                    f'{self.indent()}{self.feel("danceability")}: Danceability (makes you move){self.newline(num=1)}'
                    f'{self.indent()}{self.feel("energy")}: Energy (NRG){self.newline(num=1)}'
                    f'{self.indent()}{self.feel("liveness")}: Liveness (sounds from a stadium){self.newline(num=1)}'
                    f'{self.indent()}{self.feel("valence")}: Valence (level of happiness){self.newline(num=1)}'
                    f'{self.indent()}{self.feel("speechiness")}: Speechiness (more spoken word){self.newline(num=1)}'
                    f'{self.indent()}{self.feel("acousticness")}: Acousticness (less production){self.newline(num=1)}'
                    f'{self.indent()}{self.feel("instrumentalness")}: Instrumentalness (more instruments){self.newline(num=1)}'
                    )

        elif plot_name == 'tags':
            tag_count = len(parameters.get('top_tags'))
            top_tags = self.texter.get_plurals(parameters.get('top_tags'), markdown='**')
            exclusives = parameters.get('exclusives')
            if exclusives:
                tag_ex = self.texter.get_plurals(exclusives, markdown='**')
                tag_ex_a = '' if len(tag_ex['s']) else 'a '
                tag_ex_like = 'like ' if len(tag_ex['s']) else ''
                add_on = (f', but this league also uses {tag_ex_a} unique '
                          f'tag{tag_ex["s"]} {tag_ex_like}{self.feel("tag_ex")}{tag_ex["text"]}{self.feel("tag_ex")}'
                          )
            else:
                add_on = ''
            
            text = (f'This is a word cloud of the most popular descriptors of '
                    f'songs submitted in this league, from Spotify genres '
                    f'and Last.FM tags. You can see the top '
                    f'{tag_count if tag_count > 1 else ""} tag{top_tags.get("s")} {top_tags.get("be")} '
                    f'{self.feel("tag")}{top_tags.get("text")}{self.feel("tag")}{add_on}. Your tags are '
                    f'in <font color="{parameters["highlight"]}">blue</font>.'
                    )

        elif plot_name == 'top_songs':
            max_year = parameters.get('max_year')
            min_year = parameters.get('min_year')
            oldest_year = parameters.get('oldest_year')
            average_age = parameters.get('average_age')
            text = (f'This is a mapping of how songs were ranked for each round. '
                    f'A song near the top, and in large font, received more '
                    f'points than songs below it. A song in bold means it closed-out '
                    f'the panel (every player voted for it).'
                    f'{self.newline()}'
                    f'The horizontal placement of the song indicates it\'s '
                    f'release date. Songs to the left are recent releases and '
                    f'songs to the right are older.'
                    f'{self.newline()}'
                    f'Most songs were released between {self.feel("release_date")}**{min_year}** '
                    f'and **{max_year}**{self.feel("release_date")}. '
                    f'The average age of a #1 song is {self.feel("release_age")}**{average_age} '
                    f'year{"" if average_age == 1 else "s"}**{self.feel("release_age")}. The oldest '
                    f'song was released in {self.feel("release_old")}**{oldest_year}**{self.feel("release_old")}.'
                    )

        elif plot_name == 'top_songs_round':
            text = f'>_{parameters.get("description")}_'

        elif plot_name == 'playlist':
            count = parameters.get('count')
            duration = self.texter.get_times(parameters.get('duration'), markdown='**')
            text = (f'This is a collection of all the tracks ever submitted '
                    f'in this league, all {self.feel("track_count")}**{count}**{self.feel("track_count")} of them, and it would '
                    f'take you {self.feel("track_duration")}{duration}{self.feel("track_duration")} to listen to the whole thing!')

        elif plot_name == 'hoarding':
            text = (f'This shows how people spread their votes across rounds. Out of the available song '
                    f'selection, some people concentrate on assigning points to a few favorites, '
                    f'whereas others give many songs a few points each.'
                    f'{self.newline()}'
                    f'In this group, {self.feel("generous")}**{parameters.get("generous")}**{self.feel("generous")} '
                    f'is the most generous with spreading votes to all players, '
                    f'while {self.feel("hoarder")}**{parameters.get("hoarder")}**{self.feel("hoarder")} is the '
                    f'one who goes hardest on key tracks.'
                    )

        if text:
            tooltip = {'label': label,
                       'content': text}

        else:
            tooltip = None

        return tooltip

    def get_column(self, parameters={}):
        leagues_list = []
        league_titles = parameters.get('leagues')
        if len(league_titles):
            league_titles = [self.feel_title(t, markdown='**') for t in league_titles]
            leagues_in = self.texter.get_plurals(league_titles)['text']
            other_leagues = 'Other ' if parameters.get('other_leagues') else ''
            leagues_list.append(f'{other_leagues}Leagues Played In: {leagues_in}')
        leagues = self.bar_list(leagues_list, indent=False, bar=False)

        competitions_list = []
        if parameters.get('current_competition'):
            place = self.texter.get_ordinal(parameters['badge2'])
            total = parameters['n_players']
            competitions_list.append(f'Currently competing in {self.feel("competitions")}'
                                     f'{parameters["current_competition"]}{self.feel("competitions")}'
                                     f'{self.newline(1)}{self.indent()}(ranked {place} of {total})')
        competitions = self.bar_list(competitions_list, indent=False)

        awards_list = []
        if parameters.get('dirtiest'):
            awards_list.append(f'{self.feel("dirtiest")}**Most Explicit Player**{self.feel("dirtiest")}')
        if parameters.get('clean'):
            awards_list.append(f'{self.feel("clean")}**Squeaky Clean Lyrics**{self.feel("clean")}')
        if parameters.get('discoverer'):
            awards_list.append(f'{self.feel("discoverer")}**Best Music Discoverer**{self.feel("discoverer")}')
        if parameters.get('popular'):
            awards_list.append(f'{self.feel("popular")}**Most Hep Tracks**{self.feel("popular")}')
        if parameters.get('generous'):
            awards_list.append(f'{self.feel("generous")}**Equal Opportunity Voter**{self.feel("generous")}')
        # stingy, maxed_out
        if not len(awards_list):
            awards_list.append(f'{self.feel("participant")}**Participation Trophy**{self.feel("participant")}')
        awards = self.bar_list(awards_list)

        pulse_list = []
        if parameters.get('likes'):
            pulse_list.append(f'Likes best: {self.feel("likes")}**{parameters["likes"]}**{self.feel("likes")}')
        if parameters.get('liked'):
            pulse_list.append(f'Most liked by: {self.feel("liked")}**{parameters["liked"]}**{self.feel("liked")}')
        if parameters.get('closest'):
            pulse_list.append(f'Most similar to: {self.feel("closest")}**{parameters["closest"]}**{self.feel("closest")}')
        pulse = self.bar_list(pulse_list)

        stats_list = []
        if parameters.get('win_rate'):
            stats_list.append(f'Batting Average: {self.feel("win_rate")}**{parameters["win_rate"]:.3f}**{self.feel("win_rate")}')
        if parameters.get('play_rate'):
            stats_list.append(f'Games Played: {self.feel("play_rate")}**{parameters["play_rate"]:.3f}**{self.feel("play_rate")}')
        stats = self.bar_list(stats_list)

        wins_list = []
        if parameters.get('wins'):
            round_titles = [self.texter.clean_text(t) for t in parameters['wins']]
            rounds_won = self.texter.get_plurals(round_titles, markdown='**')
            wins_list.append(f'Round{rounds_won["s"]} won: {self.feel("wins")}{rounds_won["text"]}{self.feel("wins")}')
        if parameters.get('competition_wins'):
            competition_titles = [self.texter.clean_text(t) for t in parameters['competition_wins']]
            competitions_won = self.texter.get_plurals(competition_titles, markdown='**')
            wins_list.append(f'Competition{competitions_won["s"]} won: {self.feel("competitions")}{competitions_won["text"]}{self.feel("competitions")}')
        wins = self.bar_list(wins_list)

        text = (f'## Player Stats'
                f'{leagues}{competitions}{awards}{pulse}{stats}{wins}'
                )

        return text

    def bar_list(self, items_list, indent=True, bar=True):
        if len(items_list):
            indent_me = self.indent() if indent else ''
            bar_me = self.bar() if bar else self.newline(1)
            items = (f'{bar_me}' + 
                     ''.join(f'{indent_me}{x}{self.newline(1)}' for x in items_list) +
                     f'{self.newline(1)}'
                     )
        else:
            items = ''

        return items

    def feel_title(self, text, default=emoji['round_title'], markdown=''):
        clean_text = self.texter.clean_text(text)
        emoji = self.texter.match_emoji(text, self.emojis, default=default)
        title = f'{emoji}{markdown}{clean_text}{markdown}{emoji}'

        return title

    def feel(self, text, default=''):
        return self.emoji.get(text, default)