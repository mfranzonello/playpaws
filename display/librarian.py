from pandas import isnull

from common.words import Texter

class Library:
    emojis = {'🐾': ['paws', 'dog', 'animal'],
              '💿': ['mixtape', 'CD'],
              '🧭': ['compass'],
              '👬': ['brothers', 'men'],
              }

    def __init__(self):
        self.texter = Texter()

    def get_and_set_fonts(self, font_manager, rcParams, dir_path):
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
            title = self.texter.clean_text(parameters.get('title'))
            emoji = self.texter.match_emoji(title, self.emojis, default='🎧')
            viewer = parameters.get('viewer')
            creator = parameters.get('creator')
            if creator == viewer:
                creator = 'YOU'
            text = (f'Welcome to the MöbiMusic league analyzer, 👋**{viewer}**👋! These are the nerb '
                    f'results of all the current rounds for the {emoji}**{title}**{emoji} league, '
                    f'created by 🤓**{creator}**🤓. Keep scrolling to see how players have '
                    f'placed and what it all sounds like.'
                    )

        elif plot_name == 'members':
            leaders = self.texter.get_plurals(parameters.get('leader'), markdown='🥇**')
            closest_dfc = self.texter.get_plurals(parameters.get('closest_dfc'), markdown='🎯**')                         
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
                                                       f'{"❔" if isnull(winner) else "🏆"}'
                                                       f'**{"TBD" if isnull(winner) else winner}**'
                                                       f'{"❔" if isnull(winner) else "🏆"}' \
                for round_title, chooser, winner in zip(round_titles, choosers, winners))
            text = (f'This chart shows how players finished in each round. '
                    f'{self.newline()}{placements}')

        elif plot_name == 'rankings':
            dirty = self.texter.get_plurals(parameters.get('dirty'), markdown='🙊**')
            discovery = self.texter.get_plurals(parameters.get('discovery'), markdown='🔎**')
            popular = self.texter.get_plurals(parameters.get('popular'), markdown='✨**')
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
                    f'{self.indent()}🥁: Tempo (beats per minute){self.newline(num=1)}'
                    f'{self.indent()}💃: Danceability (makes you move){self.newline(num=1)}'
                    f'{self.indent()}⚡: Energy (NRG){self.newline(num=1)}'
                    f'{self.indent()}🏟: Liveness (sounds from a stadium){self.newline(num=1)}'
                    f'{self.indent()}💖: Valence (level of happiness){self.newline(num=1)}'
                    f'{self.indent()}💬: Speechiness (more spoken word){self.newline(num=1)}'
                    f'{self.indent()}🎸: Acousticness (less production){self.newline(num=1)}'
                    f'{self.indent()}🎹: Instrumentalness (more instruments){self.newline(num=1)}'
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
                          f'tag{tag_ex["s"]} {tag_ex_like}💬{tag_ex["text"]}💬'
                          )
            else:
                add_on = ''
            
            text = (f'This is a word cloud of the most popular descriptors of '
                    f'songs submitted in this league, from Spotify genres '
                    f'and Last.FM tags. You can see the top '
                    f'{tag_count if tag_count > 1 else ""} tag{top_tags.get("s")} {top_tags.get("be")} '
                    f'🗨️{top_tags.get("text")}🗨️{add_on}.'
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
                    f'Most songs were released between 📅**{min_year}** and **{max_year}**📅. '
                    f'The average age of a #1 song is 🎂**{average_age} '
                    f'year{"" if average_age == 1 else "s"}**🎂. The oldest '
                    f'song was released in ⌛**{oldest_year}**⌛.'
                    )

        elif plot_name == 'top_songs_round':
            text = f'>_{parameters.get("description")}_'

        elif plot_name == 'playlist':
            count = parameters.get('count')
            duration = self.texter.get_times(parameters.get('duration'), markdown='**')
            text = (f'This is a collection of all the tracks ever submitted '
                    f'in this league, all 🎶**{count}**🎶 of them, and it would '
                    f'take you 🕓{duration}🕓 to listen to the whole thing!')

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
            emojis = [self.texter.match_emoji(t, self.emojis, default='🎧') for t in league_titles]
            league_titles = [f'{e}{t}{e}' for e, t in  zip(emojis, league_titles)]
            leagues_in = self.texter.get_plurals(league_titles, markdown='**')['text']
            other_leagues = 'Other ' if parameters.get('other_leagues') else ''
            leagues_list.append(f'{other_leagues}Leagues Played In: {leagues_in}')
        leagues = self.bar_list(leagues_list, indent=False)

        awards_list = []
        if parameters.get('dirtiest'):
            awards_list.append(f'🙊**Most Explicit Player**🙊')
        if parameters.get('discoverer'):
            awards_list.append(f'🔎**Best Music Discoverer**🔎')
        if parameters.get('popular'):
            awards_list.append(f'✨**Most Hep Tracks**✨')
        # hoarder, generous
        if not len(awards_list):
            awards_list.append(f'🤗**Participation Trophy**🤗')
        awards = self.bar_list(awards_list)

        pulse_list = []
        if parameters.get("likes"):
            pulse_list.append(f'Likes best: 💞**{parameters["likes"]}**💞')
        if parameters.get("liked"):
            pulse_list.append(f'Most liked by: 💕**{parameters["liked"]}**💕')
        if parameters.get("closest"):
            pulse_list.append(f'Most similar to: 👯**{parameters["closest"]}**👯')
        pulse = self.bar_list(pulse_list)

        stats_list = []
        if parameters.get('win_rate'):
            stats_list.append(f'Batting Average: ⚾**{parameters["win_rate"]:.3f}**⚾')
        if parameters.get('play_rate'):
            stats_list.append(f'Games Played: 🔥**{parameters["play_rate"]:.3f}**🔥')
        stats = self.bar_list(stats_list)

        wins_list = []
        if parameters.get('wins'):
            rounds_won = self.texter.get_plurals(parameters['wins'], markdown='**')
            wins_list.append(f'Round{rounds_won["s"]} won: 🏅{rounds_won["text"]}🏅')
        wins = self.bar_list(wins_list)

        text = (f'## Player Stats'
                f'{leagues}{awards}{pulse}{stats}{wins}'
                )

        return text

    def bar_list(self, items_list, indent=True):
        if len(items_list):
            indent_me = self.indent() if indent else ''
            items = (f'{self.bar()}' + 
                     ''.join(f'{indent_me}{x}{self.newline(1)}' for x in items_list) +
                     f'{self.newline(1)}'
                     )
        else:
            items = ''

        return items
