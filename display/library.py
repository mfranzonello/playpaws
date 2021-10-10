from pandas import isnull

from common.words import Texter, Feeler

class Librarian:
    def __init__(self):
        self.texter = Texter()
        self.feeler = Feeler()

    def get_tooltip(self, plot_name, parameters={}):
        text = None

        # pick expander label
        if plot_name in ['welcome', 'title']:
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
            emoji = self.feeler.match_emoji(title, default='🎧')
            creator = parameters.get('creator')
            text = (f'Welcome to the MöbiMusic league analyzer! These are the nerb '
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

    def newline(self, num=2):
        return '  \n'*num

    def indent(self, num=10):
        return '&nbsp;'*num
