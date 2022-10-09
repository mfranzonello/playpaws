''' Generating captions and descriptions for data visuals '''

from common.words import Texter

class Library:
    def __init__(self, emoji={}, emojis={}):
        self.texter = Texter()
        self.emoji = emoji
        self.emojis = emojis

    def add_emoji(self, emoji, emojis):
        self.emoji.update(emoji)
        self.emojis.update(emojis)

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
            viewer = self.texter.get_display_name(parameters.get('viewer'))
            creator = self.texter.get_display_name(parameters.get('creator'))
            if creator == viewer:
                creator = 'YOU'
            text = (f'Welcome to the MöbiMusic league analyzer, {self.feel("viewer")}**{viewer}**{self.feel("viewer")}! These are the nerb '
                    f'results of all the current rounds for the {title} league, '
                    f'created by {self.feel("creator")}**{creator}**{self.feel("creator")}. Keep scrolling to see how players have '
                    f'placed and what it all sounds like.'
                    )

        elif plot_name == 'members':
            leaders = self.texter.get_plurals(parameters.get('leader'),
                                              markdown=f'{self.feel("leader")}**')
            closest_dfc = self.texter.get_plurals(parameters.get('closest_dfc'),
                                                  markdown=f'{self.feel("closest_dfc")}**')                         
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
            placements = f'{self.newline(num=1)}'.join(f'{self.texter.clean_text(round_title)} '
                                                       f'(chosen by {self.texter.get_display_name(chooser)}):'
                                                       f'{self.newline(num=1)}{self.indent(20)}'
                                                       f'{self.feel("tbd") if winner == [None] else self.feel("winner")}' #isnull
                                                       f'**{"TBD" if winner == [None] else " & ".join(winner)}**'
                                                       f'{self.feel("tbd") if winner == [None] else self.feel("winner")}' \
                for round_title, chooser, winner in zip(round_titles, choosers, winners))
            text = (f'This chart shows how players finished in each round. '
                    f'{self.newline()}{placements}')

        elif plot_name == 'rankings':
            dirty = self.texter.get_plurals(parameters.get('dirty'), markdown=f'{self.feel("dirtiest")}**')
            discovery = self.texter.get_plurals(parameters.get('discovery'), markdown=f'{self.feel("discoverer")}**')
            popular = self.texter.get_plurals(parameters.get('popular'), markdown=f'{self.feel("popular")}**')
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
                    f'discoverer{discovery.get("s")} {discovery.get("be")} '
                    f'{discovery.get("text")}. '
                    f'Meanwhile, players with higher popularity scores are sharing songs '
                    f'that listeners are playing more often. The player{popular.get("s")} '
                    f'sharing the most popular tracks {popular.get("be")} '
                    f'{popular.get("text")}.'                    
                    )

        elif plot_name == 'features':
            text = (f'This shows the evolution of how each round sounds.{self.newline()}'
                    f'{self.indent()}{self.feel("tempo")}: Tempo (beats per minute){self.newline(num=1)}' # note that this indent is only working for the first
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
            text = f'>_{parameters["description"].strip()}_' if parameters.get('description') else ''

        elif plot_name == 'playlist':
            count = parameters.get('count')
            duration = self.texter.get_times(parameters.get('duration'), markdown='**')
            text = (f'This is a collection of all the tracks ever submitted '
                    f'in this league, all {self.feel("track_count")}**{count}**{self.feel("track_count")} of them, and it would '
                    f'take you {self.feel("track_duration")}{duration}{self.feel("track_duration")} to listen to the whole thing!')

        elif plot_name == 'hoarding':
            generous = self.texter.get_plurals(parameters.get('generous'), markdown='**')
            hoarder = self.texter.get_plurals(parameters.get('hoarder'), markdown='**', plural_case=['es', ''])
            text = (f'This shows how people spread their votes across rounds. Out of the available song '
                    f'selection, some people concentrate on assigning points to a few favorites, '
                    f'whereas others give many songs a few points each.'
                    f'{self.newline()}'
                    f'In this group, {self.feel("generous")}**{generous.get("text")}**'
                    f'{self.feel("generous")} {generous.get("be")} the most generous with spreading votes '
                    f'to all players, while {self.feel("hoarder")}**{hoarder.get("text")}**'
                    f'{self.feel("hoarder")} go{hoarder.get("s")} hardest on key tracks.'
                    )

        elif plot_name == 'pie':
            text = (f'THIS IS A WORK IN PROGRESS')

        if text:
            tooltip = {'label': label,
                       'content': text}

        else:
            tooltip = None

        return tooltip

    def get_column(self, parameters={}, subsection=None):
        leagues, competitions, awards, pulse, stats, wins = [''] * 6

        award_titles = [{'item': 'dirtitest', 'title': 'Most Explicit Player'},
                {'item': 'clean', 'title': 'Squeaky Clean Lyrics'},
                {'item': 'discoverer', 'title': 'Best Music Discoverer'},
                {'item': 'popular', 'title': 'Most Hep Tracks'},
                {'item': 'generous', 'title': 'Equal Opportunity Voter'},
                {'item': 'stingy', 'title': 'Doesn\'t Like to Share'},
                # {'item': 'maxed_out', 'title': '', 'feel': ''},
                {'item': 'chatty', 'title': 'Chatty Cathy'},
                {'item': 'quiet', 'title': 'Stealth Mode'},
                {'item': 'fast_submit', 'title': 'Fastest Shooting Submitter', 'feel': 'fast'},
                {'item': 'slow_submit', 'title': 'Slow Poke Submitter', 'feel': 'slow'},
                {'item': 'fast_vote', 'title': 'Decisive Voter Mind', 'feel': 'fast'},
                {'item': 'slow_vote', 'title': 'Voting Analysis Paralysis', 'feel': 'slow'},
                # {'item': 'delay', 'title': 'Hold Up', 'feel': ''},
                ]

        pulse_titles = [{'item': 'likes', 'title': 'Likes best'},
                        {'item': 'liked', 'title': 'Most liked by'},
                        {'item': 'closest', 'title': 'Most similar to'}
                        ]

        stats_titles = [{'item': 'win_rate', 'title': 'Batting Average'},
                        {'item': 'play_rate', 'title': 'Games Played'},
                        ]

        if parameters.get('god'):
            awards_list = [(f'{self.feel(it.get("feel", it["item"]))}'
                            f'{it["title"]}{self.feel(it.get("feel", it["item"]))}: '
                            f'{self.texter.get_plurals(parameters[it["item"]], markdown="**")["text"]}'
                            ) \
                for it in award_titles if parameters.get(it['item'])]
        
            awards = self.list_items(awards_list)
   
        else:
            leagues_list = []
            league_titles = parameters.get('leagues')
            if len(league_titles):
                league_titles = [self.feel_title(t, markdown='**') for t in league_titles]
                leagues_in = self.texter.get_plurals(league_titles)['text']
                other_leagues = 'Other ' if parameters.get('other_leagues') else ''
                leagues_list.append(f'{other_leagues}Leagues Played In: {leagues_in}')
            leagues = self.list_items(leagues_list) if subsection == 'stats' else ''

            competitions_list = []
            if parameters.get('current_competition'):
                place = self.texter.get_ordinal(parameters['badge2'])
                total = parameters['n_players']
                competitions_list.append(f'Currently competing in {self.feel("competitions")}'
                                            f'{parameters["current_competition"]}{self.feel("competitions")}'
                                            f'{self.newline(1)}{self.indent()}(ranked {place} of {total})')
            competitions = self.list_items(competitions_list) if subsection == 'stats' else ''

            stats_list = [(f'{it["title"]}: {self.feel(it.get("feel", it["item"]))}'
                            f'**{parameters[it["item"]]:.3f}**'
                            f'{self.feel(it.get("feel", it["item"]))}') \
                for it in stats_titles if parameters.get(it['item'])]

            stats = self.list_items(stats_list) if subsection == 'stats' else ''

            pulse_list = [(f'{it["title"]}: {self.feel(it.get("feel", it["item"]))}'
                            f'{self.texter.get_plurals(parameters[it["item"]], markdown="**")["text"]}'
                            f'{self.feel(it.get("feel", it["item"]))}') \
                for it in pulse_titles if parameters.get(it['item'])]

            pulse = self.list_items(pulse_list) if subsection == 'pulse' else ''

            awards_list = [(f'{self.feel(it.get("feel", it["item"]))}'
                            f'**{it["title"]}**'
                            f'{self.feel(it.get("feel", it["item"]))}') \
                for it in award_titles if parameters.get(it['item'])]

            if not len(awards_list):
                awards_list.append(f'{self.feel("participant")}**Participation Trophy**{self.feel("participant")}')
            awards = self.list_items(awards_list) if subsection == 'awards' else ''

            wins_titles = [{'item': 'wins', 'title': 'Round'},
                            {'item': 'competition_wins', 'title': 'Competition', 'feel': 'competitions'},
                            ]

            wins_list = [(f'{it["title"]}'
                            f'{self.texter.get_plurals([self.texter.clean_text(t) for t in parameters[it["item"]]])["s"]}'
                            f' won: {self.feel(it.get("feel", it["item"]))}'
                            f'{self.texter.get_plurals([self.texter.clean_text(t) for t in parameters[it["item"]]], markdown="**")["text"]}'
                            f'{self.feel(it.get("feel", it["item"]))}') \
                for it in wins_titles if parameters.get(it['item'])]

            wins = self.list_items(wins_list) if subsection == 'wins' else ''

        text = self.bar().join(i for i in [leagues, competitions, awards, pulse, stats, wins] if len(i))

        return text

    def list_items(self, items_list):
        if len(items_list):
            items = ''.join(f'{x}{self.newline(1)}' for x in items_list) + self.newline(1)
        else:
            items = ''

        return items

    def feel_title(self, text, default=None, markdown=''):
        if not default:
            default = self.emoji.get('round_title')
        clean_text = self.texter.clean_text(text)
        emoji = self.texter.match_emoji(text, self.emojis, default=default)
        title = f'{emoji}{markdown}{clean_text}{markdown}{emoji}'

        return title

    def feel(self, text, default=''):
        return self.emoji.get(text, default)