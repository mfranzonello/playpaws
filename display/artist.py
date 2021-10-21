from colorsys import rgb_to_hsv
from math import inf, nan, isnan
from urllib.request import urlopen

from PIL import Image, ImageDraw, ImageFont, ImageOps, UnidentifiedImageError
from numpy import asarray
from pandas import DataFrame
from colorthief import ColorThief

from display.media import Imager, Gallery, Byter
from display.storage import Boxer
from display.streaming import Streamable

class Paintbrush:
    color_wheel = 255
    
    colors = {'grey': (172, 172, 172),
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
              'copper': (145, 110, 45),
              'gold': (212, 175, 55),
              'silver': (192, 192, 192),
              'bronze': (205, 127, 50),
              'gunmetal_grey': (99, 96, 89),
              'dark_grey': (75, 75, 75),
              }

    tableau_colors = {'c0': (31, 119, 180),
                      'c1': (255, 127, 14),
                      'c2': (44, 160, 44),
                      'c3': (214, 39, 40),
                      'c4': (148, 103, 189),
                      'c5': (140, 86, 75),
                      'c6': (227, 119, 194),
                      'c7': (127, 127, 127),
                      'c8': (188, 189, 34),
                      'c9': (23, 190, 207),
                      }

    def __init__(self):
        self.byter = Byter()

    def get_color(self, color_name, normalize=False, hsv=False, hx=False, lighten=None):
        color = self.colors.get(color_name, (0, 0, 0))
        if lighten:
            color = self.lighten_color(color, lighten)

        if normalize:
            color = self.normalize_color(color)
        elif hsv:
            color = self.hsv_color(color)
        elif hx:
            color = self.hex_color(color)

        return color

    def get_colors(self, *color_names):
        if len(color_names) == 1:
            colors = self.get_color(color_names[0])
        else:
            colors = [self.get_color(color_name) for color_name in color_names]
        
        return colors

    def get_plot_color(self, color_name):
        return self.tableau_colors.get(color_name.lower(), (0, 0, 0))

    def lighten_color(self, color, pct=0):
        color = tuple(int(max(0, min(self.color_wheel, c + pct * self.color_wheel))) for c in color)
        return color

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

    def get_scatter_colors(self, colors_rgb):
        colors = [self.normalize_color(rgb, self.color_wheel) for rgb in colors_rgb]
        return colors

    def normalize_color(self, color, divisor=color_wheel):
        color = tuple(c / divisor for c in color)
        return color

    def hsv_color(self, color):
        color = rgb_to_hsv(*(c/self.color_wheel for c in color))
        return color

    def hex_color(self, color):
        color = '#' + ''.join('{c:02x}' for c in color)
        return color

    def get_palette(self, image, color_count=30):
        cf = ColorThief(self.byter.bit_me(image))
        full_palette = cf.get_palette(color_count=color_count, quality=1)
        palette = sorted(full_palette, key=self.is_prominent, reverse=True)

        return palette

    def is_prominent(self, rgb):
        # rank based on order (appearance), darkness (value below), greyness (RGB are all close) and melanin (skin colors)
        prominent = (1- self.is_dark(rgb)) * (1- self.is_light(rgb)) * sum([1-self.is_grey(rgb), 1-self.is_skin(rgb)]) / 2 #not any(f(rgb) for f in [self.is_dark, self.is_grey, self.is_skin])

        return prominent

    def is_light(self, rgb, threshold=250):
        light = min(rgb) >= threshold

        return light

    def is_dark(self, rgb, threshold=100):
        dark = max(rgb) < threshold

        return dark

    def is_grey(self, rgb, threshhold=40):
        r, g, b = rgb
        grey = sum([(r-g)**2 + (r-b)**2 + (g-b)**2])**0.5 < threshhold

        return grey

    def is_skin(self, rgb, skin_tone=(232, 190, 172), r_weight=2, g_weight=4, b_weight=3, threshold=100):
        # find the distance between two colors
        weights = [r_weight, g_weight, b_weight]
        
        # use euclidian 
        skin = sum(w * (c0 - c1)**2 for w, c0, c1 in zip(weights, rgb, skin_tone))**0.5 < threshold
        
        return skin

class Canvas(Imager, Streamable):
    def __init__(self, database, streamer=None):
        super().__init__()
        self.gallery = Gallery(database, crop=True)
        self.boxer = Boxer()
        self.add_streamer(streamer)
        self.mobis = {}
        self.paintbrush = Paintbrush()
        
        self.ppt = 0.75

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

    def add_border(self, image, color=(0,0,0), padding=0):
        ##w0, h0 = image.size
        ##image = image.resize((self.antialias*w0, self.antialias*h0))
        W, H = image.size
        border_size = (int(W * (1 + padding/2)), int(H * (1 + padding/2)))

        if color == 'palette':
            color = self.paintbrush.get_palette(image)[0]

        border = self.get_color_image(color, border_size)
        w, h = border.size
        border.paste(image, ((w-W)//2, (h-H)//2), image)
        border = border.resize((W, H))#, resample=Image.ANTIALIAS)
        ##border = border.resize((w0, h0)), resample=Image.ANTIALIAS)
        return border

    def add_badge(self, image, text, font, pct=0.25, color=(0,0,0), border_color=(0,0,0),
                  padding=0, position='LR'):
        W, H = image.size
        badge_size = (int(W * pct), int(H * pct))
        w, h = badge_size
        badge = self.get_color_image(color, badge_size)
        badge = self.add_text(badge, text, font)
        badge = self.add_border(badge, color=border_color, padding=padding)

        # place badge on image
        if position not in ['UL', 'UR', 'LL', 'LR']:
            x, y = (0, 0)
        else:
            if position[0] == 'L':
                y = H - h - 1
            elif position[0] == 'U':
                y = 1
            if position[1] == 'L':
                x = 1
            elif position[1] == 'R':
                x = W - w - 1

        image.paste(badge, (x, y), badge)

        return image
        
    def get_mask_array(self, image_bytes):
        #fp = urlopen(src)
        try:
            image = Image.open(image_bytes) #fp)
            mask = asarray(image)
        except UnidentifiedImageError:
            self.streamer.print(f'can\'t open image') # at {src}')
            mask = None

        return mask

    def get_time_parameters(self, text_df, aspect, base):
        h = base
        w = aspect[0] * h / aspect[1]
        
        # remove text too small
        text_df = text_df.replace([inf, -inf], nan).dropna(subset=['size'])
        
        # normalize x location    
        x_max = text_df['x'].max()
        x_min = text_df['x'].min()
        text_df['x'] = text_df.apply(lambda x: (x['x'] - x_max)/(x_min - x_max) * w, axis=1)
       
        # find length of text
        text_df['image_font'] = text_df.apply(lambda x: ImageFont.truetype(x['font_name'],
                                                                           int(x['size'] * self.ppt * base / 2)),
                                              axis=1)
        text_df['length_top'] = text_df.apply(lambda x: x['image_font'].getmask(x['text_top']).getbbox()[2],
                                              axis=1)
        text_df['length_bottom'] = text_df.apply(lambda x: x['image_font'].getmask(x['text_bottom']).getbbox()[2],
                                                 axis=1)
        text_df['length'] = text_df[['length_top', 'length_bottom']].max(1)
        text_df['height'] = text_df.apply(lambda x: x['image_font'].getmetrics()[0] \
                                                    - x['image_font'].font.getsize(x['text_bottom'])[1][1],
                                          axis=1)
        text_df['left'] = text_df.apply(lambda x: x['x'] - base * x['size']/2,
                                        axis=1)
        text_df['right'] = text_df.apply(lambda x: x['x'] + base * x['size']/2,
                                         axis=1)

        # adjust width for box on the edge
        left = text_df['left'].min()
        right = text_df['right'].max()

        W = int(-left + right)
        H = int(h)
        x0 = -left
        x1 = right - w

        return text_df, W, H, x0, x1

    def get_timeline_image(self, text_df, W, H, x0, x1, base, highlight_color,
                           padding=0.1, min_box_size=5):
        image = Image.new('RGBA', (W, H), (255, 255, 255, 0))
        draw = ImageDraw.Draw(image)
        
        for i, text_row in text_df.iterrows():
            box_src = text_row['src']
            box_size = text_row['size'] * base
            padded_size = box_size * (1 - padding)
            box_color = text_row['color']
            pad_offset = box_size * padding / 2

            x = W - box_size/2 if (text_row['x'] == text_df['x'].max()) else min(text_row['x'] + x0, W - box_size/2)
            y = text_row['y'] * base

            if padded_size:
                if box_src and padded_size > min_box_size:
                    src_size = tuple([int(padded_size)] * 2)
                    box_img = Image.open(urlopen(box_src)).resize(src_size)

                    if text_row['status']=='open':
                        # grey out an image in an open round
                        box_img = ImageOps.grayscale(box_img)
                        
                    x_adj = box_size/2
                    image.paste(box_img, (int(x - x_adj + pad_offset), int(y + pad_offset)))
                    
                else:
                    draw.rectangle([int(x - x_adj + pad_offset),
                                    int(y + pad_offset),
                                    int(x + x_adj - pad_offset),
                                    int(y + box_size - pad_offset)],
                                   fill=box_color)
                if text_row['highlight']:
                    draw.rectangle([int(x - box_size/2 + pad_offset), int(y + pad_offset),
                                    int(x + box_size/2 - pad_offset), int(y + box_size - pad_offset)],
                                   outline=highlight_color, width=int(pad_offset))

            flip = x + box_size/2 + text_row['length'] > W
            if not flip:
                x_text_top = x + box_size/2
                x_text_bottom = x + box_size/2
            else:
                x_text_top = x - box_size/2 - text_row['length_top']
                x_text_bottom = x - box_size/2 - text_row['length_bottom']
            
            draw.text((int(x_text_top), int(y)), text_row['text_top'],
                      fill=box_color, font=text_row['image_font'])
            draw.text((int(x_text_bottom), int(y + box_size/2)), text_row['text_bottom'], #- text_row['height']/2 * self.ppt
                      fill=box_color, font=text_row['image_font'])
            if text_row['highlight']:
                y_line = y - pad_offset/2
                draw.rectangle([int(x_text_top), int(y_line + box_size/2),
                                int(x_text_top + text_row['length_top']), int(y_line + box_size/2) - max(1, int(pad_offset/3))],
                                fill=box_color)
                draw.rectangle([int(x_text_bottom), int(y_line + box_size),
                                int(x_text_bottom + text_row['length_bottom']), int(y_line + box_size) - max(1, int(pad_offset/3))],
                                fill=box_color)

        return image