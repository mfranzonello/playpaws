from math import inf, nan, isnan
from urllib.request import urlopen

from PIL import Image, ImageDraw, ImageFont, ImageOps, UnidentifiedImageError
from matplotlib.dates import date2num
from numpy import asarray
from pandas import DataFrame

from display.media import Imager, Gallery
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
              }

    def __init__(self):
        pass

    def get_color(self, color_name):
        return self.colors.get(color_name, (0, 0, 0))

    def get_colors(self, *color_names):
        if len(color_names) == 1:
            colors = self.get_color(color_names[0])
        else:
            colors = [self.get_color(color_name) for color_name in color_names]
        
        return colors

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

    def normalize_color(self, color, divisor):
        color = tuple(c / divisor for c in color)
        return color

class Canvas(Imager, Streamable):
    def __init__(self, database, streamer=None):
        super().__init__()
        self.gallery = Gallery(database, crop=True)
        self.boxer = Boxer()
        self.add_streamer(streamer)
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

    def add_border(self, image, color=(0,0,0), padding=0):
        W, H = image.size
        border_size = (int(W * (1 + padding/2)), int(H * (1 + padding/2)))
        border = self.get_color_image(color, border_size)
        w, h = border.size
        border.paste(image, ((w-W)//2, (h-H)//2), image)
        border = border.resize(image.size)
        return border

    def add_badge(self, image, text, font, pct=0.25, color=(0,0,0), border_color=(0,0,0), padding=0):
        W, H = image.size
        badge_size = (int(W * pct), int(H * pct))
        w, h = badge_size
        badge = self.get_color_image(color, badge_size)
        badge = self.add_text(badge, text, font)
        badge = self.add_border(badge, color=border_color, padding=padding)

        # place badge on image
        image.paste(badge, (W - w - 1, H - h - 1), badge)

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
        H = int(base) #int((text_df['y_round'].max() + 1) * base)
        W = int(aspect[0] * H / aspect[1])

        ppt = 0.75

        max_x = text_df['x'].max()

        # remove text too small
        text_df = text_df.replace([inf, -inf], nan).dropna(subset=['size'])

        text_df['image_font'] = text_df.apply(lambda x: ImageFont.truetype(x['font_name'],
                                                                           int(x['size'] * ppt * base / 2)), axis=1)
        text_df['length'] = text_df.apply(lambda x: max(x['image_font'].getmask(x['text_top']).getbbox()[2],
                                                        x['image_font'].getmask(x['text_bottom']).getbbox()[2]),
                                          axis=1)
        
        text_df['total_length'] = text_df['length'].add(date2num(max_x) - date2num(text_df['x']))
        max_x_i = text_df[text_df['total_length'] == text_df['total_length'].max()].index[0]

        X = (date2num(max_x) - date2num(text_df['x'][max_x_i]))
        x_ratio = (W - text_df['length'][max_x_i]) / X
        D = x_ratio * X

        return text_df, D, max_x, x_ratio, W, H, base
       
    def get_timeline_image(self, text_df, max_x, x_ratio, W, H, base,
                           padding=0.1, min_box_size=5):
        image = Image.new('RGB', (W, H), (255, 255, 255))
        draw = ImageDraw.Draw(image)

        for i in text_df.index:
            x_text = int(x_ratio * (date2num(max_x) - date2num(text_df['x'][i])))
            y_text = int(text_df['y'][i] * base)

            box_src = text_df['src'][i]
            box_size = int(text_df['size'][i] * base)
            padded_size = int(box_size * (1 - padding))
            box_color = text_df['color'][i]
            pad_offset = int(box_size * padding / 2)

            if padded_size:
                if box_src and padded_size > min_box_size:
                    src_size = tuple([padded_size] * 2)
                    box_img = Image.open(urlopen(box_src)).resize(src_size)

                    if isnan(text_df['points'][i]):
                        # grey out an image without points
                        ImageOps.grayscale(box_img)

                    image.paste(box_img, (x_text + pad_offset, y_text + pad_offset))
                else:
                    draw.rectangle([x_text + pad_offset,
                                    y_text + pad_offset,
                                    x_text + box_size - pad_offset,
                                    y_text + box_size - pad_offset],
                                   fill=box_color)

            draw.text((x_text + box_size, y_text), text_df['text_top'][i],
                      fill=box_color, font=text_df['image_font'][i])
            draw.text((x_text + box_size, y_text + box_size/2), text_df['text_bottom'][i],
                      fill=box_color, font=text_df['image_font'][i])

        return image