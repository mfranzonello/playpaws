import json

from pandas import read_json, DataFrame, Series

class Jason:
    def __init__(self):
        pass

    def to_json(self, data):
        to_store = json.dumps({key: self.break_me(data[key]) for key in data})

        return to_store

    def break_me(self, fixed):
        if isinstance(fixed, (DataFrame, Series)):
            broken = fixed.to_json()
        else:
            broken = fixed
        return broken

    def from_json(self, stored):
        loaded = json.loads(stored)
        data = {key: self.fix_me(loaded[key]) for key in loaded}

        return data

    def fix_me(self, broken):
        if self.is_json(broken):
            try:
                fixed = read_json(broken)
            except ValueError:
                fixed = read_json(broken, typ='series')
        else:
            fixed = broken

        return fixed

    def is_json(self, item):
        if type(item) is str:
            jsonable = (((item[0] == '[') and (item[-1] == ']')) or \
                ((item[0] == '{') and (item[-1] == '}'))) and \
                ((item.count('[') > 0) or (item.count('{') > 0)) and \
                (item.count('{') == item.count('}')) and \
                (item.count('[') == item.count(']'))

        else:
            jsonable = False

        return jsonable