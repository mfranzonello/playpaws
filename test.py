from pickle import dump, load

class Pickler:
    def pickle(obj, directory, name):
        dump(obj, open(f'{directory}/{name}.p', 'wb'))

    def unpickle(directory, name):
        obj = load(open(f'{directory}/{name}.p', 'rb'))
        return obj


