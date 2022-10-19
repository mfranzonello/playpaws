''' Manual override from JSON to fix league misses '''

import display.printing
from preparing.update import Extender

def reopen_all():
    extender = Extender(database=None)
    extender.reopen_all()

def set_bonus():
    pass

def start_competitions():
    pass

def end_competitions():
    pass
        
def main():
    reopen_all()

if __name__ == '__main__':
    main()