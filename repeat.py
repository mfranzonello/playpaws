''' Pushes out deadlines as needed '''

from preparing.update import Extender
from common.data import Database

def update_deadlines():
    database = Database()
    extender = Extender(database)

    extender.extend_all_deadlines(days=1, hours_left=24)

def main():
    update_deadlines()

if __name__ == '__main__':
    main()