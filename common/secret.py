from os import getenv
from dotenv import load_dotenv, find_dotenv, set_key

dotenv_file = load_dotenv(find_dotenv())

def get_secret(name):
    return getenv(name)

def set_secret(name, value):
    set_key(dotenv_file, name, value)