from os import getenv
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv)

def get_secret(name):
    return getenv(name)