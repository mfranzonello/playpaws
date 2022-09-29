from preparing.security import Lockbox, Baker

def update_cookies():
    lockbox = Lockbox()
    baker = Baker('https://app.musicleague.com')

    baker.reset_cookies(lockbox)

def main():
    update_cookies()

if __name__ == '__main__':
    main()
