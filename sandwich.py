from preparing.security import Lockbox, Baker

def update_cookies():
    lockbox = Lockbox()
    baker = Baker()

    baker.reset_cookies(lockbox)

def main():
    update_cookies()

if __name__ == '__main__':
    main()
