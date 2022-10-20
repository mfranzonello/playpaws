''' Keeps cookies fresh and accessible '''

from preparing.security import Lockbox, Baker, Selena

def update_secrets():
    lockbox = Lockbox()
    lockbox.update_secrets()

def update_cookies():
    lockbox = Lockbox()
    baker = Baker()
    selena = Selena()

    # see if cookies have expired
    stale = baker.check_freshness()

    # refresh cookies with background Chrome
    if stale:
        selena.turn_on()
        selena.go_to_site()
        selena.turn_off()

    # store new cookies
    baker.reset_cookies(lockbox)

def main():
    update_secrets()
    update_cookies()

if __name__ == '__main__':
    main()