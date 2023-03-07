''' Keeps cookies fresh and accessible '''

from preparing.security import Lockbox, Baker, Selena

##def update_secrets():
##    lockbox = Lockbox()
##    lockbox.update_secrets()

##def update_cookies():
##    lockbox = Lockbox()
##    baker = Baker()
##    selena = Selena()

##    # see if cookies have expired
##    stale = baker.check_freshness()

##    # refresh cookies with background Chrome
##    if stale:
##        selena.turn_on()
##        selena.login()
##        found_cookie = selena.get_cookies()
##        selena.turn_off()

##        # store new cookies
##        baker.bake_cookies(None, found_cookie, lockbox)

##    else:
##        baker.reset_cookies(lockbox)

def update_cookies():
    lockbox = Lockbox()
    baker = Baker()
    selena = Selena()

    # create cookies
    selena.turn_on()
    selena.login()
    found_cookie = selena.get_cookies()
    selena.turn_off()

    # store cookies
    baker.bake_cookies(found_cookie, lockbox)

def main():
    #update_secrets()
    update_cookies()

if __name__ == '__main__':
    main()