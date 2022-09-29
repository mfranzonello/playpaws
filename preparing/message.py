from common.data import Database

# class that checks Gmail
class Mailer:
    complete_phrase = 'The Votes Are In'

    def __init__(self, my_alias:str, notifier_alias):#, database:Database):
        self.alias = alias
        #self.database = database
        #self.leagues = self.database.get_leagues()
        
    # determines if there is an update ready
    def check_mail(self):
        need_to_update = False

        new_messages = self.check_new_messages
        # check to see if there are unread messages from the notifier alias that match the string

        return need_to_update

    def check_new_messages(self):
        # look for unread messages

        new_messages = None

        return new_messages

    def mark_as_read(self, message):
        # mark as new message as read

        return

    def mark_messages_as_read(self, messages):
        for message in messages:
            self.mark_as_read(self, message)

