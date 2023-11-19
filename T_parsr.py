import sys
import pandas as pd
from telethon.sync import TelegramClient


#This is personal data for Telegram autorization
api_id      = XXXXXX
api_hash    = 'XXXXX'
phone       = '7913XXXXX'
#===============================================


def get_messages(link, msg_limit):
    '''Get messages from Telegram chat'''

    client = TelegramClient(phone, api_id, api_hash)
    client.start()

    df = pd.DataFrame(
        columns=['datetime', 'user_id', 'text'])

    for message in client.get_messages(link, msg_limit):
        df.loc[len(df)] = (
            message.date, message.sender_id, message.text)

    return df


if __name__ == "__main__":

    try:
        link = sys.argv[1]
        msg_limit = sys.argv[2]
        tmp_file = sys.argv[3]
        
    except:
        raise ValueError ('''Please use correct parameters:
            [1]-chat link,
            [2]-num_of_messages,
            [3]-filename_for_write''')

    msg_limit = int(msg_limit)
 
    df = get_messages(link, msg_limit)

    df.to_csv(
        tmp_file,
        sep='\t',
        index=False,
        encoding='utf-8'
        )