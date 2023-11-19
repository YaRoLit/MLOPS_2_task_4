import sys
from telethon.sync import TelegramClient


#This is personal data for Telegram autorization
api_id      = XXXXX
api_hash    = 'XXXXX'
phone       = '7913XXXX'
#===============================================


def send_text_message_to_Telegram(receiver: str, message: str) -> None:
    '''Just send the message'''

    client = TelegramClient(
        phone,
        api_id,
        api_hash)
    
    client.start()

    entity = client.get_entity(receiver)
    
    client.send_message(
        entity=entity,
        message=message)


def send_file_to_Telegram(receiver: str, file: str) -> None:
    '''Just send the file'''

    client = TelegramClient(
        phone,
        api_id,
        api_hash)
    
    client.start()

    entity = client.get_entity(receiver)

    client.send_file(
        entity=entity,
        file=file)


if __name__ == "__main__":

    try:
        message_type = sys.argv[1]
        message_body = sys.argv[2]
        receiver_id  = sys.argv[3]

    except:
        raise ValueError ('''Please use correct parameters:
            [1]-type of message (file or text),
            [2]-message body: text or file path
            [3]-receiver id: username or channel link''')

    if message_type == 'text':
        send_text_message_to_Telegram(
            receiver=receiver_id,
            message=message_body
        )
    elif message_type == 'file':
        send_file_to_Telegram(
            receiver=receiver_id,
            file=message_body
        )
    else:
        raise ValueError ('Wrong message type (needs "text" or "file")')