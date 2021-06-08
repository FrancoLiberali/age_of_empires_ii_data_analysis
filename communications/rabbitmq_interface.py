from communications.constants import SENTINEL_MESSAGE, STRING_ENCODING, MATCHES_IDS_SEPARATOR

def send_matches_ids(channel, queue_name, matches_ids):
    matches_ids_string = MATCHES_IDS_SEPARATOR.join(matches_ids)
    send_string_to_queue(channel, queue_name, matches_ids_string)

def send_sentinel(channel, queue_name):
    send_string_to_queue(channel, queue_name, SENTINEL_MESSAGE)


def send_string_to_queue(channel, queue_name, message):
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message.encode(STRING_ENCODING)
    )
