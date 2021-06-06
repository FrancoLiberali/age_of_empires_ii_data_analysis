from communications.constants import SENTINEL_MESSAGE, STRING_ENCODING, MATCHES_IDS_SEPARATOR

def send_matches_ids(channel, queue_name, matches_ids):
    response_string = MATCHES_IDS_SEPARATOR.join(matches_ids)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=response_string.encode(STRING_ENCODING)
    )

# TODO usar tambien el client esto
def send_sentinel(channel, queue):
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=SENTINEL_MESSAGE.encode(STRING_ENCODING)
    )
