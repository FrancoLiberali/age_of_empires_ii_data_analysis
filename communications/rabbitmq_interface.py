from communications.constants import SENTINEL_MESSAGE, STRING_COLUMN_SEPARATOR, STRING_ENCODING, MATCHES_IDS_SEPARATOR, STRING_LINE_SEPARATOR

# TODO que no sea tan distinto hablar con un channel o con un exchange

def send_matches_ids(channel, queue_name, matches_ids):
    matches_ids_string = MATCHES_IDS_SEPARATOR.join(matches_ids)
    send_string_to_queue(channel, queue_name, matches_ids_string)

def send_sentinel_to_queue(channel, queue_name):
    send_string_to_queue(channel, queue_name, SENTINEL_MESSAGE)


def send_string_to_queue(channel, queue_name, message):
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message.encode(STRING_ENCODING)
    )


def send_list_of_columns_to_exchange(channel, exchange_name, list_of_columns, routing_key=''):
    list_string = STRING_LINE_SEPARATOR.join(
        [
            STRING_COLUMN_SEPARATOR.join(columns) for columns in list_of_columns
        ]
    )
    send_string_to_exchange(
        channel, exchange_name, list_string, routing_key)

def send_string_to_exchange(channel, exchange_name, message, routing_key=''):
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key,
        body=message.encode(STRING_ENCODING)
    )


def send_sentinel_to_exchange(channel, exchange_name, routing_key=''):
    send_string_to_exchange(channel, exchange_name,
                            SENTINEL_MESSAGE, routing_key)
