import pika
import time

from common.constants import RATING_INDEX
from common.partition_function import get_key, get_posibles_keys
from communications.constants import CLIENT_TO_WEAKER_WINNER_QUEUE_NAME, FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME, STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE

MINIMUM_RATING = 1000  # TODO envvar

# TODO codigo repetido con el filtro de long matches

def send_to_group_by(channel, matched_rows):
    rows_by_key = {}
    for matched_row in matched_rows:
        key = get_key(matched_row)
        rows_list = rows_by_key.get(key, [])
        rows_list.append(STRING_COLUMN_SEPARATOR.join(matched_row))
        rows_by_key[key] = rows_list

    # print(f"rows to send: {rows_by_key}")
    for key, rows in rows_by_key.items():
        rows_string = STRING_LINE_SEPARATOR.join(rows)
        channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                              routing_key=key,
                              body=rows_string.encode(STRING_ENCODING))
    
def send_sentinel(channel):
    for key in get_posibles_keys():
        channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                              routing_key=key,
                              body=SENTINEL_MESSAGE.encode(STRING_ENCODING))

def is_matched(columns):
    rating = columns[RATING_INDEX]
    return rating != '' and int(rating) > MINIMUM_RATING

def filter_by_rating(channel, method, properties, body):
    # TODO codigo repetido lo del receive sentinel
    chunk_string = body.decode(STRING_ENCODING)
    if chunk_string == SENTINEL_MESSAGE:
        print("Sentinel message received, stoping consuming")
        channel.stop_consuming()
        send_sentinel(channel)
    else:
        matched_rows = []
        for row in chunk_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched(columns):
                matched_rows.append(columns)
        if (len(matched_rows) > 0):
            send_to_group_by(channel, matched_rows)


def main():
    # TODO hacer algo para que este espere que todos los reducers esten listos, porque el exchange no rutea si tardan en susbscribirse
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()
    channel.queue_declare(queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME)
    channel.exchange_declare(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        exchange_type='direct')

    channel.basic_consume(
        queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME,
        on_message_callback=filter_by_rating,
        auto_ack=True  # TODO sacar esto
    )

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    connection.close()


if __name__ == '__main__':
    main()
