import os
import pika

from communications.constants import FROM_CLIENT_PLAYER_CIV_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_RATING_INDEX, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE
from communications.rabbitmq_interface import send_list_of_columns_to_exchange, send_sentinel_to_exchange


MIN_RATING = 2000
OUTPUT_EXCHANGE_NAME = os.environ["OUTPUT_EXCHANGE_NAME"]


# TODO codigo repetido con el otr filtro de matches, misma estructura para filtro

def is_matched(columns):
    # TODO codigo repetido con long matches server el rating
    rating = columns[FROM_CLIENT_PLAYER_RATING_INDEX]
    return rating != '' and int(rating) > MIN_RATING


def filter_by_rating(channel, method, properties, body):
    chunk_string = body.decode(STRING_ENCODING)
    if chunk_string == SENTINEL_MESSAGE:
        print("Sentinel message received, stoping receiving matches")
        channel.stop_consuming()
        print("Sending sentinel to next stage to notify that all matches ids has been sended")
        send_sentinel_to_exchange(
            channel, OUTPUT_EXCHANGE_NAME)
    else:
        players_matched = []
        for row in chunk_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched(columns):
                players_matched.append(
                    [
                        columns[FROM_CLIENT_PLAYER_MATCH_INDEX],
                        columns[FROM_CLIENT_PLAYER_CIV_INDEX],
                    ]
                )
        if (len(players_matched) > 0):
            send_list_of_columns_to_exchange(
                channel, OUTPUT_EXCHANGE_NAME, players_matched)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()

    # TODO codigo repetido con group by match
    channel.exchange_declare(
        exchange=PLAYERS_FANOUT_EXCHANGE_NAME,
        exchange_type='fanout') # TODO variable global
    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    channel.queue_bind(
        exchange=PLAYERS_FANOUT_EXCHANGE_NAME,
        queue=private_queue_name)

    channel.exchange_declare(
        exchange=OUTPUT_EXCHANGE_NAME,
        exchange_type="fanout") # TODO ver donde va esto

    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=filter_by_rating,
    )

    print(f'Starting to receive players to filter by rating > {MIN_RATING}')
    channel.start_consuming()
    connection.close()


if __name__ == '__main__':
    main()
