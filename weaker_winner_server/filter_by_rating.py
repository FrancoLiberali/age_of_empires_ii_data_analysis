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
PLAYERS_CHUNK_SIZE = 100


def send_players_by_key(channel, players_by_key, check_chunk_size = True):
    for key, players in list(players_by_key.items()):
        if check_chunk_size and len(players) > PLAYERS_CHUNK_SIZE:
            players_string = STRING_LINE_SEPARATOR.join(players)
            channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                                  routing_key=key,
                                  body=players_string.encode(STRING_ENCODING))
            print(f"Sended {len(players)} players to key: {key}")
            players_by_key.pop(key)
            del players

def send_to_group_by(players_by_key, channel, matched_players):
    for matched_players in matched_players:
        key = get_key(matched_players)
        rows_list = players_by_key.get(key, [])
        rows_list.append(STRING_COLUMN_SEPARATOR.join(matched_players))
        players_by_key[key] = rows_list

    send_players_by_key(channel, players_by_key)


def send_sentinel(channel):
    for key in get_posibles_keys():
        channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                              routing_key=key,
                              body=SENTINEL_MESSAGE.encode(STRING_ENCODING))

def is_matched(columns):
    rating = columns[RATING_INDEX]
    return rating != '' and int(rating) > MINIMUM_RATING


def get_filter_by_rating_function(players_by_key):
    def filter_by_rating(channel, method, properties, body):
        # TODO codigo repetido lo del receive sentinel
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping consuming")
            channel.stop_consuming()
            send_players_by_key(channel, players_by_key, False) # send the remaining players
            send_sentinel(channel)
        else:
            matched_players = []
            for player_string in chunk_string.split(STRING_LINE_SEPARATOR):
                player_columns = player_string.split(STRING_COLUMN_SEPARATOR)
                if is_matched(player_columns):
                    matched_players.append(player_columns)
            if (len(matched_players) > 0):
                send_to_group_by(players_by_key, channel, matched_players)
    return filter_by_rating


def main():
    # TODO hacer algo para que este espere que todos los reducers esten listos, porque el exchange no rutea si tardan en susbscribirse
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()
    channel.queue_declare(queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME)
    channel.exchange_declare(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        exchange_type='direct')

    players_by_key = {}
    channel.basic_consume(
        queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME,
        on_message_callback=get_filter_by_rating_function(players_by_key),
        auto_ack=True  # TODO sacar esto
    )

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
    connection.close()


if __name__ == '__main__':
    main()
