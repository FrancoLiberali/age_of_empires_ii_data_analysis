import os
import pika

from communications.constants import MATCHES_KEY, \
    FROM_CLIENT_MATCH_TOKEN_INDEX, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_FANOUT_EXCHANGE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE
from communications.rabbitmq_interface import send_list_of_columns_to_exchange, send_sentinel_to_exchange


LADDER_1V1 = 'RM_1v1'  # TODO envvar
MAP_ARENA = 'arena' # TODO envvar
NO_MIRROR = 'False' # TODO envvar, TODO sacar de aca al hacer el punto 4

LADDER_TEAM = 'RM_TEAM'  # TODO envvar
MAP_ISLANDS = 'islands'  # TODO envvar

LADDER_INDEX_INDEX = 4
MAP_INDEX = 5
MIRROR_INDEX = 6

OUTPUT_EXCHANGE_NAME_1V1 = os.environ["OUTPUT_EXCHANGE_NAME_1V1"]
OUTPUT_EXCHANGE_NAME_TEAM = os.environ["OUTPUT_EXCHANGE_NAME_TEAM"]


# TODO codigo repetido con el otr filtro de matches, misma estructura para filtro

def is_matched_1v1(columns):
    return (
        columns[LADDER_INDEX_INDEX] == LADDER_1V1 and
        columns[MAP_INDEX] == MAP_ARENA and
        columns[MIRROR_INDEX] == NO_MIRROR
    )


def is_matched_team(columns):
    return (
        columns[LADDER_INDEX_INDEX] == LADDER_TEAM and
        columns[MAP_INDEX] == MAP_ISLANDS
    )


def send_matches(channel, matches_list, output_exchange_name):
    if (len(matches_list) > 0):
        send_list_of_columns_to_exchange(
            channel, output_exchange_name, matches_list, MATCHES_KEY)

def add_to_matches(matches_list, match_columns):
    matches_list.append(
        [
            match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX],
        ]
    )

def filter_by_ladder_and_map(channel, method, properties, body):
    chunk_string = body.decode(STRING_ENCODING)
    if chunk_string == SENTINEL_MESSAGE:
        print("Sentinel message received, stoping receiving matches")
        channel.stop_consuming()
        print("Sending sentinel to next stage to notify that all matches ids has been sended")
        send_sentinel_to_exchange(
            channel, OUTPUT_EXCHANGE_NAME_1V1, MATCHES_KEY)
        send_sentinel_to_exchange(
            channel, OUTPUT_EXCHANGE_NAME_TEAM, MATCHES_KEY)
    else:
        matches_1v1_matched = []
        matches_team_matched = []
        for row in chunk_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched_1v1(columns):
                add_to_matches(matches_1v1_matched, columns)
            elif is_matched_team(columns):
                add_to_matches(matches_team_matched, columns)
        send_matches(channel, matches_1v1_matched, OUTPUT_EXCHANGE_NAME_1V1)
        send_matches(channel, matches_team_matched, OUTPUT_EXCHANGE_NAME_TEAM)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()

    # TODO codigo repetido con long matches
    channel.exchange_declare(
        exchange=MATCHES_FANOUT_EXCHANGE_NAME,
        exchange_type='fanout')
    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    channel.queue_bind(
        exchange=MATCHES_FANOUT_EXCHANGE_NAME,
        queue=private_queue_name
    )

    channel.exchange_declare(
        exchange=OUTPUT_EXCHANGE_NAME_1V1,
        exchange_type='direct')
    channel.exchange_declare(
        exchange=OUTPUT_EXCHANGE_NAME_TEAM,
        exchange_type='direct')

    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=filter_by_ladder_and_map,
    )

    print(f'Starting to receive matches to filter by ladder, map and mirror')
    channel.start_consuming()
    connection.close()


if __name__ == '__main__':
    main()
