import pika
from datetime import datetime, time

from communications.constants import FROM_CLIENT_MATCH_AVERAGE_RATING_INDEX, \
    FROM_CLIENT_MATCH_DURATION_INDEX, \
    FROM_CLIENT_MATCH_SERVER_INDEX, \
    FROM_CLIENT_MATCH_TOKEN_INDEX, GROUP_BY_CIV_REDUCERS_TO_WINNER_RATE_CALCULATOR_QUEUE_NAME, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_FANOUT_EXCHANGE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import send_list_of_columns_to_queue, send_matches_ids, send_sentinel_to_queue, send_string_to_queue

FROM_GROUP_BY_CIV_CIV_INDEX = 0
FROM_GROUP_BY_CIV_WINS_INDEX = 1
FROM_GROUP_BY_CIV_DEFEATS_INDEX = 2

WINS_INDEX = 0
DEFEATS_INDEX = 1

def get_group_wins_and_defeats_by_civ_function(wins_and_defeats_by_civ):
    def group_wins_and_defeats_by_civ(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping receiving")
            channel.stop_consuming()
        else:
            for civ_wins_and_defeats in chunk_string.split(STRING_LINE_SEPARATOR):
                columns = civ_wins_and_defeats.split(STRING_COLUMN_SEPARATOR)
                civ = columns[FROM_GROUP_BY_CIV_CIV_INDEX]
                wins_and_defeats_of_civ = wins_and_defeats_by_civ.get(
                    civ, [0, 0])
                wins_and_defeats_of_civ[WINS_INDEX] += int(columns[FROM_GROUP_BY_CIV_WINS_INDEX])
                wins_and_defeats_of_civ[DEFEATS_INDEX] += int(columns[FROM_GROUP_BY_CIV_DEFEATS_INDEX])
                wins_and_defeats_by_civ[civ] = wins_and_defeats_of_civ
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return group_wins_and_defeats_by_civ


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()
    channel.queue_declare(
        queue=GROUP_BY_CIV_REDUCERS_TO_WINNER_RATE_CALCULATOR_QUEUE_NAME)

    channel.queue_declare(queue=WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    wins_and_defeats_by_civ = {}
    channel.basic_consume(
        queue=GROUP_BY_CIV_REDUCERS_TO_WINNER_RATE_CALCULATOR_QUEUE_NAME,
        on_message_callback=get_group_wins_and_defeats_by_civ_function(
            wins_and_defeats_by_civ),
    )

    print('Starting to receive wins and defeats by civ to calculate winner rate')
    channel.start_consuming()

    print('All wins and defeats received, calculating winner rate')
    winner_rates = {}
    for civ, wins_and_defeats in wins_and_defeats_by_civ.items():
        wins = wins_and_defeats[WINS_INDEX]
        defeats = wins_and_defeats[DEFEATS_INDEX]
        winner_rates[civ] = str(wins / (wins + defeats) * 100)

    sorted_by_winner_rate = sorted(
        winner_rates.items(), key=lambda x: x[1], reverse=True)
    send_list_of_columns_to_queue(
        channel,
        WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME,
        sorted_by_winner_rate
    )

    connection.close()


if __name__ == '__main__':
    main()
