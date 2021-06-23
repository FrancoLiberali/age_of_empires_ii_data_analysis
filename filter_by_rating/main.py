import os

from communications.constants import FROM_CLIENT_PLAYER_CIV_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_RATING_INDEX, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection, get_on_sentinel_send_sentinel_callback_function

MIN_RATING = 2000
OUTPUT_EXCHANGE_NAME = os.environ["OUTPUT_EXCHANGE_NAME"]


# TODO codigo repetido con el otr filtro de matches, misma estructura para filtro

def is_matched(columns):
    # TODO codigo repetido con long matches server el rating
    rating = columns[FROM_CLIENT_PLAYER_RATING_INDEX]
    return rating != '' and int(rating) > MIN_RATING


def get_filter_by_rating_function(output_exchange):
    def filter_by_rating(queue, received_string, _):
        players_matched = []
        for row in received_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched(columns):
                players_matched.append(
                    [
                        columns[FROM_CLIENT_PLAYER_MATCH_INDEX],
                        columns[FROM_CLIENT_PLAYER_CIV_INDEX],
                    ]
                )
        if (len(players_matched) > 0):
            output_exchange.send_list_of_columns(players_matched)
    return filter_by_rating


def main():
    # TODO codigo repetido con group by match
    connection = RabbitMQConnection()
    input_exchage = ExchangeInterface.newFanout(
        connection, PLAYERS_FANOUT_EXCHANGE_NAME)
    input_queue = QueueInterface.newPrivate(connection)
    input_queue.bind(input_exchage)

    output_exchage = ExchangeInterface.newFanout(
        connection, OUTPUT_EXCHANGE_NAME)

    print(f'Starting to receive players to filter by rating > {MIN_RATING}')
    input_queue.consume(
        get_filter_by_rating_function(output_exchage),
        get_on_sentinel_send_sentinel_callback_function(output_exchage)
    )

    connection.close()


if __name__ == '__main__':
    main()
