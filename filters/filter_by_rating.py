from config.envvars import MIN_RATING_KEY, OUTPUT_EXCHANGE_NAME_KEY, get_config_param
from communications.constants import FROM_CLIENT_PLAYER_CIV_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_RATING_INDEX, \
    PLAYERS_FANOUT_EXCHANGE_NAME
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection, get_on_sentinel_send_sentinel_callback_function, split_columns_into_list, split_rows_into_list
from logger.logger import Logger

logger = Logger()
MIN_RATING = get_config_param(MIN_RATING_KEY, logger)

def is_matched(columns):
    rating = columns[FROM_CLIENT_PLAYER_RATING_INDEX]
    return rating != '' and int(rating) > MIN_RATING


def get_filter_by_rating_function(output_exchange):
    def filter_by_rating(queue, received_string, _):
        players_matched = []
        for row in split_rows_into_list(received_string):
            columns = split_columns_into_list(row)
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
    connection = RabbitMQConnection()
    input_exchage = ExchangeInterface.newFanout(
        connection, PLAYERS_FANOUT_EXCHANGE_NAME)
    input_queue = QueueInterface.newPrivate(connection)
    input_queue.bind(input_exchage)

    output_exchage = ExchangeInterface.newFanout(
        connection,
        get_config_param(OUTPUT_EXCHANGE_NAME_KEY, logger)
    )

    logger.info(
        f'Starting to receive players to filter by rating > {MIN_RATING}')
    input_queue.consume(
        get_filter_by_rating_function(output_exchage),
        on_sentinel_callback=get_on_sentinel_send_sentinel_callback_function(
            output_exchage)
    )

    connection.close()


if __name__ == '__main__':
    main()
