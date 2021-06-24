#!/usr/bin/env python3
from config.envvars import DURATION_FORMAT_KEY, EAST_US_SERVER_KEY, KOREA_CENTRAL_SERVER_KEY, MINIMUM_AVERAGE_RATING_KEY, MINIMUM_DURATION_KEY, SOUTH_EAST_ASIA_SERVER_KEY, get_config_param
from datetime import datetime, time

from communications.constants import FROM_CLIENT_MATCH_AVERAGE_RATING_INDEX, \
    FROM_CLIENT_MATCH_DURATION_INDEX, \
    FROM_CLIENT_MATCH_SERVER_INDEX, \
    FROM_CLIENT_MATCH_TOKEN_INDEX, \
    MATCHES_FANOUT_EXCHANGE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection, get_on_sentinel_send_sentinel_callback_function, split_columns_into_list, split_rows_into_list
from logger.logger import Logger

logger = Logger()
MINIMUM_AVERAGE_RATING = get_config_param(MINIMUM_AVERAGE_RATING_KEY, logger)
MINIMUM_DURATION = time(
    hour=get_config_param(
        MINIMUM_DURATION_KEY, logger
    )
)
REQUIRED_SERVERS = [
    get_config_param(KOREA_CENTRAL_SERVER_KEY, logger),
    get_config_param(SOUTH_EAST_ASIA_SERVER_KEY, logger),
    get_config_param(EAST_US_SERVER_KEY, logger)
]
DURATION_FORMAT = get_config_param(DURATION_FORMAT_KEY, logger)

def is_average_rating_enough(average_rating_string):
    # TODO catchear exceptions
    return (average_rating_string != '' and int(average_rating_string) > MINIMUM_AVERAGE_RATING)

def is_duration_enough(duration_string):
    try:
        return (datetime.strptime(duration_string, DURATION_FORMAT).time() > MINIMUM_DURATION)
    except ValueError as e:
        if duration_string[-3:] == "day" or duration_string[-4:] == "days":
            return True
        else:
            logger.error(f"Found a duration with unknown format: {duration_string}")
            return False

def is_matched(columns):
    return (
        is_average_rating_enough(columns[FROM_CLIENT_MATCH_AVERAGE_RATING_INDEX]) and
        columns[FROM_CLIENT_MATCH_SERVER_INDEX] in REQUIRED_SERVERS and
        is_duration_enough(columns[FROM_CLIENT_MATCH_DURATION_INDEX])
    )


def get_filter_by_duration_average_rating_and_server_function(output_queue):
    def filter_by_duration_average_rating_and_server(queue, received_string, _):
        matches_ids = []
        for row in split_rows_into_list(received_string):
            columns = split_columns_into_list(row)
            if is_matched(columns):
                matches_ids.append(columns[FROM_CLIENT_MATCH_TOKEN_INDEX])
        output_queue.send_list_as_rows(matches_ids)
    return filter_by_duration_average_rating_and_server

def main():
    connection = RabbitMQConnection()
    input_exchage = ExchangeInterface.newFanout(
        connection, MATCHES_FANOUT_EXCHANGE_NAME)
    input_queue = QueueInterface.newPrivate(connection)
    input_queue.bind(input_exchage)
    
    output_queue = QueueInterface(
        connection, LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    logger.info('Starting to receive matches to filter')
    input_queue.consume(
        get_filter_by_duration_average_rating_and_server_function(output_queue),
        get_on_sentinel_send_sentinel_callback_function(output_queue)
    )
    connection.close()

if __name__ == '__main__':
    main()
