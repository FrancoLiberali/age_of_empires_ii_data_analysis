#!/usr/bin/env python3
from datetime import datetime, time

from communications.constants import FROM_CLIENT_MATCH_AVERAGE_RATING_INDEX, \
    FROM_CLIENT_MATCH_DURATION_INDEX, \
    FROM_CLIENT_MATCH_SERVER_INDEX, \
    FROM_CLIENT_MATCH_TOKEN_INDEX, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_FANOUT_EXCHANGE_NAME, \
    LONG_MATCHES_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection, get_on_sentinel_send_sentinel_callback_function

MINIMUM_AVERAGE_RATING = 2000 # TODO envvar
MINIMUM_DURATION = time(hour=2)  # TODO envvar
REQUIRED_SERVERS = ['koreacentral', 'southeastasia', 'eastus']  # TODO envvar
DURATION_FORMAT = '%H:%M:%S' # TODO envvar

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
            # TODO log error
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
        for row in received_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched(columns):
                matches_ids.append(columns[FROM_CLIENT_MATCH_TOKEN_INDEX])
        output_queue.send_matches_ids(matches_ids)
    return filter_by_duration_average_rating_and_server

def main():
    connection = RabbitMQConnection()
    input_exchage = ExchangeInterface.newFanout(
        connection, MATCHES_FANOUT_EXCHANGE_NAME)
    input_queue = QueueInterface.newPrivate(connection)
    input_queue.bind(input_exchage)
    
    output_queue = QueueInterface(
        connection, LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    print('Starting to receive matches to filter')
    input_queue.consume(
        get_filter_by_duration_average_rating_and_server_function(output_queue),
        get_on_sentinel_send_sentinel_callback_function(output_queue)
    )
    connection.close()

if __name__ == '__main__':
    main()
