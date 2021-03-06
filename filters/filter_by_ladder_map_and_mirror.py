from config.envvars import LADDER_1V1_KEY, LADDER_TEAM_KEY, MAP_ARENA_KEY, MAP_ISLANDS_KEY, NO_MIRROR_KEY, OUTPUT_EXCHANGE_NAME_1V1_KEY, OUTPUT_EXCHANGE_NAME_TEAM_KEY, get_config_params
from communications.constants import FILTER_BY_LADDER_MAP_AND_MIRROR_QUEUE_NAME, \
    MATCHES_KEY, \
    FROM_CLIENT_MATCH_TOKEN_INDEX, \
    MATCHES_FANOUT_EXCHANGE_NAME
from communications.rabbitmq_interface import ExchangeInterface, LastHashStrategy, QueueInterface, RabbitMQConnection, split_columns_into_list, split_rows_into_list
import healthcheck.server
from logger.logger import Logger

logger = Logger()
CONFIG_1V1 = get_config_params([
        LADDER_1V1_KEY,
        MAP_ARENA_KEY,
        NO_MIRROR_KEY,
    ], logger)

CONFIG_TEAM = get_config_params([
    LADDER_TEAM_KEY,
    MAP_ISLANDS_KEY,
], logger)

LADDER_INDEX_INDEX = 4
MAP_INDEX = 5
MIRROR_INDEX = 6

def is_matched_1v1(columns):
    return (
        columns[LADDER_INDEX_INDEX] == CONFIG_1V1[LADDER_1V1_KEY] and
        columns[MAP_INDEX] == CONFIG_1V1[MAP_ARENA_KEY] and
        columns[MIRROR_INDEX] == CONFIG_1V1[NO_MIRROR_KEY]
    )


def is_matched_team(columns):
    return (
        columns[LADDER_INDEX_INDEX] == CONFIG_TEAM[LADDER_TEAM_KEY] and
        columns[MAP_INDEX] == CONFIG_TEAM[MAP_ISLANDS_KEY]
    )

def add_to_matches(matches_list, match_columns):
    matches_list.append(
        [
            match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX],
        ]
    )


def get_on_sentinel_callback_function(output_1v1_exchage, output_team_exchage):
    def on_sentinel_callback(_, __):
        logger.info(
            "Sending sentinel to next stage to notify that all matches ids has been sended")
        output_1v1_exchage.send_sentinel(MATCHES_KEY)
        output_team_exchage.send_sentinel(MATCHES_KEY)
    return on_sentinel_callback

def get_filter_by_ladder_map_and_mirror_function(output_1v1_exchage, output_team_exchage):
    def filter_by_ladder_map_and_mirror(queue, received_string, _, __):
        matches_1v1_matched = []
        matches_team_matched = []
        for row in split_rows_into_list(received_string):
            columns = split_columns_into_list(row)
            if is_matched_1v1(columns):
                add_to_matches(matches_1v1_matched, columns)
            elif is_matched_team(columns):
                add_to_matches(matches_team_matched, columns)
        output_1v1_exchage.send_list_of_columns(matches_1v1_matched, MATCHES_KEY)
        output_team_exchage.send_list_of_columns(matches_team_matched, MATCHES_KEY)
    return filter_by_ladder_map_and_mirror


def main():
    healthcheck.server.start_in_new_process()
    output_exchanges = get_config_params([
        OUTPUT_EXCHANGE_NAME_1V1_KEY,
        OUTPUT_EXCHANGE_NAME_TEAM_KEY,
    ], logger)
    connection = RabbitMQConnection()

    input_exchage = ExchangeInterface.newFanout(
        connection, MATCHES_FANOUT_EXCHANGE_NAME)

    input_queue = QueueInterface(
        connection,
        FILTER_BY_LADDER_MAP_AND_MIRROR_QUEUE_NAME,
        LastHashStrategy.NO_LAST_HASH_SAVING
    )
    input_queue.bind(input_exchage)

    output_1v1_exchage = ExchangeInterface.newDirect(
        connection, output_exchanges[OUTPUT_EXCHANGE_NAME_1V1_KEY])
    output_team_exchage = ExchangeInterface.newDirect(
        connection, output_exchanges[OUTPUT_EXCHANGE_NAME_TEAM_KEY])

    while True:
        logger.info(
            f'Starting to receive matches to filter by ladder, map and mirror')
        input_queue.consume(
            get_filter_by_ladder_map_and_mirror_function(
                output_1v1_exchage,
                output_team_exchage
            ),
            on_sentinel_callback=get_on_sentinel_callback_function(
                output_1v1_exchage,
                output_team_exchage
            ),
        )


if __name__ == '__main__':
    main()
