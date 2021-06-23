import os

from communications.constants import MATCHES_KEY, \
    FROM_CLIENT_MATCH_TOKEN_INDEX, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_FANOUT_EXCHANGE_NAME
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, RabbitMQConnection


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

def add_to_matches(matches_list, match_columns):
    matches_list.append(
        [
            match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX],
        ]
    )


def get_on_sentinel_callback_function(output_1v1_exchage, output_team_exchage):
    def on_sentinel_callback():
        print(
            "Sending sentinel to next stage to notify that all matches ids has been sended")
        output_1v1_exchage.send_sentinel(MATCHES_KEY)
        output_team_exchage.send_sentinel(MATCHES_KEY)
    return on_sentinel_callback

def get_filter_by_ladder_and_map_function(output_1v1_exchage, output_team_exchage):
    def filter_by_ladder_and_map(queue, received_string, _):
        matches_1v1_matched = []
        matches_team_matched = []
        for row in received_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched_1v1(columns):
                add_to_matches(matches_1v1_matched, columns)
            elif is_matched_team(columns):
                add_to_matches(matches_team_matched, columns)
        output_1v1_exchage.send_list_of_columns(matches_1v1_matched, MATCHES_KEY)
        output_team_exchage.send_list_of_columns(matches_team_matched, MATCHES_KEY)
    return filter_by_ladder_and_map


def main():
    # TODO codigo repetido con long matches
    connection = RabbitMQConnection()

    input_exchage = ExchangeInterface.newFanout(
        connection, MATCHES_FANOUT_EXCHANGE_NAME)

    input_queue = QueueInterface.newPrivate(connection)
    input_queue.bind(input_exchage)
    
    output_1v1_exchage = ExchangeInterface.newDirect(
        connection, OUTPUT_EXCHANGE_NAME_1V1)
    output_team_exchage = ExchangeInterface.newDirect(
        connection, OUTPUT_EXCHANGE_NAME_TEAM)

    print(f'Starting to receive matches to filter by ladder, map and mirror')
    input_queue.consume(
        get_filter_by_ladder_and_map_function(
            output_1v1_exchage,
            output_team_exchage
        ),
        get_on_sentinel_callback_function(
            output_1v1_exchage,
            output_team_exchage
        ),
    )

    connection.close()


if __name__ == '__main__':
    main()
