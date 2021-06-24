from config.envvars import BARRIER_QUEUE_NAME_KEY, INPUT_EXCHANGE_NAME_KEY, KEYS_QUEUE_NAME_KEY, OUTPUT_QUEUE_NAME_KEY, get_config_param
from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR # TODO ver como evitar imports de este y el de arriba
from master_reducers_arq.reducer import main_reducer
from logger.logger import Logger

logger = Logger()

MATCH_PRESENT = 1

def find_received_players_by_matches(output_queue, players_rows, players_by_match, matches):
    players_to_send = []
    for player_string in players_rows:
        player_columns = player_string.split(
             STRING_COLUMN_SEPARATOR)
        match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
        # match no represent in matches
        if matches.get(match_id, None) is None:
            # store players and wait that match to arrive
            players_of_match = players_by_match.get(match_id, [])
            players_of_match.append(player_columns)
            players_by_match[match_id] = players_of_match
        else:
            players_to_send.append(player_columns)
    output_queue.send_list_of_columns(players_to_send)


def find_players_by_received_matches(output_queue, matches_rows, players_by_match, matches):
    players_to_send = []
    for match_string in matches_rows:
        match_columns = match_string.split(
            STRING_COLUMN_SEPARATOR)
        match_id = match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX]
        # store match and wait that players of that match to arrive
        matches[match_id] = MATCH_PRESENT
        players_of_that_match = players_by_match.pop(match_id, None)
        # allready players for that match
        if players_of_that_match is not None:
            players_to_send += players_of_that_match
    output_queue.send_list_of_columns(players_to_send)


def get_filter_players_in_matches_function(players_by_match, matches, output_queue):
    # python function currying
    def filter_players_in_matches(queue, received_string, _):
        chunk_rows = received_string.split(STRING_LINE_SEPARATOR)
        identificator = chunk_rows.pop(
            JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX)
        if identificator == JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR:
            find_received_players_by_matches(
                output_queue, chunk_rows, players_by_match, matches)
        elif identificator == JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR:
            find_players_by_received_matches(
                output_queue, chunk_rows, players_by_match, matches)
    return filter_players_in_matches

def join_players_and_matches(input_queue, output_queue, keys):
    players_by_match = {}
    matches = {}

    print(f'Starting to receive players and matches in matches with keys {keys} to join them.')
    input_queue.consume(
        get_filter_players_in_matches_function(
            players_by_match, matches, output_queue
        )
    )

    print(f'All players and matches in matches with keys {keys} joined.')
    return players_by_match


def main():
    main_reducer(
        get_config_param(KEYS_QUEUE_NAME_KEY, logger),
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(INPUT_EXCHANGE_NAME_KEY, logger),
        get_config_param(OUTPUT_QUEUE_NAME_KEY, logger),
        join_players_and_matches
    )


if __name__ == '__main__':
    main()
