from communications.rabbitmq_interface import SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR, split_columns_into_list, split_rows_into_list
from config.envvars import BARRIER_QUEUE_NAME_KEY, OUTPUT_QUEUE_NAME_KEY, REDUCER_ID_KEY, get_config_param
from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR
from master_reducers_arq.reducer import main_reducer
from logger.logger import Logger

logger = Logger()

MATCH_PRESENT = 1

HEADER_LINE = f"{get_config_param(REDUCER_ID_KEY, logger)}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}"

def find_received_players_by_matches(output_queue, players_rows, players_by_match, matches):
    players_to_send = []
    for player_string in players_rows:
        player_columns = split_columns_into_list(player_string)
        match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
        # match no represent in matches
        if matches.get(match_id, None) is None:
            # store players and wait that match to arrive
            players_of_match = players_by_match.get(match_id, [])
            players_of_match.append(player_columns)
            players_by_match[match_id] = players_of_match
        else:
            players_to_send.append(player_columns)
    output_queue.send_list_of_columns(
        players_to_send,
        header_line=HEADER_LINE
    )


def find_players_by_received_matches(output_queue, matches_rows, players_by_match, matches):
    players_to_send = []
    for match_string in matches_rows:
        match_columns = split_columns_into_list(match_string)
        match_id = match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX]
        # store match and wait that players of that match to arrive
        matches[match_id] = MATCH_PRESENT
        players_of_that_match = players_by_match.pop(match_id, None)
        # allready players for that match
        if players_of_that_match is not None:
            players_to_send += players_of_that_match
    output_queue.send_list_of_columns(
        players_to_send,
        header_line=HEADER_LINE
    )


def get_filter_players_in_matches_function(players_by_match, matches, output_queue):
    # python function currying
    def filter_players_in_matches(queue, received_string, _):
        chunk_rows = split_rows_into_list(received_string)
        identificator = chunk_rows.pop(
            JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX)
        if identificator == JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR:
            find_received_players_by_matches(
                output_queue, chunk_rows, players_by_match, matches)
        elif identificator == JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR:
            find_players_by_received_matches(
                output_queue, chunk_rows, players_by_match, matches)
    return filter_players_in_matches

def join_players_and_matches(input_queue, output_queue):
    players_by_match = {}
    matches = {}

    logger.info(f'Starting to receive players and matches in matches to join them.')
    input_queue.consume(
        get_filter_players_in_matches_function(
            players_by_match, matches, output_queue
        )
    )

    logger.info(f'All players and matches in matches joined.')
    return players_by_match


def main():
    main_reducer(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_QUEUE_NAME_KEY, logger),
        join_players_and_matches
    )


if __name__ == '__main__':
    main()
