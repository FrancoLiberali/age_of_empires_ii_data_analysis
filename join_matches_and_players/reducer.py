import os
from more_itertools import first_true

from communications.file import ListFile, ListOfJsonFile
from communications.rabbitmq_interface import SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR, split_columns_into_list, split_rows_into_list
from config.envvars import BARRIER_QUEUE_NAME_KEY, OUTPUT_QUEUE_NAME_KEY, REDUCER_ID_KEY, get_config_param
from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_TOKEN_INDEX, \
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
    players_to_add = {}
    for player_string in players_rows:
        player_columns = split_columns_into_list(player_string)
        match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
        # match no represent in matches
        if matches.get(match_id, None) is None:
            # store players and wait that match to arrive
            players_of_match = players_by_match.get(match_id, [])
            player_already_exists = first_true(
                players_of_match,
                pred=lambda player: player[FROM_CLIENT_PLAYER_TOKEN_INDEX] == player_columns[FROM_CLIENT_PLAYER_TOKEN_INDEX]
            ) is not None
            if not player_already_exists:
                players_to_add_of_match = players_to_add.get(match_id, [])
                players_to_add_of_match.append(player_columns)
                players_to_add[match_id] = players_to_add_of_match

                players_of_match.append(player_columns)
                players_by_match[match_id] = players_of_match
            else:
                logger.debug(f"Duplicated player found, players_of_match: {players_of_match}, player_columns: {player_columns}")
        else:
            players_to_send.append(player_columns)
    return players_to_send, players_to_add


def find_players_by_received_matches(output_queue, matches_rows, players_by_match, matches):
    players_to_send = []
    matches_to_add = []
    for match_string in matches_rows:
        match_columns = split_columns_into_list(match_string)
        match_id = match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX]
        if matches.get(match_id, None) is None:
            # store match and wait that players of that match to arrive
            matches_to_add.append(match_id)
            matches[match_id] = MATCH_PRESENT
            players_of_that_match = players_by_match.pop(match_id, None)
            # allready players for that match
            if players_of_that_match is not None:
                players_to_send += players_of_that_match
    return players_to_send, matches_to_add

PLAYERS_LIST_FILE_NAME = "players_list.txt"

def get_players_list_dir(match_id):
    return PLAYERS_STORAGE_DIR + match_id + "/"

def add_matches_received(matches_file, new_matches_ids):
    matches_file.write(new_matches_ids)
    for match_id in new_matches_ids:
        player_of_match_dir = get_players_list_dir(match_id)
        # create dir to store players of that match
        os.makedirs(os.path.dirname(player_of_match_dir), exist_ok=True)
        # open file in w mode
        # if the file doesnt exist create it (no players for this match arrived yet)
        # if the file exists clear it (players already arrived but cleared to send to next stage)
        open(player_of_match_dir + PLAYERS_LIST_FILE_NAME, "w").close()


def add_players_received(players_to_add):
    for match_id, players in players_to_add.items():
        player_of_match_dir = get_players_list_dir(match_id)
        players_file = ListOfJsonFile(
            player_of_match_dir, PLAYERS_LIST_FILE_NAME, read_content=False
        )
        players_file.write(players)
        players_file.close()

def get_filter_players_in_matches_function(matches_file, players_by_match, matches, output_queue):
    # python function currying
    def filter_players_in_matches(queue, received_string, _):
        chunk_rows = split_rows_into_list(received_string)
        identificator = chunk_rows.pop(
            JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX)
        if identificator == JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR:
            players_to_send, players_to_add = find_received_players_by_matches(
                output_queue, chunk_rows, players_by_match, matches)
            add_players_received(players_to_add)
        elif identificator == JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR:
            players_to_send, matches_to_add = find_players_by_received_matches(
                output_queue, chunk_rows, players_by_match, matches)
            add_matches_received(matches_file, matches_to_add)

        output_queue.send_list_of_columns(
            players_to_send,
            header_line=HEADER_LINE
        )
    return filter_players_in_matches

STATE_STORAGE_DIR = "/data/"
PLAYERS_STORAGE_DIR = STATE_STORAGE_DIR + "players/"
MATCHES_FILE_NAME = "matches.txt"

def join_players_and_matches(input_queue, output_queue):
    players_by_match = {}

    matches_file = ListFile(
        STATE_STORAGE_DIR,
        MATCHES_FILE_NAME
    )
    matches = {}
    for match_id in matches_file.content:
        matches[match_id] = MATCH_PRESENT
        player_of_match_dir = get_players_list_dir(match_id)
        players_file = ListOfJsonFile(
            player_of_match_dir, PLAYERS_LIST_FILE_NAME
        )
        players_by_match[match_id] = players_file.content
        players_file.close()
    logger.debug(f"Initial matches size: {len(matches.keys())}")

    logger.info('Starting to receive players and matches in matches to join them.')
    input_queue.consume(
        get_filter_players_in_matches_function(
            matches_file, players_by_match, matches, output_queue
        )
    )

    logger.info(f'All players and matches in matches joined.')
    matches_file.close()
    return players_by_match


def main():
    main_reducer(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_QUEUE_NAME_KEY, logger),
        join_players_and_matches
    )


if __name__ == '__main__':
    main()
