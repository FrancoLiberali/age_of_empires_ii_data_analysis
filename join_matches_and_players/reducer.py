import os
import shutil

from communications.file import BooleanFile, ListFile, ListOfJsonFile
from communications.rabbitmq_interface import SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR, split_columns_into_list, split_rows_into_list
from config.envvars import BARRIER_QUEUE_NAME_KEY, OUTPUT_QUEUE_NAME_KEY, REDUCER_ID_KEY, get_config_param
from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, MATCHES_SENTINEL
from master_reducers_arq.reducer import main_reducer, player_already_exists_in_list
from logger.logger import Logger

logger = Logger()

MATCH_PRESENT = 1

HEADER_LINE = f"{get_config_param(REDUCER_ID_KEY, logger)}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}"


def find_received_players_by_matches(players_rows, players_by_match, matches, no_more_matches):
    players_to_send = []
    players_to_add = {}
    for player_string in players_rows:
        player_columns = split_columns_into_list(player_string)
        match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
        match_is_present = matches.get(match_id, None) is not None
        # match no represent in matches
        if not match_is_present and not no_more_matches:
            # store players and wait that match to arrive
            players_of_match = players_by_match.get(match_id, [])
            player_already_exists = player_already_exists_in_list(
                player_columns, players_of_match)
            if not player_already_exists:
                players_to_add_of_match = players_to_add.get(match_id, [])
                players_to_add_of_match.append(player_columns)
                players_to_add[match_id] = players_to_add_of_match

                players_of_match.append(player_columns)
                players_by_match[match_id] = players_of_match
            else:
                logger.debug(f"Duplicated player found, players_of_match: {players_of_match}, player_columns: {player_columns}")
        elif match_is_present:
            players_to_send.append(player_columns)
    return players_to_send, players_to_add


def find_players_by_received_matches(matches_rows, players_by_match, matches):
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
        else:
            logger.debug(f"Duplicated match found, match_id: {match_id}")
    return players_to_send, matches_to_add

def add_matches_received(matches_file, new_matches_ids):
    matches_file.write(new_matches_ids)
    for match_id in new_matches_ids:
        try:
            os.remove(PLAYERS_STORAGE_DIR + match_id)
        except FileNotFoundError:
            pass


def add_players_received(players_to_add):
    for match_id, players in players_to_add.items():
        players_file = ListOfJsonFile(
            PLAYERS_STORAGE_DIR, match_id, read_content=False
        )
        players_file.write(players)
        players_file.close()

NO_MORE_MATCHES_FILE_NAME = "no_more_matches.txt"

def get_filter_players_in_matches_function(matches_file, players_by_match, matches, output_queue):
    no_more_matches_file = BooleanFile(
        STATE_STORAGE_DIR, NO_MORE_MATCHES_FILE_NAME)
    no_more_matches = [no_more_matches_file.content]
    # python function currying
    def filter_players_in_matches(queue, received_string, _, __):
        if received_string == MATCHES_SENTINEL:
            logger.info("No more matches are comming, stoping saving players until it match come")
            no_more_matches[0] = True
            no_more_matches_file.write(True)
            # delete all players stored, its match will never come
            shutil.rmtree(PLAYERS_STORAGE_DIR, ignore_errors=True)
        else:
            chunk_rows = split_rows_into_list(received_string)
            identificator = chunk_rows.pop(
                JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX)
            if identificator == JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR:
                players_to_send, players_to_add = find_received_players_by_matches(
                    chunk_rows, players_by_match, matches, no_more_matches[0])
                output_queue.send_list_of_columns(
                    players_to_send,
                    header_line=HEADER_LINE
                )
                add_players_received(players_to_add)
            elif identificator == JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR:
                players_to_send, matches_to_add = find_players_by_received_matches(
                    chunk_rows, players_by_match, matches)
                output_queue.send_list_of_columns(
                    players_to_send,
                    header_line=HEADER_LINE
                )
                add_matches_received(matches_file, matches_to_add)
    return filter_players_in_matches


def get_on_sentinel_callback_function(send_sentinel_to_master_function):
    def on_sentinel_callback(_, __):
        shutil.rmtree(STATE_STORAGE_DIR, ignore_errors=True)
        send_sentinel_to_master_function(_, __)
    return on_sentinel_callback

STATE_STORAGE_DIR = "/data/"
PLAYERS_STORAGE_DIR = STATE_STORAGE_DIR + "players/"
MATCHES_FILE_NAME = "matches.txt"

def get_initial_matches_and_players(matches_file):
    matches = {}
    for match_id in matches_file.content:
        matches[match_id] = MATCH_PRESENT

    players_by_match = {}
    os.makedirs(os.path.dirname(PLAYERS_STORAGE_DIR), exist_ok=True)
    for match_id in os.listdir(PLAYERS_STORAGE_DIR):
        players_file = ListOfJsonFile(
            PLAYERS_STORAGE_DIR, match_id
        )
        players_by_match[match_id] = players_file.content
        players_file.close()
    logger.debug(f"Initial matches size: {len(matches.keys())}")
    logger.debug(f"Initial players size: {len(players_by_match.keys())}")
    return matches, players_by_match


def join_players_and_matches(input_queue, output_queue, send_sentinel_to_master_function):
    matches_file = ListFile(
        STATE_STORAGE_DIR,
        MATCHES_FILE_NAME
    )
    matches, players_by_match = get_initial_matches_and_players(matches_file)

    logger.info('Starting to receive players and matches in matches to join them.')
    input_queue.consume(
        get_filter_players_in_matches_function(
            matches_file, players_by_match, matches, output_queue
        ),
        on_sentinel_callback=get_on_sentinel_callback_function(
            send_sentinel_to_master_function
        )
    )

    logger.info(f'All players and matches in matches joined.')
    matches_file.close()
    return players_by_match

def main():
    main_reducer(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_QUEUE_NAME_KEY, logger),
        join_players_and_matches,
    )


if __name__ == '__main__':
    main()
