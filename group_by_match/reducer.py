import os
from communications.file import ListOfJsonFile
from communications.rabbitmq_interface import SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR, split_columns_into_list, split_rows_into_list
from config.envvars import MINIMUM_RATING_KEY, MINIMUM_RATING_PORCENTAGE_DIFF_KEY, REDUCER_ID_KEY, get_config_param
from communications.constants import FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_RATING_INDEX, \
    FROM_CLIENT_PLAYER_WINNER_INDEX, \
    GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, \
    PLAYER_LOSER, \
    PLAYER_WINNER, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from master_reducers_arq.reducer import main_reducer, player_already_exists_in_list
from logger.logger import Logger

logger = Logger()

BARRIER_QUEUE_NAME = GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME
OUTPUT_QUEUE_NAME = WEAKER_WINNER_TO_CLIENT_QUEUE_NAME

HEADER_LINE = f"{get_config_param(REDUCER_ID_KEY, logger)}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}"
NOT_POSSIBLE = None
MATCH_ID_INDEX = 0
NOT_POSSIBLE_INDEX = 1

def can_match_be_1_vs_1(players_list, new_player):
    return (
        len(players_list) == 0 or
        (
            len(players_list) == 1 and
            players_list[0][FROM_CLIENT_PLAYER_WINNER_INDEX] != new_player[FROM_CLIENT_PLAYER_WINNER_INDEX]
        )
    )


def get_group_by_match_function(players_by_match, players_file):
    # python function currying
    minimun_rating = get_config_param(MINIMUM_RATING_KEY, logger)
    def group_by_match(queue, received_string, _):
        players_to_add = []
        for player_string in split_rows_into_list(received_string):
            player_columns = split_columns_into_list(player_string)
            match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
            players_of_match = players_by_match.get(match_id, [])
            if players_of_match is not NOT_POSSIBLE:
                player_already_exists = player_already_exists_in_list(player_columns, players_of_match)
                if not player_already_exists:
                    can_be_valid = (
                        player_columns[FROM_CLIENT_PLAYER_RATING_INDEX] != '' and
                        (
                            player_columns[FROM_CLIENT_PLAYER_WINNER_INDEX] == PLAYER_LOSER or
                            int(player_columns[FROM_CLIENT_PLAYER_RATING_INDEX]) > minimun_rating
                        ) and
                        can_match_be_1_vs_1(players_of_match, player_columns)
                    )
                    if can_be_valid:
                        players_to_add.append(player_columns)

                        players_of_match.append(player_columns)
                        players_by_match[match_id] = players_of_match
                    else:
                        # mark this match as not possible
                        players_by_match[match_id] = NOT_POSSIBLE
                        players_to_add.append([match_id, NOT_POSSIBLE])
        players_file.write(players_to_add)
    return group_by_match

STATE_STORAGE_DIR = "/data/"
PLAYERS_LIST_FILE_NAME = "players_list.txt"

def process_player_by_match(input_queue, output_queue, send_sentinel_to_master_function):
    players_by_match = {}
    players_list_file = ListOfJsonFile(
        STATE_STORAGE_DIR,
        PLAYERS_LIST_FILE_NAME
    )
    for player_columns in players_list_file.content:
        if player_columns[NOT_POSSIBLE_INDEX] == NOT_POSSIBLE:
            players_by_match[player_columns[MATCH_ID_INDEX]] = NOT_POSSIBLE
        else:
            match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
            players_of_match = players_by_match.get(
                match_id, [])
            players_of_match.append(player_columns)
            players_by_match[match_id] = players_of_match

    logger.info(f'Starting to receive players in matches to group them.')
    input_queue.consume(
        get_group_by_match_function(players_by_match, players_list_file),
        on_sentinel_callback=get_send_matches_ids_to_client_function(
            output_queue, players_by_match, players_list_file, send_sentinel_to_master_function
        )
    )

def filter_players_by_weaker_winner(players_by_match):
    minimun_rating = get_config_param(MINIMUM_RATING_KEY, logger)
    minimun_rating_porcentage_diff = get_config_param(
        MINIMUM_RATING_PORCENTAGE_DIFF_KEY, logger)
    matches_ids = []
    for match_id, players_list in players_by_match.items():
        # final check that all matches are of two players
        if players_list is not NOT_POSSIBLE and len(players_list) == 2:
            winner = next(
                (player for player in players_list if player[FROM_CLIENT_PLAYER_WINNER_INDEX] == PLAYER_WINNER))
            loser = next(
                (player for player in players_list if player[FROM_CLIENT_PLAYER_WINNER_INDEX] == PLAYER_LOSER))

            if winner[FROM_CLIENT_PLAYER_RATING_INDEX] != '' and loser[FROM_CLIENT_PLAYER_RATING_INDEX] != '':
                winner_rating = int(winner[FROM_CLIENT_PLAYER_RATING_INDEX])
                loser_rating = int(loser[FROM_CLIENT_PLAYER_RATING_INDEX])
                rating_diff = (loser_rating - winner_rating) / \
                    winner_rating * 100
                if winner_rating > minimun_rating and rating_diff > minimun_rating_porcentage_diff:
                    matches_ids.append(match_id)
    return matches_ids


def get_send_matches_ids_to_client_function(output_queue, players_by_match, players_list_file, send_sentinel_to_master_function):
    def send_matches_ids_to_client(_, __):
        logger.info(f'All players in matches grouped.')
        matches_ids = filter_players_by_weaker_winner(players_by_match)
        logger.info(f"All matches with weaker winner found")

        output_queue.send_list_as_rows(
            matches_ids,
            header_line=HEADER_LINE
        )

        send_sentinel_to_master_function(_, __)
        players_list_file.close()
        os.remove(STATE_STORAGE_DIR + PLAYERS_LIST_FILE_NAME)

    return send_matches_ids_to_client

def main():
    main_reducer(
        BARRIER_QUEUE_NAME,
        OUTPUT_QUEUE_NAME,
        process_player_by_match
    )

if __name__ == '__main__':
    main()
