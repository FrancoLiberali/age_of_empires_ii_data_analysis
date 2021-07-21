from communications.rabbitmq_interface import split_columns_into_list, split_rows_into_list
from config.envvars import MINIMUM_RATING_KEY, MINIMUM_RATING_PORCENTAGE_DIFF_KEY, get_config_param
from communications.constants import FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_RATING_INDEX, \
    FROM_CLIENT_PLAYER_WINNER_INDEX, \
    GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, \
    PLAYER_LOSER, \
    PLAYER_WINNER, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from master_reducers_arq.reducer import main_reducer
from logger.logger import Logger

logger = Logger()

BARRIER_QUEUE_NAME = GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME
OUTPUT_QUEUE_NAME = WEAKER_WINNER_TO_CLIENT_QUEUE_NAME

def can_match_be_1_vs_1(players_list, new_player):
    return (players_list is not None and (len(players_list) == 0 or (len(players_list) == 1 and players_list[0][FROM_CLIENT_PLAYER_WINNER_INDEX] != new_player[FROM_CLIENT_PLAYER_WINNER_INDEX])))

def get_group_by_match_function(players_by_match):
    # python function currying
    def group_by_match(queue, received_string, _):
        for player_string in split_rows_into_list(received_string):
            player_columns = split_columns_into_list(player_string)
            match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
            players_of_match = players_by_match.get(match_id, [])
            if can_match_be_1_vs_1(players_of_match, player_columns):
                players_of_match.append(player_columns)
                players_by_match[match_id] = players_of_match
            else:
                del players_by_match[match_id]
                players_by_match[match_id] = None # mark this match as not possible
    return group_by_match


def process_player_by_match(input_queue, output_queue):
    players_by_match = {}
    logger.info(f'Starting to receive players in matches to group them.')
    input_queue.consume(
        get_group_by_match_function(players_by_match)
    )

    logger.info(f'All players in matches grouped.')
    return players_by_match


def filter_players_by_weaker_winner(players_by_match):
    minimun_rating = get_config_param(MINIMUM_RATING_KEY, logger)
    minimun_rating_porcentage_diff = get_config_param(
        MINIMUM_RATING_PORCENTAGE_DIFF_KEY, logger)
    matches_ids = []
    for match_id, players_list in players_by_match.items():
        # final check that all matches are of two players
        if players_list is not None and len(players_list) == 2:
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


def send_matches_ids_to_client(output_queue, players_by_match):
    matches_ids = filter_players_by_weaker_winner(players_by_match)
    logger.info(f"All matches with weaker winner found")

    output_queue.send_list_as_rows(matches_ids)

def main():
    main_reducer(
        BARRIER_QUEUE_NAME,
        OUTPUT_QUEUE_NAME,
        process_player_by_match,
        send_matches_ids_to_client
    )

if __name__ == '__main__':
    main()
