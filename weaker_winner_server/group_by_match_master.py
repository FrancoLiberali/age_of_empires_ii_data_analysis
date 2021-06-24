from config.envvars import PLAYERS_CHUNK_SIZE_KEY, get_config_param
from communications.constants import FROM_CLIENT_PLAYER_MATCH_INDEX, \
    GROUP_BY_MATCH_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME, \
    GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, split_columns_into_list, split_rows_into_list
from master_reducers_arq.master import main_master
from logger.logger import Logger

logger = Logger()
PLAYERS_CHUNK_SIZE = get_config_param(PLAYERS_CHUNK_SIZE_KEY, logger)


def send_players_by_key(output_exchange, players_by_key, check_chunk_size=True):
    # TODO codigo repetido con group by civ
    for key, players in list(players_by_key.items()):
        if len(players) > PLAYERS_CHUNK_SIZE or not check_chunk_size:
            output_exchange.send_list_as_rows(players, key)
            players_by_key.pop(key)
            del players


def add_to_players_by_key(output_exchange, partition_function, players_by_key, received_players):
    for player_string in received_players:
        key = partition_function.get_key(
            split_columns_into_list(player_string)[FROM_CLIENT_PLAYER_MATCH_INDEX]
        )
        rows_list = players_by_key.get(key, [])
        rows_list.append(player_string)
        players_by_key[key] = rows_list

    send_players_by_key(output_exchange, players_by_key)


def get_on_sentinel_callback_function(output_exchange, players_by_key):
    # TODO codigo repetido con group by civ
    def on_sentinel_callback():
        # send the remaining players
        send_players_by_key(output_exchange, players_by_key, False)
    return on_sentinel_callback

def get_dispach_to_reducers_function(output_exchange, players_by_key, partition_function):
    def dispach_to_reducers(queue, received_string, _):
        received_players = [
            player_string for player_string in split_rows_into_list(received_string)
        ]
        add_to_players_by_key(
            output_exchange,
            partition_function,
            players_by_key,
            received_players
        )
    return dispach_to_reducers


def receive_and_dispach_players(entry_queue, output_exchange, partition_function):
    players_by_key = {}
    logger.info("Starting to receive players from client and dispach it to reducers by key")
    entry_queue.consume(
        get_dispach_to_reducers_function(
            output_exchange, players_by_key, partition_function
        ),
        get_on_sentinel_callback_function(
            output_exchange, players_by_key
        )
    )


def subscribe_to_entries(connection):
    input_exchage = ExchangeInterface.newFanout(
        connection, PLAYERS_FANOUT_EXCHANGE_NAME)
    input_queue = QueueInterface.newPrivate(connection)
    input_queue.bind(input_exchage)

    return input_queue


def main():
    main_master(
        GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME,
        GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME,
        WEAKER_WINNER_TO_CLIENT_QUEUE_NAME,
        GROUP_BY_MATCH_MASTER_TO_REDUCERS_EXCHANGE_NAME,
        subscribe_to_entries,
        receive_and_dispach_players
    )

if __name__ == '__main__':
    main()
