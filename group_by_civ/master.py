from config.envvars import BARRIER_QUEUE_NAME_KEY, OUTPUT_EXCHANGE_NAME_KEY, PLAYERS_CHUNK_SIZE_KEY, PLAYERS_INPUT_QUEUE_NAME_KEY, REDUCERS_OUTPUT_QUEUE_NAME_KEY, get_config_param
from communications.constants import FROM_CLIENT_PLAYER_MATCH_INDEX
from communications.rabbitmq_interface import QueueInterface, split_columns_into_list, split_rows_into_list
from master_reducers_arq.master import main_master
from logger.logger import Logger

logger = Logger()
PLAYERS_CHUNK_SIZE = get_config_param(PLAYERS_CHUNK_SIZE_KEY, logger)

def send_players_by_key(output_exchange, players_by_key, check_chunk_size=True):
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
    def on_sentinel_callback(_):
        # send the remaining players
        send_players_by_key(output_exchange, players_by_key, False)
    return on_sentinel_callback


def get_dispach_to_reducers_function(output_exchange, players_by_key, partition_function):
    def dispach_to_reducers(queue, received_string, _):
        received_players = [
            player_string for player_string in split_rows_into_list(received_string)]
        add_to_players_by_key(
            output_exchange,
            partition_function,
            players_by_key,
            received_players
        )
    return dispach_to_reducers


def receive_and_dispach_players(entry_queue, output_exchange, partition_function):
    players_by_key = {}
    logger.info(
        "Starting to receive players from client and dispach it to reducers by key")
    entry_queue.consume(
        get_dispach_to_reducers_function(
            output_exchange, players_by_key, partition_function
        ),
        on_sentinel_callback=get_on_sentinel_callback_function(
            output_exchange, players_by_key
        )
    )


def declare_input_queue(connection):
    return QueueInterface(
        connection,
        get_config_param(PLAYERS_INPUT_QUEUE_NAME_KEY, logger)
    )

def main():
    main_master(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(REDUCERS_OUTPUT_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_EXCHANGE_NAME_KEY, logger),
        declare_input_queue,
        receive_and_dispach_players
    )


if __name__ == '__main__':
    main()
