from communications.constants import FROM_CLIENT_PLAYER_MATCH_INDEX
from communications.rabbitmq_interface import split_rows_into_list
from master_reducers_arq.master import add_to_dict_by_key, main_master, send_dict_by_key
from logger.logger import Logger

logger = Logger()

def get_on_sentinel_callback_function(output_exchange, players_by_key):
    def on_sentinel_callback(_, __):
        # send the remaining players
        send_dict_by_key(output_exchange, players_by_key,
                         check_chunk_size=False)
    return on_sentinel_callback


def get_dispach_to_reducers_function(output_exchange, players_by_key, partition_function, skip_header):
    def dispach_to_reducers(queue, received_string, _):
        received_players = split_rows_into_list(
            received_string, skip_header=skip_header)
        add_to_dict_by_key(
            output_exchange,
            partition_function,
            players_by_key,
            received_players,
            FROM_CLIENT_PLAYER_MATCH_INDEX
        )
    return dispach_to_reducers


def get_receive_and_dispach_players_function(skip_header):
    def receive_and_dispach_players(entry_queue, output_exchange, partition_function):
        players_by_key = {}
        logger.info(
            "Starting to receive players from client and dispach it to reducers by key")
        entry_queue.consume(
            get_dispach_to_reducers_function(
                output_exchange, players_by_key, partition_function, skip_header
            ),
            on_sentinel_callback=get_on_sentinel_callback_function(
                output_exchange, players_by_key
            )
        )
    return receive_and_dispach_players

def players_master_main(
        barrier_queue_name,
        reducers_output_queue_name,
        output_exchange_name,
        subscribe_to_entries_function,
        skip_header
    ):
    main_master(
        barrier_queue_name,
        reducers_output_queue_name,
        output_exchange_name,
        subscribe_to_entries_function,
        get_receive_and_dispach_players_function(skip_header)
    )
