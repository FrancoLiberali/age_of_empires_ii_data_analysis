from config.envvars import BARRIER_QUEUE_NAME_KEY, MATCHES_INPUT_EXCHANGE_NAME_KEY, OUTPUT_EXCHANGE_NAME_KEY, PLAYERS_INPUT_EXCHANGE_NAME_KEY, REDUCERS_OUTPUT_QUEUE_NAME_KEY, ROWS_CHUNK_SIZE_KEY, get_config_param
from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    MATCHES_KEY, \
    PLAYERS_KEY
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface, split_columns_into_list, split_rows_into_list
from master_reducers_arq.master import main_master
from logger.logger import Logger

logger = Logger()
ROWS_CHUNK_SIZE = get_config_param(ROWS_CHUNK_SIZE_KEY, logger)

def send_dict_by_key(output_exchange, dict_by_key, tag_to_send, check_chunk_size=True):
    for key, rows in list(dict_by_key.items()):
        if len(rows) > ROWS_CHUNK_SIZE or not check_chunk_size:
            output_exchange.send_list_as_rows([tag_to_send] + rows, key)
            dict_by_key.pop(key)
            del rows


def add_to_dict_by_key(output_exchange,
                       partition_function,
                       dict_by_key,
                       received_rows,
                       match_id_index,
                       tag_to_send):
    for row_string in received_rows:
        key = partition_function.get_key(
            split_columns_into_list(row_string)[match_id_index]
        )
        rows_list = dict_by_key.get(key, [])
        rows_list.append(row_string)
        dict_by_key[key] = rows_list

    send_dict_by_key(output_exchange, dict_by_key, tag_to_send)

INPUTS_AMOUNT = 2

def get_on_sentinel_callback_function(output_exchange, players_by_key, matches_by_key, sentinels_count):
    def on_sentinel_callback(_):
        sentinels_count[0] += 1
        logger.info(
            f"Sentinel message: {sentinels_count[0]}/{INPUTS_AMOUNT} received")
        if sentinels_count[0] == INPUTS_AMOUNT:
            logger.info("Stoping receive and dispach it to reducers.")
            # send the remaining players and matches
            send_dict_by_key(output_exchange, players_by_key,
                             JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, False)
            send_dict_by_key(output_exchange, matches_by_key,
                             JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, False)
            return QueueInterface.STOP_CONSUMING
        return QueueInterface.NO_STOP_CONSUMING
    return on_sentinel_callback

def get_dispach_to_reducers_function(output_exchange, players_by_key, matches_by_key, partition_function):
    def dispach_to_reducers(queue, received_string, routing_key):
        received_entries = [line for line in split_rows_into_list(received_string)]
        if routing_key == PLAYERS_KEY:
            add_to_dict_by_key(
                output_exchange,
                partition_function,
                players_by_key,
                received_entries,
                FROM_CLIENT_PLAYER_MATCH_INDEX,
                JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR
            )
        elif routing_key == MATCHES_KEY:
            add_to_dict_by_key(
                output_exchange,
                partition_function,
                matches_by_key,
                received_entries,
                FROM_CLIENT_MATCH_TOKEN_INDEX,
                JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR
            )
    return dispach_to_reducers


def receive_and_dispach_players_and_matches(entry_queue, output_exchange, partition_function):
    players_by_key = {}
    matches_by_key = {}
    sentinels_count = [0]
    logger.info("Starting to receive players and matches and dispach it to reducers by key")
    entry_queue.consume(
        get_dispach_to_reducers_function(
            output_exchange,
            players_by_key,
            matches_by_key,
            partition_function
        ),
        on_sentinel_callback=get_on_sentinel_callback_function(
            output_exchange,
            players_by_key,
            matches_by_key,
            sentinels_count
        )
    )


def subscribe_to_entries(connection):
    matches_input_exchage = ExchangeInterface.newDirect(
        connection,
        get_config_param(MATCHES_INPUT_EXCHANGE_NAME_KEY, logger)
    )
    players_input_exchage = ExchangeInterface.newFanout(
        connection,
        get_config_param(PLAYERS_INPUT_EXCHANGE_NAME_KEY, logger)
    )

    input_queue = QueueInterface.newPrivate(connection)
    input_queue.bind(matches_input_exchage, MATCHES_KEY)
    input_queue.bind(players_input_exchage, PLAYERS_KEY)

    return input_queue

def main():
    main_master(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(REDUCERS_OUTPUT_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_EXCHANGE_NAME_KEY, logger),
        subscribe_to_entries,
        receive_and_dispach_players_and_matches
    )


if __name__ == '__main__':
    main()
