from config.envvars import BARRIER_QUEUE_NAME_KEY, INPUT_QUEUE_NAME_KEY, MATCHES_INPUT_EXCHANGE_NAME_KEY, OUTPUT_EXCHANGE_NAME_KEY, PLAYERS_INPUT_EXCHANGE_NAME_KEY, REDUCERS_OUTPUT_QUEUE_NAME_KEY, ROWS_CHUNK_SIZE_KEY, get_config_param
from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    MATCHES_KEY, MATCHES_SENTINEL, \
    PLAYERS_KEY, SENTINEL_KEY
from communications.rabbitmq_interface import ExchangeInterface, LastHashStrategy, QueueInterface, split_columns_into_list, split_rows_into_list
from master_reducers_arq.master import STATE_RECEIVING_SENTINELS, add_to_dict_by_key, main_master, send_dict_by_key, send_sentinel_to_reducers
from logger.logger import Logger

logger = Logger()
INPUTS_AMOUNT = 2


def get_on_sentinel_callback_function(output_exchange, players_by_key, matches_by_key, sentinels_count, state_file):
    def on_sentinel_callback(_, routing_key):
        sentinels_count[0] += 1
        logger.info(
            f"Sentinel message: {sentinels_count[0]}/{INPUTS_AMOUNT} received")
        if routing_key == MATCHES_KEY:
            # TODO descomentar esto para volver a poner la optimizacion de chunks si queda tiempo. Requiere reanalizar estados
            # send the remaining matches
            # send_dict_by_key(output_exchange, matches_by_key,
                            #  JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, check_chunk_size=False)
            output_exchange.send_string(MATCHES_SENTINEL, SENTINEL_KEY)
        if sentinels_count[0] == INPUTS_AMOUNT:
            logger.info("Stoping receive and dispach it to reducers.")
            # TODO descomentar esto para volver a poner la optimizacion de chunks si queda tiempo. Requiere reanalizar estados
            # send the remaining players and matches
            # send_dict_by_key(output_exchange, players_by_key,
                            #  JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, check_chunk_size=False)
            # send_dict_by_key(output_exchange, matches_by_key,
                            #  JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, check_chunk_size=False)
            send_sentinel_to_reducers(output_exchange)
            state_file.write(STATE_RECEIVING_SENTINELS)
            return QueueInterface.STOP_CONSUMING
        return QueueInterface.NO_STOP_CONSUMING
    return on_sentinel_callback

def get_dispach_to_reducers_function(output_exchange, players_by_key, matches_by_key, partition_function):
    def dispach_to_reducers(queue, received_string, routing_key):
        received_entries = split_rows_into_list(received_string)
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


def receive_and_dispach_players_and_matches(entry_queue, output_exchange, partition_function, state_file):
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
            sentinels_count,
            state_file
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

    input_queue = QueueInterface(
        connection,
        get_config_param(INPUT_QUEUE_NAME_KEY, logger),
        last_hash_strategy=LastHashStrategy.LAST_HASH_PER_ROUTING_KEY
    )
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
