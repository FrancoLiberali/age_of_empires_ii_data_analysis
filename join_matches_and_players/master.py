from communications.file import JsonFile
from config.envvars import BARRIER_QUEUE_NAME_KEY, INPUT_QUEUE_NAME_KEY, MATCHES_INPUT_EXCHANGE_NAME_KEY, OUTPUT_EXCHANGE_NAME_KEY, PLAYERS_INPUT_EXCHANGE_NAME_KEY, REDUCERS_OUTPUT_QUEUE_NAME_KEY, get_config_param
from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    MATCHES_KEY, MATCHES_SENTINEL, \
    PLAYERS_KEY, SENTINEL_KEY
from communications.rabbitmq_interface import ExchangeInterface, LastHashStrategy, QueueInterface, split_rows_into_list
from master_reducers_arq.master import SENTINEL_RECEIVED, STATE_RECEIVING_SENTINELS, STATE_STORAGE_DIR, add_to_dict_by_key, main_master, send_sentinel_to_reducers
from logger.logger import Logger

logger = Logger()
INPUTS_AMOUNT = 2
ENTRY_SENTINELS_FILE = "entry_sentinels.json"

def get_on_sentinel_callback_function(output_exchange, players_by_key, matches_by_key, state_file):
    sentinels_received_file = JsonFile(
        STATE_STORAGE_DIR, ENTRY_SENTINELS_FILE
    )
    entries_map = sentinels_received_file.content
    logger.debug(
        f"Initial entry sentinels received: {len(entries_map.keys())}")
    def on_sentinel_callback(_, routing_key):
        if entries_map.get(routing_key, None) is None:
            entries_map[routing_key] = SENTINEL_RECEIVED
            sentinels_received_file.write(entries_map)

        sentinels_count = len(entries_map.keys())
        logger.info(
            f"Sentinel message: {sentinels_count}/{INPUTS_AMOUNT} received")
        if routing_key == MATCHES_KEY:
            logger.info("Sending matches sentinels because all matches had come")
            # TODO descomentar esto para volver a poner la optimizacion de chunks si queda tiempo. Requiere reanalizar estados
            # send the remaining matches
            # send_dict_by_key(output_exchange, matches_by_key,
                            #  JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, check_chunk_size=False)
            output_exchange.send_string(MATCHES_SENTINEL, SENTINEL_KEY)
        if sentinels_count == INPUTS_AMOUNT:
            logger.info("Stoping receive and dispach it to reducers.")
            # TODO descomentar esto para volver a poner la optimizacion de chunks si queda tiempo. Requiere reanalizar estados
            # send the remaining players and matches
            # send_dict_by_key(output_exchange, players_by_key,
                            #  JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, check_chunk_size=False)
            # send_dict_by_key(output_exchange, matches_by_key,
                            #  JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, check_chunk_size=False)
            send_sentinel_to_reducers(output_exchange)
            state_file.write(STATE_RECEIVING_SENTINELS)
            sentinels_received_file.write({})
            sentinels_received_file.close()
            return QueueInterface.STOP_CONSUMING
        return QueueInterface.NO_STOP_CONSUMING
    return on_sentinel_callback

def get_dispach_to_reducers_function(output_exchange, players_by_key, matches_by_key, partition_function):
    def dispach_to_reducers(queue, received_string, routing_key, _):
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
    # TODO CONSULTAR, al matarlo igual rabbit sigue diciendo que la conexion esta y los mensajes como estan en Unacked
    # no los vuelve a leer, despues de un rato recien reacciona
    main_master(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(REDUCERS_OUTPUT_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_EXCHANGE_NAME_KEY, logger),
        subscribe_to_entries,
        receive_and_dispach_players_and_matches
    )


if __name__ == '__main__':
    main()
