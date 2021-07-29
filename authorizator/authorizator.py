import datetime
import os
import pathlib
import threading
import time
import uuid

from communications.constants import AUTHORIZED_CODE, CLIENTS_REQUESTS_QUEUE_NAME, CLIENTS_RESPONSES_EXCHANGE_NAME, CLIENT_REQUEST_RESPONSE_QUEUE_NAME_INDEX, CLIENT_REQUEST_SEPARATOR, CLIENT_REQUEST_TYPE_AUTHORIZE, CLIENT_REQUEST_TYPE_INDEX, CLIENT_REQUEST_TYPE_QUERY, LONG_MATCHES_TO_AUTHORIZATOR_QUEUE_NAME, \
    TOP_5_USED_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME, UNAUTHORIZED_CODE, \
    WEAKER_WINNER_TO_AUTHORIZATOR_QUEUE_NAME, \
    WINNER_RATE_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME
from communications.file import FileAlreadyExistError, ListFile, OneLineFile, safe_remove_file
from communications.rabbitmq_interface import LastHashStrategy, QueueInterface, RabbitMQConnection, split_rows_into_list, ExchangeInterface
import healthcheck.server
from logger.logger import Logger

logger = Logger(True)

STORAGE_DIR = "/data/"
NO_DATASET_BEING_PROCESSED = ""
DATASET_TOKEN_FILE_NAME = "dataset_token.txt"
OUTPUT_1_FILE_NAME = "output1.txt"
OUTPUT_1_SENTINEL_FILE_NAME = "output1_sentinel.txt"
OUTPUT_2_FILE_NAME = "output2.txt"
OUTPUT_2_SENTINEL_FILE_NAME = "output2_sentinel.txt"
OUTPUT_3_FILE_NAME = "output3.txt"
OUTPUT_4_FILE_NAME = "output4.txt"
FILES_PER_PROCESSED_DATASET_AMOUNT  = 6

MAX_LOCK_TIME_IN_SECONDS = 5
DATASET_TOKEN_LOCK_FILE_NAME = "dataset_token_lock.txt"
OUTPUT_1_LOCK_FILE_NAME = "output1_lock.txt"
OUTPUT_2_LOCK_FILE_NAME = "output2_lock.txt"

LOCK_LOCKED_SLEEP_TIME_IN_SECONDS = 1

connection_output_1 = RabbitMQConnection()
queue_output_1 = QueueInterface(
    connection_output_1, LONG_MATCHES_TO_AUTHORIZATOR_QUEUE_NAME)
connection_output_2 = RabbitMQConnection()
queue_output_2 = QueueInterface(
    connection_output_2,
    WEAKER_WINNER_TO_AUTHORIZATOR_QUEUE_NAME,
    last_hash_strategy=LastHashStrategy.LAST_HASH_PER_REDUCER_ID
)
connection_output_3 = RabbitMQConnection()
queue_output_3 = QueueInterface(
    connection_output_3, WINNER_RATE_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME)
connection_output_4 = RabbitMQConnection()
queue_output_4 = QueueInterface(
    connection_output_4, TOP_5_USED_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME)

def with_lock(lock_file_name, function, *args):
    lock_file_full_path = STORAGE_DIR + lock_file_name
    while (True):
        try:
            OneLineFile(STORAGE_DIR, lock_file_name, only_create=True)
            return_value = function(*args)
            os.remove(lock_file_full_path)
            return return_value
        except FileAlreadyExistError:
            path = pathlib.Path(lock_file_full_path)
            if path.exists():
                created_time = datetime.datetime.fromtimestamp(path.stat().st_mtime)
                difference = (
                    datetime.datetime.now() - created_time
                ).total_seconds()
                if difference > MAX_LOCK_TIME_IN_SECONDS:
                    safe_remove_file(lock_file_full_path)
            time.sleep(LOCK_LOCKED_SLEEP_TIME_IN_SECONDS)
            continue

def with_dataset_token_lock(function, *args):
    return with_lock(DATASET_TOKEN_LOCK_FILE_NAME, function, *args)

def get_dataset_token_file():
    return OneLineFile(
        STORAGE_DIR,
        DATASET_TOKEN_FILE_NAME
    )


def internal_get_dataset_token():
    return get_dataset_token_file().content

def get_dataset_token():
    return with_dataset_token_lock(internal_get_dataset_token)

def internal_set_finished_with_dataset(dataset_token):
    for queue in [queue_output_1, queue_output_2, queue_output_3, queue_output_4]:
        # ack all messages to remove duplicates that otherwise would be considered as results of the next dataset
        # TODO queue.clear()
        queue.set_last_hash("")
    dataset_token_file = get_dataset_token_file()
    if dataset_token_file.content == dataset_token:
        logger.info(f"Setting that dataset {dataset_token} finished")
        dataset_token_file.write(NO_DATASET_BEING_PROCESSED)
    else:
        logger.debug(f"Trying to set that dataset {dataset_token} but it is not being processed")

def set_finished_with_dataset(dataset_token):
    with_dataset_token_lock(
        internal_set_finished_with_dataset,
        dataset_token,
    )


def check_if_dataset_finished(dataset_token):
    if len(os.listdir(get_dataset_token_dir(dataset_token))) == FILES_PER_PROCESSED_DATASET_AMOUNT:
        logger.info(f"All results for {dataset_token} received from system")
        set_finished_with_dataset(dataset_token)

def get_dataset_token_dir(dataset_token):
    return STORAGE_DIR + dataset_token + '/'


def write_to_output(output_file_name, dataset_token, new_matches):
    output_file = ListFile(
        get_dataset_token_dir(dataset_token), output_file_name)
    actual_matches = output_file.content
    if not new_matches[0] in actual_matches:
        output_file.write(new_matches)
    output_file.close()

def get_receive_matches_ids_function(output_file_name, output_lock_file_name, skip_header):
    # function currying in python
    def receive_matches_ids(queue, received_string, _, __):
        logger.info("Received matches ids from system")
        dataset_token = get_dataset_token()
        new_matches = split_rows_into_list(
            received_string, skip_header=skip_header)
        with_lock(
            output_lock_file_name,
            write_to_output,
            output_file_name, dataset_token, new_matches
        )
    return receive_matches_ids


def get_print_matches_ids_function(output_file_name, output_sentinel_file_name, message):
    # function currying in python
    def print_matches_ids(_, __):
        dataset_token = get_dataset_token()
        try:
            logger.info("Received sentinel of output")
            OneLineFile(
                get_dataset_token_dir(dataset_token),
                output_sentinel_file_name,
                only_create=True
            ).close()
            output_file = ListFile(
                get_dataset_token_dir(dataset_token),
                output_file_name
            )
            logger.info(message)
            logger.info('\n'.join(output_file.content))
            output_file.close()
        except FileAlreadyExistError:
            logger.debug(
                f"Sentinel received already exists: {output_sentinel_file_name}")
        check_if_dataset_finished(dataset_token)
    return print_matches_ids


def get_matches_ids(
        queue,
        output_file_name,
        output_sentinel_file_name,
        output_lock_file_name,
        message,
        skip_header=False
    ):
    queue.consume(
        get_receive_matches_ids_function(
            output_file_name,
            output_lock_file_name,
            skip_header
        ),
        on_sentinel_callback=get_print_matches_ids_function(
            output_file_name,
            output_sentinel_file_name,
            message
        ),
    )


def receive_long_matches_ids():
    logger.info("Starting to receive ids of long matches replied")
    while True:
        get_matches_ids(
            queue_output_1,
            OUTPUT_1_FILE_NAME,
            OUTPUT_1_SENTINEL_FILE_NAME,
            OUTPUT_1_LOCK_FILE_NAME,
            "Los IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus son: "
        )


def receive_weaker_winner_matches_ids():
    logger.info("Starting to receive ids of matches with weaker winner replied")
    while True:
        get_matches_ids(
            queue_output_2,
            OUTPUT_2_FILE_NAME,
            OUTPUT_2_SENTINEL_FILE_NAME,
            OUTPUT_2_LOCK_FILE_NAME,
            "Los IDs de matches en partidas 1v1 donde el ganador tiene un rating 30 % menor al perdedor y el rating del ganador es superior a 1000 son: ",
            skip_header=True
        )


def receive_winner_rate_of_all_civs(queue, received_string, _, __):
    logger.info("Porcentaje de victorias por civilización en partidas 1v1(ladder == RM_1v1) con civilizaciones diferentes en mapa arena son:")
    logger.info(received_string)
    dataset_token = get_dataset_token()
    try:
        output_3_file = ListFile(
            get_dataset_token_dir(dataset_token),
            OUTPUT_3_FILE_NAME,
            only_create=True
        )
        output_3_file.write(split_rows_into_list(received_string))
        output_3_file.close()
    except FileAlreadyExistError:
        logger.debug(f"The data received for winner rate already exists: {received_string}")
    check_if_dataset_finished(dataset_token)


def receive_winner_rate_by_civ():
    logger.info("Starting to receive to winner rate by civ replied")
    queue_output_3.consume(receive_winner_rate_of_all_civs)


def receive_top_5_civs_used(queue, received_string, _, __):
    logger.info("Top 5 civilizaciones más usadas por pro players(rating > 2000) en team games (ladder == RM_TEAM) en mapa islands son: ")
    logger.info(received_string)
    dataset_token = get_dataset_token()
    try:
        output_4_file = ListFile(
            get_dataset_token_dir(dataset_token),
            OUTPUT_4_FILE_NAME,
            only_create=True
        )
        output_4_file.write(split_rows_into_list(received_string))
        output_4_file.close()
    except FileAlreadyExistError:
        logger.debug(f"The data received for top 5 civs already exists: {received_string}")
    check_if_dataset_finished(dataset_token)

def receive_top_5_used_civs():
    logger.info("Starting to receive to top 5 civs used replied")
    queue_output_4.consume(receive_top_5_civs_used)

def start_results_receiving_threads():
    receive_long_matches_ids_th = threading.Thread(
        target=receive_long_matches_ids
    )
    receive_long_matches_ids_th.start()

    receive_weaker_winner_matches_ids_th = threading.Thread(
        target=receive_weaker_winner_matches_ids
    )
    receive_weaker_winner_matches_ids_th.start()

    receive_winner_rate_by_civ_th = threading.Thread(
        target=receive_winner_rate_by_civ
    )
    receive_winner_rate_by_civ_th.start()

    receive_top_5_used_civs_th = threading.Thread(
        target=receive_top_5_used_civs
    )
    receive_top_5_used_civs_th.start()


def reply_authorization_request(output_exchange, response_queue_name):
    logger.info("Authorize request arrived")
    dataset_token = uuid.uuid4().hex
    dataset_token_file = get_dataset_token_file()
    if dataset_token_file.content == NO_DATASET_BEING_PROCESSED:
        dataset_token_file.write(dataset_token)
        dataset_token_file.close()
        logger.info(f"Authorizing client with dataset token {dataset_token}")
        output_exchange.send_string(
            CLIENT_REQUEST_SEPARATOR.join(
                [AUTHORIZED_CODE, dataset_token]
            ),
            response_queue_name
        )
    else:
        logger.info(f"Client not authorized")
        output_exchange.send_string(UNAUTHORIZED_CODE, response_queue_name)


def reply_query_request(output_exchange, response_queue_name, request):
    pass


def get_handle_client_request_function(output_exchange):
    def handle_client_request(queue, received_string, _, __):
        request = received_string.split(CLIENT_REQUEST_SEPARATOR)
        response_queue_name = request[CLIENT_REQUEST_RESPONSE_QUEUE_NAME_INDEX]
        if request[CLIENT_REQUEST_TYPE_INDEX] == CLIENT_REQUEST_TYPE_AUTHORIZE:
            with_dataset_token_lock(
                reply_authorization_request,
                output_exchange,
                response_queue_name
            )
        if request[CLIENT_REQUEST_TYPE_INDEX] == CLIENT_REQUEST_TYPE_QUERY:
            reply_query_request(output_exchange,
                                response_queue_name, request)
    return handle_client_request


def main():
    healthcheck.server.start_in_new_process()

    start_results_receiving_threads()

    connection = RabbitMQConnection()
    requests_queue = QueueInterface(
        connection,
        CLIENTS_REQUESTS_QUEUE_NAME,
        last_hash_strategy=LastHashStrategy.NO_LAST_HASH_SAVING
    )
    output_exchange = ExchangeInterface.newDirect(connection, CLIENTS_RESPONSES_EXCHANGE_NAME)
    requests_queue.consume(
        get_handle_client_request_function(output_exchange)
    )


if __name__ == '__main__':
    main()
