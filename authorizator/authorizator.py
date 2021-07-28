import datetime
import os
import pathlib
import threading
import time

from communications.constants import LONG_MATCHES_TO_AUTHORIZATOR_QUEUE_NAME, \
    TOP_5_USED_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME, \
    WEAKER_WINNER_TO_AUTHORIZATOR_QUEUE_NAME, \
    WINNER_RATE_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME
from communications.file import FileAlreadyExistError, ListFile, OneLineFile, safe_remove_file
from communications.rabbitmq_interface import LastHashStrategy, QueueInterface, RabbitMQConnection, split_rows_into_list
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

def with_lock(lock_file_name, function, *args):
    lock_file_full_path = STORAGE_DIR + lock_file_name
    while (True):
        try:
            OneLineFile(STORAGE_DIR, lock_file_name, only_create=True)
            function(*args)
            os.remove(lock_file_full_path)
            break
        except FileAlreadyExistError:
            path = pathlib.Path(lock_file_full_path)
            created_time = datetime.datetime.fromtimestamp(path.stat().st_mtime)
            difference = (
                datetime.datetime.now() - created_time
            ).total_seconds()
            if difference > MAX_LOCK_TIME_IN_SECONDS:
                safe_remove_file(lock_file_full_path)
            time.sleep(LOCK_LOCKED_SLEEP_TIME_IN_SECONDS)
            continue

def with_dataset_token_lock(function, dataset_token):
    with_lock(DATASET_TOKEN_LOCK_FILE_NAME, function, dataset_token)

def internal_set_finished_with_dataset(dataset_token):
    dataset_token_file = OneLineFile(
        STORAGE_DIR,
        DATASET_TOKEN_FILE_NAME
    )
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

def get_receive_matches_ids_function(dataset_token, output_file_name, output_lock_file_name, skip_header):
    # function currying in python
    def receive_matches_ids(queue, received_string, _, __):
        logger.info("Received matches ids from system")
        new_matches = split_rows_into_list(
            received_string, skip_header=skip_header)
        with_lock(
            output_lock_file_name,
            write_to_output,
            output_file_name, dataset_token, new_matches
        )
    return receive_matches_ids


def get_print_matches_ids_function(dataset_token, output_file_name, output_sentinel_file_name, message):
    # function currying in python
    def print_matches_ids(_, __):
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
        dataset_token,
        output_file_name,
        output_sentinel_file_name,
        output_lock_file_name,
        message,
        skip_header=False
    ):
    queue.consume(
        get_receive_matches_ids_function(
            dataset_token,
            output_file_name,
            output_lock_file_name,
            skip_header
        ),
        on_sentinel_callback=get_print_matches_ids_function(
            dataset_token,
            output_file_name,
            output_sentinel_file_name,
            message
        ),
    )


def receive_long_matches_ids(dataset_token):
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, LONG_MATCHES_TO_AUTHORIZATOR_QUEUE_NAME)

    logger.info("Starting to receive ids of long matches replied")
    get_matches_ids(
        queue,
        dataset_token,
        OUTPUT_1_FILE_NAME,
        OUTPUT_1_SENTINEL_FILE_NAME,
        OUTPUT_1_LOCK_FILE_NAME,
        "Los IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus son: "
    )
    connection.close()


def receive_weaker_winner_matches_ids(dataset_token):
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection,
        WEAKER_WINNER_TO_AUTHORIZATOR_QUEUE_NAME,
        last_hash_strategy=LastHashStrategy.LAST_HASH_PER_REDUCER_ID
    )

    logger.info("Starting to receive ids of matches with weaker winner replied")
    get_matches_ids(
        queue,
        dataset_token,
        OUTPUT_2_FILE_NAME,
        OUTPUT_2_SENTINEL_FILE_NAME,
        OUTPUT_2_LOCK_FILE_NAME,
        "Los IDs de matches en partidas 1v1 donde el ganador tiene un rating 30 % menor al perdedor y el rating del ganador es superior a 1000 son: ",
        skip_header=True
    )
    connection.close()


def get_receive_winner_rate_of_all_civs_function(dataset_token):
    def receive_winner_rate_of_all_civs(queue, received_string, _, __):
        logger.info("Porcentaje de victorias por civilización en partidas 1v1(ladder == RM_1v1) con civilizaciones diferentes en mapa arena son:")
        logger.info(received_string)
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
        queue.stop_consuming()
    return receive_winner_rate_of_all_civs


def receive_winner_rate_by_civ(dataset_token):
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, WINNER_RATE_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME)

    logger.info("Starting to receive to winner rate by civ replied")
    queue.consume(get_receive_winner_rate_of_all_civs_function(dataset_token))
    connection.close()


def get_receive_top_5_civs_used_function(dataset_token):
    def receive_top_5_civs_used(queue, received_string, _, __):
        logger.info("Top 5 civilizaciones más usadas por pro players(rating > 2000) en team games (ladder == RM_TEAM) en mapa islands son: ")
        logger.info(received_string)
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
        queue.stop_consuming()
    return receive_top_5_civs_used

def receive_top_5_used_civs(dataset_token):
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, TOP_5_USED_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME)

    logger.info("Starting to receive to top 5 civs used replied")
    queue.consume(get_receive_top_5_civs_used_function(dataset_token))
    connection.close()

def main():
    healthcheck.server.start_in_new_process()

    dataset_token = "tokennnn" # TODO arreglar
    dataset_token_file = OneLineFile(
        STORAGE_DIR,
        DATASET_TOKEN_FILE_NAME
    )
    dataset_token_file.write(dataset_token)
    dataset_token_file.close()

    receive_long_matches_ids_th = threading.Thread(
        target=receive_long_matches_ids,
        args=(dataset_token,)
    )
    receive_long_matches_ids_th.start()

    receive_weaker_winner_matches_ids_th = threading.Thread(
        target=receive_weaker_winner_matches_ids,
        args=(dataset_token,)
    )
    receive_weaker_winner_matches_ids_th.start()

    receive_winner_rate_by_civ_th = threading.Thread(
        target=receive_winner_rate_by_civ,
        args=(dataset_token,)
    )
    receive_winner_rate_by_civ_th.start()

    receive_top_5_used_civs_th = threading.Thread(
        target=receive_top_5_used_civs,
        args=(dataset_token,)
    )
    receive_top_5_used_civs_th.start()

    receive_long_matches_ids_th.join()
    receive_weaker_winner_matches_ids_th.join()
    receive_winner_rate_by_civ_th.join()
    receive_top_5_used_civs_th.join()


if __name__ == '__main__':
    main()
