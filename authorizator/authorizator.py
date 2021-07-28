import os
import threading

from communications.constants import LONG_MATCHES_TO_CLIENT_QUEUE_NAME, \
    TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME, \
    WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME
from communications.file import ListFile
from communications.rabbitmq_interface import LastHashStrategy, QueueInterface, RabbitMQConnection, split_rows_into_list
import healthcheck.server
from logger.logger import Logger

logger = Logger(True)

STORAGE_DIR = "/data/"
OUTPUT_1_FILE_NAME = "output1.txt"
OUTPUT_2_FILE_NAME = "output2.txt"
OUTPUT_3_FILE_NAME = "output3.txt"
OUTPUT_4_FILE_NAME = "output4.txt"

def get_receive_matches_ids_function(matches_ids, skip_header):
    # function currying in python
    def receive_matches_ids(queue, received_string, _, __):
        for match_id in split_rows_into_list(received_string, skip_header=skip_header):
            matches_ids.append(match_id)
    return receive_matches_ids

def get_print_matches_ids_function(matches_ids, message):
    # function currying in python
    def print_matches_ids(_, __):
        logger.info(message)
        logger.info('\n'.join(matches_ids))
    return print_matches_ids


def get_matches_ids(queue, message, skip_header=False):
    matches_ids = []
    queue.consume(
        get_receive_matches_ids_function(
            matches_ids,
            skip_header
        ),
        on_sentinel_callback=get_print_matches_ids_function(
            matches_ids,
            message
        ),
    )

def receive_long_matches_ids():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, LONG_MATCHES_TO_CLIENT_QUEUE_NAME)

    logger.info("Starting to receive ids of long matches replied")
    get_matches_ids(
        queue,
        "Los IDs de matches que excedieron las dos horas de juego por pro players (average_rating > 2000) en los servers koreacentral, southeastasia y eastus son: "
    )
    connection.close()

def receive_weaker_winner_matches_ids():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection,
        WEAKER_WINNER_TO_CLIENT_QUEUE_NAME,
        last_hash_strategy=LastHashStrategy.LAST_HASH_PER_REDUCER_ID
    )

    logger.info("Starting to receive ids of matches with weaker winner replied")
    get_matches_ids(
        queue,
        "Los IDs de matches en partidas 1v1 donde el ganador tiene un rating 30 % menor al perdedor y el rating del ganador es superior a 1000 son: ",
        skip_header=True
    )
    connection.close()


def receive_winner_rate_of_all_civs(queue, received_string, _, __):
    logger.info("Porcentaje de victorias por civilización en partidas 1v1(ladder == RM_1v1) con civilizaciones diferentes en mapa arena son:")
    logger.info(received_string)
    # TODO falta id de dataset
    if not os.path.exists(STORAGE_DIR + OUTPUT_3_FILE_NAME):
        output_3_file = ListFile(STORAGE_DIR, OUTPUT_3_FILE_NAME)
        output_3_file.write(split_rows_into_list(received_string))
        output_3_file.close()
    queue.stop_consuming()


def receive_winner_rate_by_civ():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    logger.info("Starting to receive to winner rate by civ replied")
    queue.consume(receive_winner_rate_of_all_civs)
    connection.close()


def receive_top_5_civs_used(queue, received_string, _, __):
    logger.info("Top 5 civilizaciones más usadas por pro players(rating > 2000) en team games (ladder == RM_TEAM) en mapa islands son: ")
    logger.info(received_string)
    # TODO falta id de dataset
    if not os.path.exists(STORAGE_DIR + OUTPUT_4_FILE_NAME):
        output_4_file = ListFile(STORAGE_DIR, OUTPUT_4_FILE_NAME)
        output_4_file.write(split_rows_into_list(received_string))
        output_4_file.close()
    queue.stop_consuming()

def receive_top_5_used_civs():
    connection = RabbitMQConnection()
    queue = QueueInterface(
        connection, TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    logger.info("Starting to receive to top 5 civs used replied")
    queue.consume(receive_top_5_civs_used)
    connection.close()

def main():
    healthcheck.server.start_in_new_process()
    receive_long_matches_ids_th = threading.Thread(
        target=receive_long_matches_ids)
    receive_long_matches_ids_th.start()

    receive_weaker_winner_matches_ids_th = threading.Thread(
        target=receive_weaker_winner_matches_ids)
    receive_weaker_winner_matches_ids_th.start()

    receive_winner_rate_by_civ_th = threading.Thread(
        target=receive_winner_rate_by_civ)
    receive_winner_rate_by_civ_th.start()

    receive_top_5_used_civs_th = threading.Thread(
        target=receive_top_5_used_civs)
    receive_top_5_used_civs_th.start()

    receive_long_matches_ids_th.join()
    receive_weaker_winner_matches_ids_th.join()
    receive_winner_rate_by_civ_th.join()
    receive_top_5_used_civs_th.join()


if __name__ == '__main__':
    main()
