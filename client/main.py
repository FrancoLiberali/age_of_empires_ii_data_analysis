#!/usr/bin/env python3
import csv
import threading
import time

from config.envvars import CHUCKSIZE_IN_LINES_KEY, ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY, ENTRY_MATCH_DURATION_INDEX_KEY, ENTRY_MATCH_LADDER_INDEX_KEY, ENTRY_MATCH_MAP_INDEX_KEY, ENTRY_MATCH_MIRROR_INDEX_KEY, ENTRY_MATCH_SERVER_INDEX_KEY, ENTRY_MATCH_TOKEN_INDEX_KEY, ENTRY_PLAYER_CIV_INDEX_KEY, ENTRY_PLAYER_MATCH_INDEX_KEY, ENTRY_PLAYER_RATING_INDEX_KEY, ENTRY_PLAYER_TOKEN_INDEX_KEY, ENTRY_PLAYER_WINNER_INDEX_KEY, MATCHES_FILE_NAME_KEY, PLAYERS_FILE_NAME_KEY, get_config_param, get_config_params
from communications.constants import AUTHORIZED_CODE, AUTHORIZE_RESPONSE_AUTHORIZATION_INDEX, AUTHORIZE_RESPONSE_DATASET_TOKEN_INDEX, CLIENTS_REQUESTS_QUEUE_NAME, CLIENTS_RESPONSES_EXCHANGE_NAME, CLIENT_REQUEST_SEPARATOR, CLIENT_REQUEST_TYPE_AUTHORIZE, CLIENT_REQUEST_TYPE_QUERY, DATASET_BEING_PROCESSED_CODE, DATASET_FINISHED_CODE, DATASET_NOT_FOUND_CODE, MATCHES_FANOUT_EXCHANGE_NAME, PLAYERS_FANOUT_EXCHANGE_NAME, QUERY_RESPONSE_RESULTS_INDEX, QUERY_RESPONSE_RESULTS_OUTPUT_SEPARATOR, QUERY_RESPONSE_STATE_INDEX
from communications.rabbitmq_interface import ExchangeInterface, LastHashStrategy, QueueInterface, RabbitMQConnection
from logger.logger import Logger

logger = Logger(True)

DATASETS_DIR = "/datasets/"
CHUCKSIZE_IN_LINES = get_config_param(CHUCKSIZE_IN_LINES_KEY, logger)

ENTRY_MATCHES_INDEXES = get_config_params([
        ENTRY_MATCH_TOKEN_INDEX_KEY,
        ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY,
        ENTRY_MATCH_SERVER_INDEX_KEY,
        ENTRY_MATCH_DURATION_INDEX_KEY,
        ENTRY_MATCH_LADDER_INDEX_KEY,
        ENTRY_MATCH_MAP_INDEX_KEY,
        ENTRY_MATCH_MIRROR_INDEX_KEY,
    ], logger)

ENTRY_PLAYERS_INDEXES = get_config_params([
        ENTRY_PLAYER_TOKEN_INDEX_KEY,
        ENTRY_PLAYER_MATCH_INDEX_KEY,
        ENTRY_PLAYER_RATING_INDEX_KEY,
        ENTRY_PLAYER_WINNER_INDEX_KEY,
        ENTRY_PLAYER_CIV_INDEX_KEY,
    ], logger)


def get_line_string_for_matches(line_list):
    return [
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_TOKEN_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_SERVER_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_DURATION_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_LADDER_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_MAP_INDEX_KEY]],
        line_list[ENTRY_MATCHES_INDEXES[ENTRY_MATCH_MIRROR_INDEX_KEY]],
    ]


def get_line_string_for_players(line_list):
    return [
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_TOKEN_INDEX_KEY]],
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_MATCH_INDEX_KEY]],
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_RATING_INDEX_KEY]],
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_WINNER_INDEX_KEY]],
        line_list[ENTRY_PLAYERS_INDEXES[ENTRY_PLAYER_CIV_INDEX_KEY]],
    ]


def send_file_in_chunks(exchange, file_path, get_columns_function):
    chunk = []
    with open(DATASETS_DIR + file_path) as csvfile:
        reader = csv.reader(csvfile)
        for i, line in enumerate(reader):
            if i == 0:
                # file header
                continue
            if (i % CHUCKSIZE_IN_LINES == 0 and i > 0):
                exchange.send_list_of_columns(chunk)
                del chunk[:]  # delete from memory
            chunk.append(get_columns_function(line))
        exchange.send_list_of_columns(chunk)
        exchange.send_sentinel()


def send_matches():
    connection = RabbitMQConnection()
    exchange = ExchangeInterface.newFanout(
        connection, MATCHES_FANOUT_EXCHANGE_NAME)

    logger.info("Starting to send matches to server")
    send_file_in_chunks(exchange,
                        get_config_param(MATCHES_FILE_NAME_KEY, logger),
                        get_line_string_for_matches)
    logger.info("Finished sending matches to server")
    connection.close()


def send_players():
    connection = RabbitMQConnection()
    exchange = ExchangeInterface.newFanout(
        connection, PLAYERS_FANOUT_EXCHANGE_NAME)

    logger.info("Starting to send players to server")
    send_file_in_chunks(exchange,
                        get_config_param(PLAYERS_FILE_NAME_KEY, logger),
                        get_line_string_for_players)
    logger.info("Finished sending players to server")
    connection.close()

def send_data_to_server():
    send_matches_th = threading.Thread(
        target=send_matches)
    send_matches_th.start()

    send_players_th = threading.Thread(
        target=send_players)
    send_players_th.start()

    send_matches_th.join()
    send_players_th.join()

def get_authorize_request(response_queue_name):
    request_list = [response_queue_name, CLIENT_REQUEST_TYPE_AUTHORIZE]
    return CLIENT_REQUEST_SEPARATOR.join(request_list)


def get_query_request(response_queue_name, dataset_token):
    request_list = [response_queue_name,
                    CLIENT_REQUEST_TYPE_QUERY, dataset_token]
    return CLIENT_REQUEST_SEPARATOR.join(request_list)

def get_handle_authorize_response_function(dataset_token):
    def handle_authorize_response(queue, received_string, _, __):
        response = received_string.split(CLIENT_REQUEST_SEPARATOR)
        if response[AUTHORIZE_RESPONSE_AUTHORIZATION_INDEX] == AUTHORIZED_CODE:
            dataset_token[0] = response[AUTHORIZE_RESPONSE_DATASET_TOKEN_INDEX]
            logger.info(f"Authorized to send dataset {dataset_token[0]}")
        else:
            logger.info("Not authorized to send dataset")
        queue.stop_consuming()
    return handle_authorize_response


def get_handle_query_response_function(results_responded, dataset_token):
    def handle_authorize_response(queue, received_string, _, __):
        response = received_string.split(CLIENT_REQUEST_SEPARATOR)
        if response[QUERY_RESPONSE_STATE_INDEX] == DATASET_FINISHED_CODE:
            results_responded[0] = True
            results = response[QUERY_RESPONSE_RESULTS_INDEX]
            results_by_output = results.split(
                QUERY_RESPONSE_RESULTS_OUTPUT_SEPARATOR)
            logger.info(f"""Los IDs de matches que excedieron las dos horas de juego por pro players(average_rating > 2000) en los servers koreacentral, southeastasia y eastus son:
{results_by_output[0]}""")
            logger.info(f"""Los IDs de matches en partidas 1v1 donde el ganador tiene un rating 30 % menor al perdedor y el rating del ganador es superior a 1000 son:
{results_by_output[1]}""")
            logger.info(f"""Porcentaje de victorias por civilizaci??n en partidas 1v1(ladder == RM_1v1) con civilizaciones diferentes en mapa arena son:
{results_by_output[2]}""")
            logger.info(f"""Top 5 civilizaciones m??s usadas por pro players(rating > 2000) en team games (ladder == RM_TEAM) en mapa islands son:
{results_by_output[3]}""")
        elif response[QUERY_RESPONSE_STATE_INDEX] == DATASET_BEING_PROCESSED_CODE:
            logger.info(f"Dataset {dataset_token} is being processed")
        elif response[QUERY_RESPONSE_STATE_INDEX] == DATASET_NOT_FOUND_CODE:
            logger.info(f"Dataset {dataset_token} not found in server")
        queue.stop_consuming()
    return handle_authorize_response

TIME_BETWEEN_AUTHORIZATION_RETRY_IN_SECONDS = 15
TIME_BETWEEN_QUERY_RETRY_IN_SECONDS = 15

def main():
    connection = RabbitMQConnection()

    response_queue = QueueInterface.newPrivate(
        connection,
        last_hash_strategy=LastHashStrategy.NO_LAST_HASH_SAVING
    )
    response_exchange = ExchangeInterface.newDirect(
        connection, CLIENTS_RESPONSES_EXCHANGE_NAME)
    response_queue.bind(response_exchange, routing_key=response_queue.name)
    requests_queue = QueueInterface(
        connection,
        CLIENTS_REQUESTS_QUEUE_NAME,
        last_hash_strategy=LastHashStrategy.NO_LAST_HASH_SAVING
    )

    dataset_token = [None]
    while True:
        logger.info("Trying to send dataset")
        requests_queue.send_string(
            get_authorize_request(response_queue.name)
        )
        response_queue.consume(get_handle_authorize_response_function(dataset_token))
        if dataset_token[0] is not None:
            send_data_to_server()
            break
        time.sleep(TIME_BETWEEN_AUTHORIZATION_RETRY_IN_SECONDS)

    while True:
        logger.info("Trying to receive results from server")
        requests_queue.send_string(
            get_query_request(response_queue.name, dataset_token[0])
        )
        results_responded = [False]
        response_queue.consume(
            get_handle_query_response_function(
                results_responded, dataset_token[0])
        )
        if results_responded[0]:
            break
        time.sleep(TIME_BETWEEN_QUERY_RETRY_IN_SECONDS)
    response_queue.delete()
    connection.close()

if __name__ == '__main__':
    main()




