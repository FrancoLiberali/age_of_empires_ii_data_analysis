from communications.rabbitmq_interface import split_columns_into_list, split_rows_into_list
from config.envvars import BARRIER_QUEUE_NAME_KEY, OUTPUT_QUEUE_NAME_KEY, get_config_param
from master_reducers_arq.reducer import main_reducer
from logger.logger import Logger

logger = Logger()

def get_count_function(results_by_civ, append_to_results_function):
    # python function currying
    def count(queue, received_string, _):
        for player_string in split_rows_into_list(received_string):
            player_columns = split_columns_into_list(player_string)
            append_to_results_function(results_by_civ, player_columns)
    return count


def get_group_players_by_civ_function(append_to_results_function):
    # python function currying
    def group_players_by_civ(input_queue, output_queue):
        values_by_civ = {}
        logger.info(
            f'Starting to receive players in matches to group by civ.')
        input_queue.consume(
            get_count_function(
                values_by_civ,
                append_to_results_function
            ),
        )

        logger.info(f'All players in matches received grouped.')
        return values_by_civ
    return group_players_by_civ


def get_send_results_by_civ_function(append_to_data_to_send_function):
    # python function currying
    def send_results_by_civ(output_queue, results_by_civ):
        logger.info(
            f"Results per civ from all matches counted: {results_by_civ}. Sending it to next stage")
        data_to_send = []
        for civ, result in results_by_civ.items():
            append_to_data_to_send_function(data_to_send, civ, result)
        output_queue.send_list_of_columns(data_to_send)
    return send_results_by_civ


def main_group_by_civ_reducer(append_to_results_function, append_to_data_to_send_function):
    main_reducer(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_QUEUE_NAME_KEY, logger),
        get_group_players_by_civ_function(append_to_results_function),
        get_send_results_by_civ_function(append_to_data_to_send_function)
    )
