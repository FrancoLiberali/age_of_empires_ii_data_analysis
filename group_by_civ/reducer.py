from communications.rabbitmq_interface import SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR, split_columns_into_list, split_rows_into_list
from config.envvars import BARRIER_QUEUE_NAME_KEY, OUTPUT_QUEUE_NAME_KEY, REDUCER_ID_KEY, get_config_param
from master_reducers_arq.reducer import main_reducer
from logger.logger import Logger

logger = Logger()


def get_count_function(append_to_results_function, append_to_data_to_send_function, output_queue):
    header_line_constant_part = f"{get_config_param(REDUCER_ID_KEY, logger)}{SENTINEL_MESSAGE_WITH_REDUCER_ID_SEPARATOR}"
    # python function currying
    def count(queue, received_string, _, __):
        results_by_civ = {}
        for player_string in split_rows_into_list(received_string):
            player_columns = split_columns_into_list(player_string)
            append_to_results_function(results_by_civ, player_columns)
        data_to_send = []
        for civ, result in results_by_civ.items():
            append_to_data_to_send_function(data_to_send, civ, result)
        # TODO ojo con esto, al usar queue.last_hash no se esta usando el actual sino el anterior, aunque igualmente deberia funcionar
        output_queue.send_list_of_columns(
            data_to_send,
            header_line=f"{header_line_constant_part}{queue.last_hash}"
        )
    return count


def get_group_players_by_civ_function(append_to_results_function, append_to_data_to_send_function):
    # python function currying
    def group_players_by_civ(input_queue, output_queue, send_sentinel_to_master_function):
        logger.info(
            f'Starting to receive players in matches to group by civ.')
        input_queue.consume(
            get_count_function(
                append_to_results_function,
                append_to_data_to_send_function,
                output_queue
            ),
            on_sentinel_callback=send_sentinel_to_master_function
        )

        logger.info(f'All players in matches received grouped.')
    return group_players_by_civ

def main_group_by_civ_reducer(append_to_results_function, append_to_data_to_send_function):
    main_reducer(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_QUEUE_NAME_KEY, logger),
        get_group_players_by_civ_function(
            append_to_results_function,
            append_to_data_to_send_function
        ),
    )
