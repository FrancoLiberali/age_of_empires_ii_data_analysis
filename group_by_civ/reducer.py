import os

from communications.constants import STRING_COLUMN_SEPARATOR, STRING_LINE_SEPARATOR
from master_reducers_arq.reducer import main_reducer

INPUT_EXCHANGE_NAME = os.environ["INPUT_EXCHANGE_NAME"]
BARRIER_QUEUE_NAME = os.environ["BARRIER_QUEUE_NAME"]
KEYS_QUEUE_NAME = os.environ["KEYS_QUEUE_NAME"]
OUTPUT_QUEUE_NAME = os.environ["OUTPUT_QUEUE_NAME"]

def get_count_function(results_by_civ, append_to_results_function):
    # python function currying
    def count(queue, received_string, _):
        players_rows = received_string.split(STRING_LINE_SEPARATOR)
        for player_string in players_rows:
            player_columns = player_string.split(STRING_COLUMN_SEPARATOR)
            append_to_results_function(results_by_civ, player_columns)
    return count


def get_group_players_by_civ_function(append_to_results_function):
    # python function currying
    def group_players_by_civ(input_queue, output_queue, keys):
        values_by_civ = {}
        print(f'Starting to receive players in matches with keys {keys} to group by civ.')
        input_queue.consume(
            get_count_function(
                values_by_civ,
                append_to_results_function
            ),
        )

        print(f'All players in matches with keys {keys} grouped.')
        return values_by_civ
    return group_players_by_civ


def get_send_results_by_civ_function(append_to_data_to_send_function):
    # python function currying
    def send_results_by_civ(output_queue, results_by_civ, keys):
        print(
            f"Results per civ from all matches with keys {keys} counted: {results_by_civ}. Sending it to next stage")
        data_to_send = []
        for civ, result in results_by_civ.items():
            append_to_data_to_send_function(data_to_send, civ, result)
        output_queue.send_list_of_columns(data_to_send)
    return send_results_by_civ


def main_group_by_civ_reducer(append_to_results_function, append_to_data_to_send_function):
    main_reducer(
        KEYS_QUEUE_NAME,
        BARRIER_QUEUE_NAME,
        INPUT_EXCHANGE_NAME,
        OUTPUT_QUEUE_NAME,
        get_group_players_by_civ_function(append_to_results_function),
        get_send_results_by_civ_function(append_to_data_to_send_function)
    )
