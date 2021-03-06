from civs_calculators_arq.civ_calculator import main_civ_calculator, write_to_new_data
from communications.constants import TOP_5_USED_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME
from communications.rabbitmq_interface import split_columns_into_list, split_rows_into_list

FROM_GROUP_BY_CIV_CIV_INDEX = 0
FROM_GROUP_BY_CIV_TIMES_INDEX = 1

def get_group_times_used_by_civ_function(times_used_by_civ):
    def group_wins_and_defeats_by_civ(queue, received_string, _, actual_hash):
        for civ_and_times_used in split_rows_into_list(received_string, skip_header=True):
            columns = split_columns_into_list(civ_and_times_used)
            civ = columns[FROM_GROUP_BY_CIV_CIV_INDEX]
            times_used_of_civ = times_used_by_civ.get(
                civ, 0)
            times_used_of_civ += int(
                columns[FROM_GROUP_BY_CIV_TIMES_INDEX]
            )
            times_used_by_civ[civ] = times_used_of_civ
        write_to_new_data(times_used_by_civ, actual_hash)
    return group_wins_and_defeats_by_civ


CIV_INDEX = 0
TIMES_INDEX = 1
TOP_LEN = 5


def calculate_top_5(logger, times_used_by_civ):
    logger.info('All times used received, calculating top 5')
    logger.debug(f'times_used_by_civ: {times_used_by_civ}')
    times_used_list = times_used_by_civ.items()
    sorted_list = sorted(
        times_used_list,
        key=lambda civ_and_times: civ_and_times[TIMES_INDEX],
        reverse=True
    )
    if len(sorted_list) > TOP_LEN:
        sorted_list = sorted_list[0:TOP_LEN]
    top_5 = list(
        map(
            lambda civ_and_times: (civ_and_times[CIV_INDEX], str(civ_and_times[TIMES_INDEX])),
            sorted_list
        )
    )
    return top_5

def main():
    main_civ_calculator(
        TOP_5_USED_CALCULATOR_TO_AUTHORIZATOR_QUEUE_NAME,
        get_group_times_used_by_civ_function,
        calculate_top_5
    )


if __name__ == '__main__':
    main()
