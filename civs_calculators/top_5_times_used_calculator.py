from config.envvars import INPUT_QUEUE_NAME_KEY, get_config_param
from communications.constants import TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import QueueInterface, RabbitMQConnection, split_columns_into_list, split_rows_into_list
from logger.logger import Logger

FROM_GROUP_BY_CIV_CIV_INDEX = 0
FROM_GROUP_BY_CIV_TIMES_INDEX = 1


def get_group_times_used_by_civ_function(times_used_by_civ):
    def group_wins_and_defeats_by_civ(queue, received_string, _):
        for civ_and_times_used in split_rows_into_list(received_string):
            columns = split_columns_into_list(civ_and_times_used)
            civ = columns[FROM_GROUP_BY_CIV_CIV_INDEX]
            times_used_of_civ = times_used_by_civ.get(
                civ, 0)
            times_used_of_civ += int(
                columns[FROM_GROUP_BY_CIV_TIMES_INDEX]
            )
            times_used_by_civ[civ] = times_used_of_civ
    return group_wins_and_defeats_by_civ

CIV_INDEX = 0
TIMES_INDEX = 1
TOP_LEN = 5

# TODO codigo repetido con el otro calculator
def main():
    logger = Logger()

    connection = RabbitMQConnection()
    input_queue = QueueInterface(
        connection,
        get_config_param(INPUT_QUEUE_NAME_KEY, logger)
    )
    output_queue = QueueInterface(
        connection, TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    times_used_by_civ = {}
    logger.info('Starting to receive times used by civ to calculate top 5')
    input_queue.consume(
        get_group_times_used_by_civ_function(
            times_used_by_civ
        )
    )

    logger.info('All times used received, calculating top 5')
    times_used_list = times_used_by_civ.items()
    sorted_list = sorted(
        times_used_list,
        key=lambda civ_and_times: civ_and_times[TIMES_INDEX],
        reverse=True
    )
    if len(sorted_list) > TOP_LEN:
        top_5 = list(map(
            lambda civ_and_times:
                (civ_and_times[CIV_INDEX], str(civ_and_times[TIMES_INDEX])),
            sorted_list[0:TOP_LEN]
        ))
    else:
        top_5 = list(map(lambda civ, times: (civ, str(times)), sorted_list))

    output_queue.send_list_of_columns(top_5)
    connection.close()


if __name__ == '__main__':
    main()
