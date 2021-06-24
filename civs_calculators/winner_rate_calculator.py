from config.envvars import INPUT_QUEUE_NAME_KEY, get_config_param
from communications.constants import STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import QueueInterface, RabbitMQConnection
from logger.logger import Logger

FROM_GROUP_BY_CIV_CIV_INDEX = 0
FROM_GROUP_BY_CIV_WINS_INDEX = 1
FROM_GROUP_BY_CIV_DEFEATS_INDEX = 2

WINS_INDEX = 0
DEFEATS_INDEX = 1

def get_group_wins_and_defeats_by_civ_function(wins_and_defeats_by_civ):
    def group_wins_and_defeats_by_civ(queue, received_string, _):
        for civ_wins_and_defeats in received_string.split(STRING_LINE_SEPARATOR):
            columns = civ_wins_and_defeats.split(STRING_COLUMN_SEPARATOR)
            civ = columns[FROM_GROUP_BY_CIV_CIV_INDEX]
            wins_and_defeats_of_civ = wins_and_defeats_by_civ.get(
                civ, [0, 0])
            wins_and_defeats_of_civ[WINS_INDEX] += int(columns[FROM_GROUP_BY_CIV_WINS_INDEX])
            wins_and_defeats_of_civ[DEFEATS_INDEX] += int(columns[FROM_GROUP_BY_CIV_DEFEATS_INDEX])
            wins_and_defeats_by_civ[civ] = wins_and_defeats_of_civ
    return group_wins_and_defeats_by_civ


def main():
    logger = Logger()

    connection = RabbitMQConnection()
    input_queue = QueueInterface(
        connection,
        get_config_param(INPUT_QUEUE_NAME_KEY, logger)
    )
    output_queue = QueueInterface(
        connection, WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    wins_and_defeats_by_civ = {}
    logger.info(
        'Starting to receive wins and defeats by civ to calculate winner rate')
    input_queue.consume(
        get_group_wins_and_defeats_by_civ_function(
            wins_and_defeats_by_civ
        ),
    )

    logger.info('All wins and defeats received, calculating winner rate')
    winner_rates = {}
    for civ, wins_and_defeats in wins_and_defeats_by_civ.items():
        wins = wins_and_defeats[WINS_INDEX]
        defeats = wins_and_defeats[DEFEATS_INDEX]
        winner_rates[civ] = str(wins / (wins + defeats) * 100)

    sorted_by_winner_rate = sorted(
        winner_rates.items(), key=lambda x: x[1], reverse=True)

    output_queue.send_list_of_columns(
        sorted_by_winner_rate
    )
    connection.close()


if __name__ == '__main__':
    main()
