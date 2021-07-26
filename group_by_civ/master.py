from master_reducers_arq.players_master import players_master_main
from config.envvars import BARRIER_QUEUE_NAME_KEY, OUTPUT_EXCHANGE_NAME_KEY, PLAYERS_INPUT_QUEUE_NAME_KEY, REDUCERS_OUTPUT_QUEUE_NAME_KEY, get_config_param
from communications.rabbitmq_interface import LastHashStrategy, QueueInterface
from logger.logger import Logger

logger = Logger()

def declare_input_queue(connection):
    return QueueInterface(
        connection,
        get_config_param(PLAYERS_INPUT_QUEUE_NAME_KEY, logger),
        last_hash_strategy=LastHashStrategy.LAST_HASH_PER_REDUCER_ID
    )

def main():
    players_master_main(
        get_config_param(BARRIER_QUEUE_NAME_KEY, logger),
        get_config_param(REDUCERS_OUTPUT_QUEUE_NAME_KEY, logger),
        get_config_param(OUTPUT_EXCHANGE_NAME_KEY, logger),
        declare_input_queue,
        True
    )


if __name__ == '__main__':
    main()
