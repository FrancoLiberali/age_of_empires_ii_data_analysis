from master_reducers_arq.players_master import players_master_main
from communications.constants import GROUP_BY_MATCH_MASTER_PLAYERS_INPUT_QUEUE_NAME, \
    GROUP_BY_MATCH_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import ExchangeInterface, QueueInterface


def subscribe_to_entries(connection):
    input_exchage = ExchangeInterface.newFanout(
        connection, PLAYERS_FANOUT_EXCHANGE_NAME)
    # TODO guarda last hash solo para usarlo en el sentinel final, no hay posibles duplicados a la entrada
    input_queue = QueueInterface(
        connection, GROUP_BY_MATCH_MASTER_PLAYERS_INPUT_QUEUE_NAME)
    input_queue.bind(input_exchage)

    return input_queue


def main():
    players_master_main(
        GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME,
        WEAKER_WINNER_TO_CLIENT_QUEUE_NAME,
        GROUP_BY_MATCH_MASTER_TO_REDUCERS_EXCHANGE_NAME,
        subscribe_to_entries,
        False
    )

if __name__ == '__main__':
    main()
