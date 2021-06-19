from communications.constants import GROUP_BY_CIV_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    GROUP_BY_CIV_MASTER_TO_REDUCERS_QUEUE_NAME, \
    GROUP_BY_CIV_REDUCERS_BARRIER_QUEUE_NAME, \
    GROUP_BY_CIV_REDUCERS_TO_WINNER_RATE_CALCULATOR_QUEUE_NAME, \
    PLAYER_LOSER, \
    PLAYER_WINNER, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    SENTINEL_MESSAGE
from communications.rabbitmq_interface import send_list_of_columns_to_queue
from master_reducers_arq.reducer import main_reducer

INPUT_EXCHANGE_NAME = GROUP_BY_CIV_MASTER_TO_REDUCERS_EXCHANGE_NAME
BARRIER_QUEUE_NAME = GROUP_BY_CIV_REDUCERS_BARRIER_QUEUE_NAME
KEYS_QUEUE_NAME = GROUP_BY_CIV_MASTER_TO_REDUCERS_QUEUE_NAME
OUTPUT_QUEUE_NAME = GROUP_BY_CIV_REDUCERS_TO_WINNER_RATE_CALCULATOR_QUEUE_NAME

FROM_JOIN_PLAYER_WINNER_INDEX = 1
FROM_JOIN_PLAYER_CIV_INDEX = 2
WINS_INDEX = 0
DEFEATS_INDEX = 1

def get_count_wins_and_defeats_by_civ_function(wins_and_defeats_by_civ):
    # python function currying
    def count_wins_and_defeats_by_civ(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping joining players and matches")
            channel.stop_consuming()
        else:
            players_rows = chunk_string.split(STRING_LINE_SEPARATOR)
            for player_string in players_rows:
                player_columns = player_string.split(STRING_COLUMN_SEPARATOR)
                player_civ = player_columns[FROM_JOIN_PLAYER_CIV_INDEX]

                wins_and_defeats_of_civ = wins_and_defeats_by_civ.get(
                    player_civ, [0, 0])

                if player_columns[FROM_JOIN_PLAYER_WINNER_INDEX] == PLAYER_WINNER:
                    wins_and_defeats_of_civ[WINS_INDEX] += 1
                elif player_columns[FROM_JOIN_PLAYER_WINNER_INDEX] == PLAYER_LOSER:
                    wins_and_defeats_of_civ[DEFEATS_INDEX] += 1

                wins_and_defeats_by_civ[player_civ] = wins_and_defeats_of_civ
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return count_wins_and_defeats_by_civ


def group_players_by_civ(channel, private_queue_name, keys):
    wins_and_defeats_by_civ = {}
    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=get_count_wins_and_defeats_by_civ_function(
            wins_and_defeats_by_civ),
    )
    print(
        f'Starting to receive players in matches with keys {keys} to group by civ.')

    channel.start_consuming()
    print(f'All players in matches with keys {keys} grouped.')
    return wins_and_defeats_by_civ


def send_wins_and_defeats_by_civ(channel, wins_and_defeats_by_civ, keys):
    print(
        f"Wins and defeats per civ from all matches with keys {keys} counted: {wins_and_defeats_by_civ}. Sending it to win rate calculator")
    channel.queue_declare(queue=OUTPUT_QUEUE_NAME)
    data_to_send = []
    for civ, wins_and_defeats in wins_and_defeats_by_civ.items():
        data_to_send.append(
            [civ] + [str(value) for value in wins_and_defeats]
        )
    send_list_of_columns_to_queue(channel, OUTPUT_QUEUE_NAME, data_to_send)

def main():
    main_reducer(
        KEYS_QUEUE_NAME,
        BARRIER_QUEUE_NAME,
        INPUT_EXCHANGE_NAME,
        group_players_by_civ,
        send_wins_and_defeats_by_civ
    )

if __name__ == '__main__':
    main()
