from multiprocessing import Process
import pika

from common.constants import LOSER, MATCH_INDEX, WINNER, WINNER_INDEX, RATING_INDEX
from common.partition_function import get_posibles_keys
from communications.constants import FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME, GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import send_matches_ids, send_sentinel

def can_match_be_1_vs_1(players_list, new_player):
    return (len(players_list) == 0 or (len(players_list) == 1 and players_list[0][WINNER_INDEX] != new_player[WINNER_INDEX]))


def get_group_by_match_function(players_by_match):
    # python function currying
    def group_by_match(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping consuming")
            channel.stop_consuming()
        else:
            for player_string in chunk_string.split(STRING_LINE_SEPARATOR):
                player_columns = player_string.split(STRING_COLUMN_SEPARATOR)
                match_id = player_columns[MATCH_INDEX]
                players_of_match = players_by_match.get(match_id, [])
                if can_match_be_1_vs_1(players_of_match, player_columns):
                    players_of_match.append(player_columns)
                    players_by_match[match_id] = players_of_match
                else:
                    del players_by_match[match_id]
    return group_by_match


MINIMUM_RATING_PROCENTAGE_DIFF = 30 # TODO envvar

def group_by_match_server(key):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.exchange_declare(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        exchange_type='direct')

    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    channel.queue_bind(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        queue=private_queue_name,
        routing_key=key)

    players_by_match = {}
    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=get_group_by_match_function(players_by_match),
        auto_ack=True  # TODO sacar esto
    )
    print('Waiting for messages. To exit press CTRL+C')

    channel.start_consuming()

    matches_ids = []
    for match_id, players_list in players_by_match.items():
        # final check that all matches are of two players
        # some of them could have size 1 if only one of the players has been filtered in previous stage
        if len(players_list) == 2:
            winner = next((player for player in players_list if player[WINNER_INDEX] == WINNER))
            loser = next((player for player in players_list if player[WINNER_INDEX] == LOSER))

            rating_diff = (int(loser[RATING_INDEX]) - int(winner[RATING_INDEX])) / \
                int(winner[RATING_INDEX]) * 100
            if rating_diff > MINIMUM_RATING_PROCENTAGE_DIFF:
                matches_ids.append(match_id)
        else:
            del players_list
    print(f"Routing Key: {key} - Matches: {matches_ids}")

    if len(matches_ids) > 0:
        channel.queue_declare(queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)
        send_matches_ids(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME, matches_ids)

    channel.queue_declare(queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME)
    send_sentinel(channel, GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME)
    connection.close()


def get_send_all_reducers_finished_sentinel_function(sentinel_received_amount):
    # python function currying
    def send_all_reducers_finished_sentinel(channel, method, properties, body):
        if body.decode(STRING_ENCODING) == SENTINEL_MESSAGE:
            sentinel_received_amount[0] = sentinel_received_amount[0] + 1
            # TODO que sea la cantidad de reducers cuando sea configurable
            sentinels_objetive = len(get_posibles_keys())
            print(
                f"Sentinels from group by match reducers received: {sentinel_received_amount[0]} / {sentinels_objetive}")
            if sentinel_received_amount[0] == sentinels_objetive:
                print("Sentinels from all group by match reducers received, sending sentinel to client")
                send_sentinel(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)
                channel.stop_consuming()
    return send_all_reducers_finished_sentinel

def main():
    # TODO pasar a distintos nodos, no procesos porque debe ser multicomputadoras
    processes = []
    # TODO que sea parametro de entrada y que se repartan las keys entre ellos
    for key in get_posibles_keys():
        process = Process(
            target=group_by_match_server,
            args=((key),)
        )
        process.start()
        processes.append(process)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME)

    sentinel_received_amount = [0] # using a list to pass by reference
    channel.basic_consume(
        queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME,
        on_message_callback=get_send_all_reducers_finished_sentinel_function(
            sentinel_received_amount),
        auto_ack=True  # TODO sacar esto
    )

    channel.start_consuming()
    connection.close()

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
