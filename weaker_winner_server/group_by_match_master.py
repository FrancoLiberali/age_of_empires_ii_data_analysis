import pika
import os

from common.constants import SENTINEL_KEY
from common.partition_function import PartitionFunction
from communications.constants import CLIENT_TO_WEAKER_WINNER_QUEUE_NAME, \
    FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME, \
    GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME, \
    GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, \
    STRING_COLUMN_SEPARATOR, \
    STRING_ENCODING, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, STRING_LINE_SEPARATOR, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import send_sentinel, send_string_to_queue


def get_receive_sentinel_function(sentinel_received_amount, sentinels_objetive):
    # python function currying
    def receive_sentinel(channel, method, properties, body):
        if body.decode(STRING_ENCODING) == SENTINEL_MESSAGE:
            sentinel_received_amount[0] += 1
            print(f"Sentinels from group by match reducers received: {sentinel_received_amount[0]} / {sentinels_objetive}")
            if sentinel_received_amount[0] == sentinels_objetive:
                channel.stop_consuming()
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return receive_sentinel


def receive_a_sentinel_per_reducer(channel, reducers_amount):
    channel.queue_declare(queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME)
    sentinel_received_amount = [0]  # using a list to pass by reference
    channel.basic_consume(
        queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME,
        on_message_callback=get_receive_sentinel_function(
            sentinel_received_amount,
            reducers_amount),
    )
    channel.start_consuming()


PLAYERS_CHUNK_SIZE = 100 # TODO envvar, es muy importante

def send_players_by_key(channel, players_by_key, check_chunk_size=True):
    for key, players in list(players_by_key.items()):
        if len(players) > PLAYERS_CHUNK_SIZE or not check_chunk_size:
            players_string = STRING_LINE_SEPARATOR.join(players)
            channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                                  routing_key=key,
                                  body=players_string.encode(STRING_ENCODING))
            players_by_key.pop(key)
            del players


def add_to_players_by_key(channel, partition_function, players_by_key, received_players):
    for player_string in received_players:
        key = partition_function.get_key(
            player_string.split(STRING_COLUMN_SEPARATOR)
        )
        rows_list = players_by_key.get(key, [])
        rows_list.append(player_string)
        players_by_key[key] = rows_list

    send_players_by_key(channel, players_by_key)


def send_sentinel_to_reducers(channel):
    channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                          routing_key=SENTINEL_KEY,
                          body=SENTINEL_MESSAGE.encode(STRING_ENCODING))


def get_dispach_to_reducers_function(players_by_key, partition_function):
    def dispach_to_reducers(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print(
                "Sentinel message received, stoping receive players and dispach it to reducers. Sending sentinel to reducers for alerting them than no more players will come.")
            channel.stop_consuming()
            # send the remaining players
            send_players_by_key(channel, players_by_key, False)
            send_sentinel_to_reducers(channel)
        else:
            received_players = [player_string for player_string in chunk_string.split(STRING_LINE_SEPARATOR)]
            add_to_players_by_key(
                channel,
                partition_function,
                players_by_key,
                received_players
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return dispach_to_reducers


def receive_and_dispach_players(channel, partition_function, reducers_amount):
    channel.queue_declare(queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME)
    channel.exchange_declare(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        exchange_type='direct')

    players_by_key = {}
    channel.basic_consume(
        queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME,
        on_message_callback=get_dispach_to_reducers_function(
            players_by_key, partition_function),
    )
    print("Starting to receive players from client and dispach it to reducers by key")
    channel.start_consuming()

    print("Waiting for a sentinel per reducer that notifies they finished grouping")
    receive_a_sentinel_per_reducer(channel, reducers_amount)


def send_keys_to_reducers(channel, partition_function, reducers_amount):
    channel.queue_declare(queue=GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME)

    posibles_keys = partition_function.get_posibles_keys()
    # TODO quiza esto seria mas facil hacerlo directamente en el start up, ya le pongo las keys en env a cada reducer
    # Porque ahora anda pero es medio polemico es tema de que todos los nodos esten escuchando keys solo con el depends_on, sino hay que meter otra barrera antes
    print(f"Starting to send keys to reducers: {posibles_keys}")
    for key in posibles_keys:
        # as it is round robin, all reducers will get equitative keys amount
        send_string_to_queue(
            channel,
            GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME,
            key
        )
    print("All keys sended, sending sentinels to notify reducers that no more keys are going to be sended")
    for _ in range(0, reducers_amount):
        send_sentinel(channel, GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME)

    print("Waiting for a sentinel per reducer that notifies they are subscribed to the corresponding keys")
    receive_a_sentinel_per_reducer(channel, reducers_amount)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    reducers_amount = int(os.environ["REDUCERS_AMOUNT"]) # TODO usar codigo unificado cuando esté
    partition_function = PartitionFunction(reducers_amount)

    send_keys_to_reducers(channel, partition_function, reducers_amount)

    receive_and_dispach_players(channel, partition_function, reducers_amount)

    print("Sending sentinel to client to notify all matches ids sended")
    channel.queue_declare(
        queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)
    send_sentinel(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)

    connection.close()

if __name__ == '__main__':
    main()
