import pika
import os

from common.partition_function import get_key, get_posibles_keys
from communications.constants import CLIENT_TO_WEAKER_WINNER_QUEUE_NAME, FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME, GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME, GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, STRING_COLUMN_SEPARATOR, \
    STRING_ENCODING, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, STRING_LINE_SEPARATOR, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import send_sentinel


def get_send_all_reducers_finished_sentinel_function(sentinel_received_amount, sentinels_objetive):
    # python function currying
    def send_all_reducers_finished_sentinel(channel, method, properties, body):
        if body.decode(STRING_ENCODING) == SENTINEL_MESSAGE:
            sentinel_received_amount[0] += 1
            # TODO que sea la cantidad de reducers cuando sea configurable
            print(
                f"Sentinels from group by match reducers received: {sentinel_received_amount[0]} / {sentinels_objetive}")
            if sentinel_received_amount[0] == sentinels_objetive:
                channel.stop_consuming()
    return send_all_reducers_finished_sentinel


def receive_a_sentinel_per_reducer(channel, reducers_amount):
    channel.queue_declare(queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME)
    sentinel_received_amount = [0]  # using a list to pass by reference
    channel.basic_consume(
        queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME,
        on_message_callback=get_send_all_reducers_finished_sentinel_function(
            sentinel_received_amount,
            reducers_amount),
        auto_ack=True  # TODO sacar esto
    )
    channel.start_consuming()


PLAYERS_CHUNK_SIZE = 100

def send_players_by_key(channel, players_by_key, check_chunk_size=True):
    for key, players in list(players_by_key.items()):
        if check_chunk_size and len(players) > PLAYERS_CHUNK_SIZE:
            players_string = STRING_LINE_SEPARATOR.join(players)
            channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                                  routing_key=key,
                                  body=players_string.encode(STRING_ENCODING))
            players_by_key.pop(key)
            del players


def send_to_reducers(players_by_key, channel, received_players):
    for player_string in received_players:
        key = get_key(player_string.split(STRING_COLUMN_SEPARATOR))
        rows_list = players_by_key.get(key, [])
        rows_list.append(player_string)
        players_by_key[key] = rows_list

    send_players_by_key(channel, players_by_key)


def send_sentinel_to_reducers(channel):
    # TODO mandar uno a cada reducer no a las keys? seria mejor, el tema es que no tengo forma de mandar algo a todos los reducers ahora, este meotod sirve
    for key in get_posibles_keys():
        channel.basic_publish(exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
                              routing_key=key,
                              body=SENTINEL_MESSAGE.encode(STRING_ENCODING))


def get_dispach_to_reducers_function(players_by_key):
    def dispach_to_reducers(channel, method, properties, body):
        # TODO codigo repetido lo del receive sentinel
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print(
                "Sentinel message received, stoping receive players and dispach it to reducers. Sending sentinel to reducers for aleting them than no more players will come.")
            channel.stop_consuming()
            # send the remaining players
            send_players_by_key(channel, players_by_key, False)
            send_sentinel_to_reducers(channel)
        else:
            received_players = [player_string for player_string in chunk_string.split(STRING_LINE_SEPARATOR)]
            send_to_reducers(players_by_key, channel, received_players)
    return dispach_to_reducers

def receive_and_dispach_players(channel):
    channel.queue_declare(queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME)
    channel.exchange_declare(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        exchange_type='direct')

    players_by_key = {}
    channel.basic_consume(
        queue=CLIENT_TO_WEAKER_WINNER_QUEUE_NAME,
        on_message_callback=get_dispach_to_reducers_function(players_by_key),
        auto_ack=True  # TODO sacar esto
    )
    channel.start_consuming()

def main():
    reducers_amount = int(os.environ["REDUCERS_AMOUNT"]) # TODO usar codigo unificado cuando esté
    posibles_keys = get_posibles_keys()
    # TODO falta que las posibles keys dependan de la cantidad de reducers, ojo cuando hay mas reducers que keys
    
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME)

    # TODO quiza esto seria mas facil hacerlo directamente en el start up, ya le pongo las keys en env a cada reducer
    # Porque ahora anda pero es medio polemico es tema de que todos los nodos esten escuchando keys solo con el depends_on, sino hay que meter otra barrera antes
    for key in posibles_keys:
        # as it is round robin, all reducers will get equitative keys amount
        channel.basic_publish(
            exchange='',
            routing_key=GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME,
            body=key.encode(STRING_ENCODING)
        )
    for _ in range(0, reducers_amount):
        send_sentinel(channel, GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME)

    receive_a_sentinel_per_reducer(channel, reducers_amount)
    print("Sentinels from all group by match reducers received, starting to receive players and dispach it to reducers")

    receive_and_dispach_players(channel)

    receive_a_sentinel_per_reducer(channel, reducers_amount)
    print("Sentinels from all group by match reducers received, sending sentinel to client")
    channel.queue_declare(
        queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)
    send_sentinel(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)

    connection.close()

if __name__ == '__main__':
    main()
