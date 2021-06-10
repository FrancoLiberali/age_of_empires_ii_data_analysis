import pika
import os

from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    JOIN_REDUCERS_BARRIER_QUEUE_NAME, \
    JOIN_MASTER_TO_REDUCERS_QUEUE_NAME, JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    MATCHES_KEY, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    PLAYERS_KEY,\
    STRING_COLUMN_SEPARATOR, \
    STRING_ENCODING, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, STRING_LINE_SEPARATOR, \
    MATCHES_TO_JOIN_MASTER_EXCHANGE_NAME, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME, \
    SENTINEL_KEY
from communications.rabbitmq_interface import send_sentinel_to_queue, send_string_to_queue
from partition_function.partition_function import PartitionFunction

# TODO codigo repetido con master de groupby

OUTPUT_EXCHANGE_NAME = JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME # TODO envvar
MATCHES_INPUT_EXCHANGE_NAME = MATCHES_TO_JOIN_MASTER_EXCHANGE_NAME  # TODO envvar
MATCHES_INPUT_EXCHANGE_TYPE = "direct"  # TODO envvar
PLAYERS_INPUT_EXCHANGE_NAME = PLAYERS_FANOUT_EXCHANGE_NAME  # TODO envvar
PLAYERS_INPUT_EXCHANGE_TYPE = "fanout"  # TODO envvar
KEYS_QUEUE_NAME = JOIN_MASTER_TO_REDUCERS_QUEUE_NAME  # TODO envvar
BARRIER_QUEUE_NAME = JOIN_REDUCERS_BARRIER_QUEUE_NAME  # TODO envvar

def get_receive_sentinel_function(sentinel_received_amount, sentinels_objetive):
    # python function currying
    def receive_sentinel(channel, method, properties, body):
        if body.decode(STRING_ENCODING) == SENTINEL_MESSAGE:
            sentinel_received_amount[0] += 1
            print(
                f"Sentinels from group by match reducers received: {sentinel_received_amount[0]} / {sentinels_objetive}")
            if sentinel_received_amount[0] == sentinels_objetive:
                channel.stop_consuming()
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return receive_sentinel


def receive_a_sentinel_per_reducer(channel, reducers_amount):
    channel.queue_declare(queue=BARRIER_QUEUE_NAME)
    sentinel_received_amount = [0]  # using a list to pass by reference
    channel.basic_consume(
        queue=BARRIER_QUEUE_NAME,
        on_message_callback=get_receive_sentinel_function(
            sentinel_received_amount,
            reducers_amount),
    )
    channel.start_consuming()


ROWS_CHUNK_SIZE = 100  # TODO envvar, es muy importante

def send_dict_by_key(channel, dict_by_key, tag_to_send, check_chunk_size=True):
    for key, rows in list(dict_by_key.items()):
        if len(rows) > ROWS_CHUNK_SIZE or not check_chunk_size:
            rows_string = STRING_LINE_SEPARATOR.join([tag_to_send] + rows)
            channel.basic_publish(exchange=OUTPUT_EXCHANGE_NAME,
                                  routing_key=key,
                                  body=rows_string.encode(STRING_ENCODING))
            dict_by_key.pop(key)
            del rows


def add_to_dict_by_key(channel,
                       partition_function,
                       dict_by_key,
                       received_rows,
                       match_id_index,
                       tag_to_send):
    for row_string in received_rows:
        key = partition_function.get_key(
            row_string.split(STRING_COLUMN_SEPARATOR)[match_id_index]
        )
        rows_list = dict_by_key.get(key, [])
        rows_list.append(row_string)
        dict_by_key[key] = rows_list

    send_dict_by_key(channel, dict_by_key, tag_to_send)

# TODO unificar con el client


def send_sentinel_to_reducers(channel):
    channel.basic_publish(exchange=OUTPUT_EXCHANGE_NAME,
                          routing_key=SENTINEL_KEY,
                          body=SENTINEL_MESSAGE.encode(STRING_ENCODING))

def get_dispach_to_reducers_function(players_by_key, matches_by_key, partition_function):
    def dispach_to_reducers(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print(
                "Sentinel message received, stoping receive and dispach it to reducers. Sending sentinel to reducers for alerting them than no more players will come.")
            channel.stop_consuming()
            # send the remaining players and matches
            send_dict_by_key(channel, players_by_key, PLAYERS_KEY, False)
            send_dict_by_key(channel, matches_by_key, MATCHES_KEY, False)
            send_sentinel_to_reducers(channel)
        else:
            if method.routing_key == PLAYERS_KEY:
                received_players = [
                    player_string for player_string in chunk_string.split(STRING_LINE_SEPARATOR)]
                add_to_dict_by_key(
                    channel,
                    partition_function,
                    players_by_key,
                    received_players,
                    FROM_CLIENT_PLAYER_MATCH_INDEX,
                    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR
                )
            elif method.routing_key == MATCHES_KEY:
                received_matches = [
                    match_string for match_string in chunk_string.split(STRING_LINE_SEPARATOR)]
                add_to_dict_by_key(
                    channel,
                    partition_function,
                    matches_by_key,
                    received_matches,
                    FROM_CLIENT_MATCH_TOKEN_INDEX,
                    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR
                )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return dispach_to_reducers


def receive_and_dispach_players_and_matches(channel, partition_function, reducers_amount):
    channel.exchange_declare(
        exchange=MATCHES_INPUT_EXCHANGE_NAME,
        exchange_type=MATCHES_INPUT_EXCHANGE_TYPE)
    channel.exchange_declare(
        exchange=PLAYERS_INPUT_EXCHANGE_NAME,
        exchange_type=PLAYERS_INPUT_EXCHANGE_TYPE)

    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    channel.queue_bind(
        exchange=MATCHES_INPUT_EXCHANGE_NAME,
        queue=private_queue_name,
        routing_key=MATCHES_KEY
    )
    channel.queue_bind(
        exchange=PLAYERS_INPUT_EXCHANGE_NAME,
        queue=private_queue_name,
        routing_key=PLAYERS_KEY
    )

    players_by_key = {}
    matches_by_key = {}
    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=get_dispach_to_reducers_function(
            players_by_key,
            matches_by_key,
            partition_function
        )
    )

    print("Starting to receive players and matches and dispach it to reducers by key")
    channel.start_consuming()

    print("Waiting for a sentinel per reducer that notifies they finished")
    receive_a_sentinel_per_reducer(channel, reducers_amount)


def send_keys_to_reducers(channel, partition_function, reducers_amount):
    channel.queue_declare(queue=KEYS_QUEUE_NAME)

    posibles_keys = partition_function.get_posibles_keys()
    # TODO quiza esto seria mas facil hacerlo directamente en el start up, ya le pongo las keys en env a cada reducer
    # Porque ahora anda pero es medio polemico es tema de que todos los nodos esten escuchando keys solo con el depends_on, sino hay que meter otra barrera antes
    print(f"Starting to send keys to reducers: {posibles_keys}")
    for key in posibles_keys:
        # as it is round robin, all reducers will get equitative keys amount
        send_string_to_queue(
            channel,
            KEYS_QUEUE_NAME,
            key
        )
    print("All keys sended, sending sentinels to notify reducers that no more keys are going to be sended")
    for _ in range(0, reducers_amount):
        send_sentinel_to_queue(
            channel, KEYS_QUEUE_NAME)

    print("Waiting for a sentinel per reducer that notifies they are subscribed to the corresponding keys")
    receive_a_sentinel_per_reducer(channel, reducers_amount)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # TODO usar codigo unificado cuando esté
    reducers_amount = int(os.environ["REDUCERS_AMOUNT"])
    partition_function = PartitionFunction(reducers_amount)

    send_keys_to_reducers(channel, partition_function, reducers_amount)

    receive_and_dispach_players_and_matches(channel, partition_function, reducers_amount)

    print("Sending sentinel to client to notify all matches ids sended")
    channel.queue_declare(
        queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)
    send_sentinel_to_queue(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)

    connection.close()


if __name__ == '__main__':
    main()
