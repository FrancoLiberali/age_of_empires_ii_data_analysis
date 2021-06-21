import os
from communications.constants import FROM_CLIENT_PLAYER_MATCH_INDEX, \
    STRING_COLUMN_SEPARATOR, \
    STRING_ENCODING, \
    SENTINEL_MESSAGE, \
    STRING_LINE_SEPARATOR
from master_reducers_arq.master import main_master

PLAYERS_INPUT_QUEUE_NAME = os.environ["PLAYERS_INPUT_QUEUE_NAME"]
OUTPUT_EXCHANGE_NAME = os.environ["OUTPUT_EXCHANGE_NAME"]
KEYS_QUEUE_NAME = os.environ["KEYS_QUEUE_NAME"]
BARRIER_QUEUE_NAME = os.environ["BARRIER_QUEUE_NAME"]
REDUCERS_OUTPUT_QUEUE_NAME = os.environ["REDUCERS_OUTPUT_QUEUE_NAME"]

PLAYERS_CHUNK_SIZE = 100  # TODO envvar, es muy importante.

def send_players_by_key(channel, players_by_key, check_chunk_size=True):
    for key, players in list(players_by_key.items()):
        if len(players) > PLAYERS_CHUNK_SIZE or not check_chunk_size:
            players_string = STRING_LINE_SEPARATOR.join(players)
            channel.basic_publish(exchange=OUTPUT_EXCHANGE_NAME,
                                  routing_key=key,
                                  body=players_string.encode(STRING_ENCODING))
            players_by_key.pop(key)
            del players


def add_to_players_by_key(channel, partition_function, players_by_key, received_players):
    for player_string in received_players:
        key = partition_function.get_key(
            player_string.split(STRING_COLUMN_SEPARATOR)[FROM_CLIENT_PLAYER_MATCH_INDEX]
        )
        rows_list = players_by_key.get(key, [])
        rows_list.append(player_string)
        players_by_key[key] = rows_list

    send_players_by_key(channel, players_by_key)


def get_dispach_to_reducers_function(players_by_key, partition_function):
    def dispach_to_reducers(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping receive players and dispach it to reducers.")
            channel.stop_consuming()
            # send the remaining players
            send_players_by_key(channel, players_by_key, False)
        else:
            received_players = [
                player_string for player_string in chunk_string.split(STRING_LINE_SEPARATOR)]
            add_to_players_by_key(
                channel,
                partition_function,
                players_by_key,
                received_players
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return dispach_to_reducers


def receive_and_dispach_players(channel, input_queue_name, partition_function):
    players_by_key = {}
    channel.basic_consume(
        queue=input_queue_name,
        on_message_callback=get_dispach_to_reducers_function(
            players_by_key, partition_function),
    )
    print("Starting to receive players from client and dispach it to reducers by key")
    channel.start_consuming()

def declare_input_queue(channel):
    channel.queue_declare(queue=PLAYERS_INPUT_QUEUE_NAME)
    return PLAYERS_INPUT_QUEUE_NAME

def main():
    main_master(
        KEYS_QUEUE_NAME,
        BARRIER_QUEUE_NAME,
        REDUCERS_OUTPUT_QUEUE_NAME,
        OUTPUT_EXCHANGE_NAME,
        declare_input_queue,
        receive_and_dispach_players
    )


if __name__ == '__main__':
    main()
