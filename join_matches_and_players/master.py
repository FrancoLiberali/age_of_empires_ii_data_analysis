from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    JOIN_REDUCERS_BARRIER_QUEUE_NAME, \
    JOIN_MASTER_TO_REDUCERS_QUEUE_NAME, \
    JOIN_REDUCERS_TO_GROUP_BY_CIV_MASTER_QUEUE_NAME, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    MATCHES_KEY, \
    PLAYERS_FANOUT_EXCHANGE_NAME, \
    PLAYERS_KEY,\
    STRING_COLUMN_SEPARATOR, \
    STRING_ENCODING, \
    SENTINEL_MESSAGE, STRING_LINE_SEPARATOR, \
    MATCHES_TO_JOIN_MASTER_EXCHANGE_NAME
from master_reducers_arq.master import main_master

OUTPUT_EXCHANGE_NAME = JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME
MATCHES_INPUT_EXCHANGE_NAME = MATCHES_TO_JOIN_MASTER_EXCHANGE_NAME
MATCHES_INPUT_EXCHANGE_TYPE = "direct"
PLAYERS_INPUT_EXCHANGE_NAME = PLAYERS_FANOUT_EXCHANGE_NAME
PLAYERS_INPUT_EXCHANGE_TYPE = "fanout"
KEYS_QUEUE_NAME = JOIN_MASTER_TO_REDUCERS_QUEUE_NAME
BARRIER_QUEUE_NAME = JOIN_REDUCERS_BARRIER_QUEUE_NAME
REDUCERS_OUTPUT_QUEUE_NAME = JOIN_REDUCERS_TO_GROUP_BY_CIV_MASTER_QUEUE_NAME


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

INPUTS_AMOUNT = 2

def get_dispach_to_reducers_function(players_by_key, matches_by_key, sentinels_count, partition_function):
    def dispach_to_reducers(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            sentinels_count[0] += 1
            print(f"Sentinel message {sentinels_count[0]}/{INPUTS_AMOUNT} received")
            if sentinels_count[0] == INPUTS_AMOUNT:
                print("Stoping receive and dispach it to reducers.")
                channel.stop_consuming()
                # send the remaining players and matches
                send_dict_by_key(channel, players_by_key, PLAYERS_KEY, False)
                send_dict_by_key(channel, matches_by_key, MATCHES_KEY, False)
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


def receive_and_dispach_players_and_matches(channel, private_queue_name, partition_function):
    players_by_key = {}
    matches_by_key = {}
    sentinels_count = [0]
    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=get_dispach_to_reducers_function(
            players_by_key,
            matches_by_key,
            sentinels_count,
            partition_function
        )
    )

    print("Starting to receive players and matches and dispach it to reducers by key")
    channel.start_consuming()


def subscribe_to_entries(channel):
    channel.exchange_declare(
        exchange=MATCHES_INPUT_EXCHANGE_NAME,
        exchange_type=MATCHES_INPUT_EXCHANGE_TYPE)
    channel.exchange_declare(
        exchange=PLAYERS_INPUT_EXCHANGE_NAME,
        exchange_type=PLAYERS_INPUT_EXCHANGE_TYPE)

    result = channel.queue_declare(queue='join_matches')  # TODO poner anonima
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
    return private_queue_name

def main():
    main_master(
        KEYS_QUEUE_NAME,
        BARRIER_QUEUE_NAME,
        REDUCERS_OUTPUT_QUEUE_NAME,
        OUTPUT_EXCHANGE_NAME,
        subscribe_to_entries,
        receive_and_dispach_players_and_matches
    )


if __name__ == '__main__':
    main()
