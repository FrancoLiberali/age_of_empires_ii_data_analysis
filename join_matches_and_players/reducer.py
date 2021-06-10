import pika

from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, FROM_CLIENT_PLAYER_CIV_INDEX, FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_RATING_INDEX, \
    FROM_CLIENT_PLAYER_WINNER_INDEX, \
    GROUP_BY_MATCH_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME, \
    GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME, JOIN_MASTER_TO_REDUCERS_QUEUE_NAME, JOIN_REDUCERS_BARRIER_QUEUE_NAME, JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX, JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    SENTINEL_KEY, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import send_matches_ids, send_sentinel_to_queue

# TODO codigo repetido con reducer de group by
INPUT_EXCHANGE_NAME = JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME  # TODO envvar
BARRIER_QUEUE_NAME = JOIN_REDUCERS_BARRIER_QUEUE_NAME  # TODO envvar
KEYS_QUEUE_NAME = JOIN_MASTER_TO_REDUCERS_QUEUE_NAME  # TODO envvar


def get_set_keys_function(keys):
    # python function currying
    def set_keys(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping receiving keys")
            channel.stop_consuming()
        else:
            keys.append(chunk_string)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return set_keys


def receive_keys(channel):
    channel.queue_declare(queue=KEYS_QUEUE_NAME)
    keys = []
    channel.basic_consume(
        queue=KEYS_QUEUE_NAME,
        on_message_callback=get_set_keys_function(keys)
    )
    print('Waiting for keys assignement')
    channel.start_consuming()
    print(f'Assigned keys are: {keys}')
    return keys


def subscribe_to_keys(channel, keys):
    print(f"Subscribing to keys")
    channel.exchange_declare(
        exchange=INPUT_EXCHANGE_NAME,
        exchange_type='direct')

    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    for key in keys + [SENTINEL_KEY]:
        channel.queue_bind(
            exchange=INPUT_EXCHANGE_NAME,
            queue=private_queue_name,
            routing_key=key)
    print(f"Finished subscribing to keys")
    return private_queue_name


def send_sentinel_to_master(channel):
    channel.queue_declare(queue=BARRIER_QUEUE_NAME)
    send_sentinel_to_queue(channel, BARRIER_QUEUE_NAME)


def filter_received_players_by_matches(players_rows, players_by_match, matches):
    for player_string in players_rows:
        player_columns = player_string.split(
             STRING_COLUMN_SEPARATOR)
        match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
        # match no represent in matches
        if matches.get(match_id, None) is None:
            # store players and wait that match to arrive
            players_of_match = players_by_match.get(match_id, [])
            players_of_match.append(
                [
                    player_columns[FROM_CLIENT_PLAYER_WINNER_INDEX],
                    player_columns[FROM_CLIENT_PLAYER_CIV_INDEX],
                ]
            )
            players_by_match[match_id] = players_of_match
        else:
            print("ya estaba el match, mandar players a groupby")


MATCH_PRESENT = 1

def find_players_by_received_matches(matches_rows, players_by_match, matches):
    for match_string in matches_rows:
        match_columns = match_string.split(
            STRING_COLUMN_SEPARATOR)
        match_id = match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX]
        # store match and wait that players of that match to arrive
        matches[match_id] = MATCH_PRESENT
        if players_by_match.get(match_id, None) is not None:
            # recordar deletear los players
            print("ya estaban los players, mandar players a groupby")

def get_filter_players_in_matches_function(players_by_match, matches):
    # python function currying
    def filter_players_in_matches(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping joining players and matches")
            channel.stop_consuming()
        else:
            chunk_rows = chunk_string.split(STRING_LINE_SEPARATOR)
            identificator = chunk_rows.pop(
                JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX)
            if identificator == JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR:
                filter_received_players_by_matches(
                    chunk_rows, players_by_match, matches)
            elif identificator == JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR:
                find_players_by_received_matches(
                    chunk_rows, players_by_match, matches)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return filter_players_in_matches

def join_players_and_matches(channel, private_queue_name, keys):
    players_by_match = {}
    matches = {}
    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=get_filter_players_in_matches_function(
            players_by_match, matches),
    )
    print(
        f'Starting to receive players and matches in matches with keys {keys} to join them.')

    channel.start_consuming()
    print(f'All players and matches in matches with keys {keys} joined.')
    return players_by_match


# MINIMUM_RATING = 1000  # TODO envvar
# 
# 
# def filter_players_by_weaker_winner(players_by_match):
    # matches_ids = []
    # for match_id, players_list in players_by_match.items():
        # final check that all matches are of two players
        # if players_list is not None and len(players_list) == 2:
            # winner = next(
                # (player for player in players_list if player[FROM_CLIENT_PLAYER_WINNER_INDEX] == WINNER))
            # loser = next(
                # (player for player in players_list if player[FROM_CLIENT_PLAYER_WINNER_INDEX] == LOSER))
# 
            # if winner[FROM_CLIENT_PLAYER_RATING_INDEX] != '' and loser[FROM_CLIENT_PLAYER_RATING_INDEX] != '':
                # winner_rating = int(winner[FROM_CLIENT_PLAYER_RATING_INDEX])
                # loser_rating = int(loser[FROM_CLIENT_PLAYER_RATING_INDEX])
                # rating_diff = (loser_rating - winner_rating) / \
                    # winner_rating * 100
                # if winner_rating > MINIMUM_RATING and rating_diff > MINIMUM_RATING_PROCENTAGE_DIFF:
                    # matches_ids.append(match_id)
    # return matches_ids


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    keys = receive_keys(channel)
    private_queue_name = subscribe_to_keys(channel, keys)
    print("Sending sentinel to master to notify ready to receive players")
    send_sentinel_to_master(channel)

    join_players_and_matches(channel, private_queue_name, keys)
# 
    # matches_ids = filter_players_by_weaker_winner(players_by_match)
    # print(f"All matches with keys {keys} with weaker winner found")
# 
    # if len(matches_ids) > 0:
        # channel.queue_declare(queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)
        # print(f"Sending matches found to client")
        # send_matches_ids(
            # channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME, matches_ids)

    print("Sending sentinel to master to notify finished")
    send_sentinel_to_master(channel)
    connection.close()


if __name__ == '__main__':
    main()
