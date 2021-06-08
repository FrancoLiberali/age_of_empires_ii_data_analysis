import pika

from common.constants import LOSER, MATCH_INDEX, SENTINEL_KEY, WINNER, WINNER_INDEX, RATING_INDEX
from communications.constants import FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME, \
    GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME, \
    GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, \
    WEAKER_WINNER_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import send_matches_ids, send_sentinel

def can_match_be_1_vs_1(players_list, new_player):
    return (players_list is not None and (len(players_list) == 0 or (len(players_list) == 1 and players_list[0][WINNER_INDEX] != new_player[WINNER_INDEX])))

def get_group_by_match_function(players_by_match):
    # python function currying
    def group_by_match(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping grouping players")
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
                    players_by_match[match_id] = None # mark this match as not possible
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return group_by_match


MINIMUM_RATING_PROCENTAGE_DIFF = 30 # TODO envvar


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
    channel.queue_declare(queue=GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME)
    keys = []
    channel.basic_consume(
        queue=GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME,
        on_message_callback=get_set_keys_function(keys)
    )
    print('Waiting for keys assignement')
    channel.start_consuming()
    print(f'Assigned keys are: {keys}')
    return keys


def subscribe_to_keys(channel, keys):
    print(f"Subscribing to keys")
    channel.exchange_declare(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        exchange_type='direct')

    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    for key in keys:
        channel.queue_bind(
            exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
            queue=private_queue_name,
            routing_key=key)
    channel.queue_bind(
        exchange=FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME,
        queue=private_queue_name,
        routing_key=SENTINEL_KEY)
    print(f"Finished subscribing to keys")
    return private_queue_name

def send_sentinel_to_master(channel):
    channel.queue_declare(queue=GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME)
    send_sentinel(channel, GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME)


def process_player_by_match(channel, private_queue_name, keys):
    players_by_match = {}
    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=get_group_by_match_function(players_by_match),
    )
    print(f'Starting to receive players in matches with keys {keys} to group them.')

    channel.start_consuming()
    print(f'All players in matches with keys {keys} grouped.')
    return players_by_match


MINIMUM_RATING = 1000  # TODO envvar

def filter_players_by_weaker_winner(players_by_match):
    matches_ids = []
    for match_id, players_list in players_by_match.items():
        # final check that all matches are of two players
        if players_list is not None and len(players_list) == 2:
            winner = next(
                (player for player in players_list if player[WINNER_INDEX] == WINNER))
            loser = next(
                (player for player in players_list if player[WINNER_INDEX] == LOSER))

            if winner[RATING_INDEX] != '' and loser[RATING_INDEX] != '':
                winner_rating = int(winner[RATING_INDEX])
                loser_rating = int(loser[RATING_INDEX])
                rating_diff = (loser_rating - winner_rating) / \
                    winner_rating * 100
                if winner_rating > MINIMUM_RATING and rating_diff > MINIMUM_RATING_PROCENTAGE_DIFF:
                    matches_ids.append(match_id)
    return matches_ids

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    keys = receive_keys(channel)
    private_queue_name = subscribe_to_keys(channel, keys)
    print("Sending sentinel to master to notify ready to receive players")
    send_sentinel_to_master(channel)

    players_by_match = process_player_by_match(channel, private_queue_name, keys)

    matches_ids = filter_players_by_weaker_winner(players_by_match)
    print(f"All matches with keys {keys} with weaker winner found")

    if len(matches_ids) > 0:
        channel.queue_declare(queue=WEAKER_WINNER_TO_CLIENT_QUEUE_NAME)
        print(f"Sending matches found to client")
        send_matches_ids(channel, WEAKER_WINNER_TO_CLIENT_QUEUE_NAME, matches_ids)

    print("Sending sentinel to master to notify finished")
    send_sentinel_to_master(channel)
    connection.close()


if __name__ == '__main__':
    main()
