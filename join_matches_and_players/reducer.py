from communications.constants import FROM_CLIENT_MATCH_TOKEN_INDEX, \
    FROM_CLIENT_PLAYER_CIV_INDEX, \
    FROM_CLIENT_PLAYER_MATCH_INDEX, \
    FROM_CLIENT_PLAYER_WINNER_INDEX, \
    JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME, \
    JOIN_MASTER_TO_REDUCERS_QUEUE_NAME, \
    JOIN_REDUCERS_BARRIER_QUEUE_NAME, \
    JOIN_REDUCERS_TO_GROUP_BY_CIV_MASTER_QUEUE_NAME, \
    JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX, \
    JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR, \
    JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    SENTINEL_MESSAGE
from communications.rabbitmq_interface import send_list_of_columns_to_queue
from master_reducers_arq.reducer import main_reducer

INPUT_EXCHANGE_NAME = JOIN_MASTER_TO_REDUCERS_EXCHANGE_NAME
BARRIER_QUEUE_NAME = JOIN_REDUCERS_BARRIER_QUEUE_NAME
KEYS_QUEUE_NAME = JOIN_MASTER_TO_REDUCERS_QUEUE_NAME
OUTPUT_QUEUE_NAME = JOIN_REDUCERS_TO_GROUP_BY_CIV_MASTER_QUEUE_NAME

MATCH_PRESENT = 1

def get_important_columns_of_player(player_columns):
    return [
        player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX],
        player_columns[FROM_CLIENT_PLAYER_WINNER_INDEX],
        player_columns[FROM_CLIENT_PLAYER_CIV_INDEX],
    ]


def send_players(channel, players_to_send):
    if len(players_to_send) > 0:
        send_list_of_columns_to_queue(
            channel,
            OUTPUT_QUEUE_NAME,
            players_to_send
        )

def find_received_players_by_matches(channel, players_rows, players_by_match, matches):
    players_to_send = []
    for player_string in players_rows:
        player_columns = player_string.split(
             STRING_COLUMN_SEPARATOR)
        match_id = player_columns[FROM_CLIENT_PLAYER_MATCH_INDEX]
        # match no represent in matches
        if matches.get(match_id, None) is None:
            # store players and wait that match to arrive
            players_of_match = players_by_match.get(match_id, [])
            players_of_match.append(
                get_important_columns_of_player(player_columns)
            )
            players_by_match[match_id] = players_of_match
        else:
            players_to_send.append(
                get_important_columns_of_player(player_columns)
            )
    send_players(channel, players_to_send)

def find_players_by_received_matches(channel, matches_rows, players_by_match, matches):
    players_to_send = []
    for match_string in matches_rows:
        match_columns = match_string.split(
            STRING_COLUMN_SEPARATOR)
        match_id = match_columns[FROM_CLIENT_MATCH_TOKEN_INDEX]
        # store match and wait that players of that match to arrive
        matches[match_id] = MATCH_PRESENT
        players_of_that_match = players_by_match.pop(match_id, None)
        # allready players for that match
        if players_of_that_match is not None:
            players_to_send += players_of_that_match
    send_players(channel, players_to_send)

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
                find_received_players_by_matches(
                    channel, chunk_rows, players_by_match, matches)
            elif identificator == JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR:
                find_players_by_received_matches(
                    channel, chunk_rows, players_by_match, matches)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return filter_players_in_matches

def join_players_and_matches(channel, private_queue_name, keys):
    channel.queue_declare(queue=OUTPUT_QUEUE_NAME)

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


def main():
    main_reducer(
        KEYS_QUEUE_NAME,
        BARRIER_QUEUE_NAME,
        INPUT_EXCHANGE_NAME,
        join_players_and_matches
    )


if __name__ == '__main__':
    main()
