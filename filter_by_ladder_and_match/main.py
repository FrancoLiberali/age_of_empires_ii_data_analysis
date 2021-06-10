import pika

from communications.constants import MATCHES_KEY, \
    FROM_CLIENT_MATCH_TOKEN_INDEX, \
    STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    MATCHES_FANOUT_EXCHANGE_NAME, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, \
    MATCHES_TO_JOIN_MASTER_EXCHANGE_NAME
from communications.rabbitmq_interface import send_list_of_columns_to_exchange, send_sentinel_to_exchange

LADDER = 'RM_1v1'  # TODO envvar
MAP='arena' # TODO envvar
NO_MIRROR = 'False' # TODO envvar, TODO sacar de aca al hacer el punto 4

# join: si llega primero el match lo tengo que guardar para que cuando llegue el player ver que el match es alguno de estos
# si llega primero el player me tengo que guardar el match id para cuando llegue el match ver si hay players esperando por ese match

LADDER_INDEX_INDEX = 4
MAP_INDEX = 5
MIRROR_INDEX = 6

OUTPUT_EXCHANGE_NAME = MATCHES_TO_JOIN_MASTER_EXCHANGE_NAME  # TODO envvar


# TODO codigo repetido con el otr filtro de matches, misma estructura para filtro

def is_matched(columns):
    return (
        columns[LADDER_INDEX_INDEX] == LADDER and
        columns[MAP_INDEX] == MAP
        # TODO sacar de aca al hacer el punto 4
        and columns[MIRROR_INDEX] == NO_MIRROR
    )


def filter_by_ladder_and_map(channel, method, properties, body):
    chunk_string = body.decode(STRING_ENCODING)
    if chunk_string == SENTINEL_MESSAGE:
        print("Sentinel message received, stoping receiving matches")
        channel.stop_consuming()
        print("Sending sentinel to next stage to notify that all matches ids has been sended")
        send_sentinel_to_exchange(channel, OUTPUT_EXCHANGE_NAME, MATCHES_KEY)
    else:
        matches_matched = []
        for row in chunk_string.split(STRING_LINE_SEPARATOR):
            columns = row.split(STRING_COLUMN_SEPARATOR)
            if is_matched(columns):
                matches_matched.append(
                    [
                        columns[FROM_CLIENT_MATCH_TOKEN_INDEX],
                        # TODO agregar aca al hacer el punto 4
                        # columns[MIRROR_INDEX],
                    ]
                )
        if (len(matches_matched) > 0):
            send_list_of_columns_to_exchange(
                channel, OUTPUT_EXCHANGE_NAME, matches_matched, MATCHES_KEY)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()

    # TODO codigo repetido con long matches
    channel.exchange_declare(
        exchange=MATCHES_FANOUT_EXCHANGE_NAME,
        exchange_type='fanout')
    result = channel.queue_declare(queue='')
    private_queue_name = result.method.queue
    channel.queue_bind(
        exchange=MATCHES_FANOUT_EXCHANGE_NAME,
        queue=private_queue_name
    )

    channel.exchange_declare(
        exchange=OUTPUT_EXCHANGE_NAME,
        exchange_type='direct')

    channel.basic_consume(
        queue=private_queue_name,
        on_message_callback=filter_by_ladder_and_map,
    )

    print(f'Starting to receive matches to filter by ladder {LADDER} and map {MAP}')
    channel.start_consuming()
    connection.close()


if __name__ == '__main__':
    main()
