import os
import pika

from communications.constants import STRING_ENCODING, \
    STRING_LINE_SEPARATOR, \
    STRING_COLUMN_SEPARATOR, \
    RABBITMQ_HOST, \
    SENTINEL_MESSAGE, \
    TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import send_list_of_columns_to_queue

INPUT_QUEUE_NAME = os.environ["INPUT_QUEUE_NAME"]

FROM_GROUP_BY_CIV_CIV_INDEX = 0
FROM_GROUP_BY_CIV_TIMES_INDEX = 1


def get_group_times_used_by_civ_function(times_used_by_civ):
    def group_wins_and_defeats_by_civ(channel, method, properties, body):
        chunk_string = body.decode(STRING_ENCODING)
        if chunk_string == SENTINEL_MESSAGE:
            print("Sentinel message received, stoping receiving")
            channel.stop_consuming()
        else:
            for civ_and_times_used in chunk_string.split(STRING_LINE_SEPARATOR):
                columns = civ_and_times_used.split(STRING_COLUMN_SEPARATOR)
                civ = columns[FROM_GROUP_BY_CIV_CIV_INDEX]
                times_used_of_civ = times_used_by_civ.get(
                    civ, 0)
                times_used_of_civ += int(
                    columns[FROM_GROUP_BY_CIV_TIMES_INDEX]
                )
                times_used_by_civ[civ] = times_used_of_civ
        channel.basic_ack(delivery_tag=method.delivery_tag)
    return group_wins_and_defeats_by_civ

CIV_INDEX = 0
TIMES_INDEX = 1
TOP_LEN = 5

# TODO codigo repetido con el otro calculator
def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))

    channel = connection.channel()
    channel.queue_declare(
        queue=INPUT_QUEUE_NAME)

    channel.queue_declare(queue=TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME)

    times_used_by_civ = {}
    channel.basic_consume(
        queue=INPUT_QUEUE_NAME,
        on_message_callback=get_group_times_used_by_civ_function(
            times_used_by_civ),
    )

    print('Starting to receive times used by civ to calculate top 5')
    channel.start_consuming()

    print('All times used received, calculating top 5')
    times_used_list = times_used_by_civ.items()
    sorted_list = sorted(
        times_used_list,
        key=lambda civ_and_times: civ_and_times[TIMES_INDEX],
        reverse=True
    )
    if len(sorted_list) > TOP_LEN:
        top_5 = map(
            lambda civ_and_times:
                (civ_and_times[CIV_INDEX], str(civ_and_times[TIMES_INDEX])),
            sorted_list[0:TOP_LEN]
        )
    else:
        top_5 = map(lambda civ, times: (civ, str(times)), sorted_list)
    send_list_of_columns_to_queue(
        channel,
        TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME,
        top_5
    )

    connection.close()


if __name__ == '__main__':
    main()
