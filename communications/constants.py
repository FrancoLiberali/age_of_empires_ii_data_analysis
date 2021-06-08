RABBITMQ_HOST = 'rabbitmq' # TODO envvar

STRING_ENCODING = 'utf-8'
STRING_LINE_SEPARATOR = '\n'
STRING_COLUMN_SEPARATOR = ', '

MATCHES_IDS_SEPARATOR = ', '

CLIENT_TO_LONG_MATCHES_QUEUE_NAME = 'client_to_long_matches'
LONG_MATCHES_TO_CLIENT_QUEUE_NAME = 'long_matches_to_client'
CLIENT_TO_WEAKER_WINNER_QUEUE_NAME = 'client_to_weaker_winner'
FILTER_BY_RATING_TO_GROUP_BY_EXCHANGE_NAME = 'filter_by_rating_to_group_by'
WEAKER_WINNER_TO_CLIENT_QUEUE_NAME = 'weaker_winner_to_client'
GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME = 'group_by_match_reducers_barrier'
GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME = 'group_by_match_master_to_reducers'

SENTINEL_MESSAGE = "SENTINEL"

