RABBITMQ_HOST = 'rabbitmq' # TODO envvar

STRING_ENCODING = 'utf-8'
STRING_LINE_SEPARATOR = '\n'
STRING_COLUMN_SEPARATOR = ', '

MATCHES_IDS_SEPARATOR = ', '

MATCHES_FANOUT_EXCHANGE_NAME = 'matches_fanout_exchange'
LONG_MATCHES_TO_CLIENT_QUEUE_NAME = 'long_matches_to_client'

PLAYERS_FANOUT_EXCHANGE_NAME = 'players_fanout_exchange'
GROUP_BY_MATCH_MASTER_TO_REDUCERS_EXCHANGE_NAME = 'group_by_match_master_to_reducers_exchange'
GROUP_BY_MATCH_REDUCERS_BARRIER_QUEUE_NAME = 'group_by_match_reducers_barrier'
GROUP_BY_MATCH_MASTER_TO_REDUCERS_QUEUE_NAME = 'group_by_match_master_to_reducers_queue'
WEAKER_WINNER_TO_CLIENT_QUEUE_NAME = 'weaker_winner_to_client'

WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME = 'winner_rate_calculator_to_client_queue'
TOP_5_USED_CALCULATOR_TO_CLIENT_QUEUE_NAME = 'top_5_used_calculator_to_client_queue'

SENTINEL_MESSAGE = "SENTINEL"

SENTINEL_KEY = "sentinel"
PLAYERS_KEY = "" # no key because it comes directly from PLAYERS_FANOUT_EXCHANGE_NAME
MATCHES_KEY = "matches"

JOIN_TO_REDUCERS_IDENTIFICATOR_INDEX = 0
JOIN_TO_REDUCERS_PLAYERS_IDENTIFICATOR = "players"
JOIN_TO_REDUCERS_MATCHES_IDENTIFICATOR = "matches"

FROM_CLIENT_MATCH_TOKEN_INDEX = 0
FROM_CLIENT_MATCH_AVERAGE_RATING_INDEX = 1
FROM_CLIENT_MATCH_SERVER_INDEX = 2
FROM_CLIENT_MATCH_DURATION_INDEX = 3
FROM_CLIENT_MATCH_LADDER_INDEX = 4
FROM_CLIENT_MATCH_MAP_INDEX = 5
FROM_CLIENT_MATCH_MIRROR_INDEX = 6

FROM_CLIENT_PLAYER_MATCH_INDEX = 0
FROM_CLIENT_PLAYER_RATING_INDEX = 1
FROM_CLIENT_PLAYER_WINNER_INDEX = 2
FROM_CLIENT_PLAYER_CIV_INDEX = 3

PLAYER_WINNER = "True"
PLAYER_LOSER = "False"


