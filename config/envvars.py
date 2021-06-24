import os

RABBITMQ_HOST_KEY = "RABBITMQ_HOST"

OUTPUT_EXCHANGE_NAME_1V1_KEY = "OUTPUT_EXCHANGE_NAME_1V1"
OUTPUT_EXCHANGE_NAME_TEAM_KEY = "OUTPUT_EXCHANGE_NAME_TEAM"
LADDER_1V1_KEY = "LADDER_1V1"
MAP_ARENA_KEY = "MAP_ARENA"
NO_MIRROR_KEY = "NO_MIRROR"
LADDER_TEAM_KEY = "LADDER_TEAM"
MAP_ISLANDS_KEY = "MAP_ISLANDS"

PLAYERS_CHUNK_SIZE_KEY = "PLAYERS_CHUNK_SIZE"
KEYS_QUEUE_NAME_KEY = "KEYS_QUEUE_NAME"
BARRIER_QUEUE_NAME_KEY = "BARRIER_QUEUE_NAME"
REDUCERS_OUTPUT_QUEUE_NAME_KEY = "REDUCERS_OUTPUT_QUEUE_NAME"
OUTPUT_EXCHANGE_NAME_KEY = "OUTPUT_EXCHANGE_NAME"
PLAYERS_INPUT_QUEUE_NAME_KEY = "PLAYERS_INPUT_QUEUE_NAME"

ROWS_CHUNK_SIZE_KEY = "ROWS_CHUNK_SIZE"
MATCHES_INPUT_EXCHANGE_NAME_KEY = "MATCHES_INPUT_EXCHANGE_NAME"
PLAYERS_INPUT_EXCHANGE_NAME_KEY = "PLAYERS_INPUT_EXCHANGE_NAME"

MINIMUM_AVERAGE_RATING_KEY = "MINIMUM_AVERAGE_RATING"
MINIMUM_DURATION_KEY = "MINIMUM_DURATION"
DURATION_FORMAT_KEY = "DURATION_FORMAT"
KOREA_CENTRAL_SERVER_KEY = "KOREA_CENTRAL_SERVER"
SOUTH_EAST_ASIA_SERVER_KEY = "SOUTH_EAST_ASIA_SERVER"
EAST_US_SERVER_KEY = "EAST_US_SERVER"

MINIMUM_RATING_KEY = "MINIMUM_RATING"
MINIMUM_RATING_PORCENTAGE_DIFF_KEY = "MINIMUM_RATING_PORCENTAGE_DIFF"

INPUT_QUEUE_NAME_KEY = "INPUT_QUEUE_NAME"

MIN_RATING_KEY = "MIN_RATING"

INPUT_EXCHANGE_NAME_KEY = "INPUT_EXCHANGE_NAME"
OUTPUT_QUEUE_NAME_KEY = "OUTPUT_QUEUE_NAME"

REDUCERS_AMOUNT_KEY = "REDUCERS_AMOUNT"

CHUCKSIZE_IN_LINES_KEY = "CHUCKSIZE_IN_LINES"
ENTRY_MATCH_TOKEN_INDEX_KEY = "ENTRY_MATCH_TOKEN_INDEX"
ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY = "ENTRY_MATCH_AVERAGE_RATING_INDEX"
ENTRY_MATCH_SERVER_INDEX_KEY = "ENTRY_MATCH_SERVER_INDEX"
ENTRY_MATCH_DURATION_INDEX_KEY = "ENTRY_MATCH_DURATION_INDEX"
ENTRY_MATCH_LADDER_INDEX_KEY = "ENTRY_MATCH_LADDER_INDEX"
ENTRY_MATCH_MAP_INDEX_KEY = "ENTRY_MATCH_MAP_INDEX"
ENTRY_MATCH_MIRROR_INDEX_KEY = "ENTRY_MATCH_MIRROR_INDEX"
ENTRY_PLAYER_MATCH_INDEX_KEY = "ENTRY_PLAYER_MATCH_INDEX"
ENTRY_PLAYER_RATING_INDEX_KEY = "ENTRY_PLAYER_RATING_INDEX"
ENTRY_PLAYER_WINNER_INDEX_KEY = "ENTRY_PLAYER_WINNER_INDEX"
ENTRY_PLAYER_CIV_INDEX_KEY = "ENTRY_PLAYER_CIV_INDEX"

INT_ENVVARS_KEYS = [
    PLAYERS_CHUNK_SIZE_KEY,
    ROWS_CHUNK_SIZE_KEY,
    CHUCKSIZE_IN_LINES_KEY,
    MINIMUM_AVERAGE_RATING_KEY,
    MINIMUM_DURATION_KEY,
    MINIMUM_RATING_KEY,
    MINIMUM_RATING_PORCENTAGE_DIFF_KEY,
    MIN_RATING_KEY,
    REDUCERS_AMOUNT_KEY,
    ENTRY_MATCH_TOKEN_INDEX_KEY,
    ENTRY_MATCH_AVERAGE_RATING_INDEX_KEY,
    ENTRY_MATCH_SERVER_INDEX_KEY,
    ENTRY_MATCH_DURATION_INDEX_KEY,
    ENTRY_MATCH_LADDER_INDEX_KEY,
    ENTRY_MATCH_MAP_INDEX_KEY,
    ENTRY_MATCH_MIRROR_INDEX_KEY,
    ENTRY_PLAYER_MATCH_INDEX_KEY,
    ENTRY_PLAYER_RATING_INDEX_KEY,
    ENTRY_PLAYER_WINNER_INDEX_KEY,
    ENTRY_PLAYER_CIV_INDEX_KEY,
]

def get_config_param(key, logger):
    try:
        envvar = os.environ[key]
        if key in INT_ENVVARS_KEYS:
            return int(envvar)
        return envvar
    except KeyError as e:
        logger.critical(
            f"Key was not found in EnvVars. Error: {e}. Aborting")
        raise e
    except ValueError as e:
        logger.critical(
            f"Key could not be parsed. Error: {e}. Aborting")
        raise e

def get_config_params(keys, logger):
    config = {}
    for key in keys:
        config[key] = get_config_param(key, logger)
    return config