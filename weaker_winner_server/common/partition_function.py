import string

from common.constants import MATCH_INDEX

def get_key(match_player_row):
    # TODO hacer que dependa de la cantidad de reducers
    return match_player_row[MATCH_INDEX][0]


def get_posibles_keys():
    # TODO hacer que dependa de la cantidad de reducers
    return (string.ascii_letters + string.digits)
