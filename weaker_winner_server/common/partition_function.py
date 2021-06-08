import string
from itertools import permutations

from common.constants import MATCH_INDEX

class PartitionFunction:
    def __init__(self, reducers_amount):
        self.reducers_amount = reducers_amount

        self.possible_characters = string.ascii_letters + string.digits
        reducer_per_character = self.reducers_amount // len(
            self.possible_characters)
        self.characters_per_key = reducer_per_character + 1

    def get_posibles_keys(self):
        permutations_list = [''.join(key_characters) for key_characters in permutations(self.possible_characters, self.characters_per_key)]
        if self.characters_per_key > 1:
            return permutations_list + [character * self.characters_per_key for character in self.possible_characters]
        else:
            return permutations_list

    def get_key(self, match_player_row):
        return match_player_row[MATCH_INDEX][0:self.characters_per_key]
