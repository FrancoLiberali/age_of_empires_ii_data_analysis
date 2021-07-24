from communications.constants import PLAYER_LOSER, \
    PLAYER_WINNER
from reducer import main_group_by_civ_reducer

FROM_JOIN_PLAYER_WINNER_INDEX = 3
FROM_JOIN_PLAYER_CIV_INDEX = 4
WINS_INDEX = 0
DEFEATS_INDEX = 1


def append_to_wins_and_defeats_by_civ(wins_and_defeats_by_civ, player_columns):
    player_civ = player_columns[FROM_JOIN_PLAYER_CIV_INDEX]
    wins_and_defeats_of_civ = wins_and_defeats_by_civ.get(
        player_civ, [0, 0])

    if player_columns[FROM_JOIN_PLAYER_WINNER_INDEX] == PLAYER_WINNER:
        wins_and_defeats_of_civ[WINS_INDEX] += 1
    elif player_columns[FROM_JOIN_PLAYER_WINNER_INDEX] == PLAYER_LOSER:
        wins_and_defeats_of_civ[DEFEATS_INDEX] += 1

    wins_and_defeats_by_civ[player_civ] = wins_and_defeats_of_civ


def append_to_data_to_send(data_to_send, civ, wins_and_defeats):
    data_to_send.append(
        [civ] + [str(value) for value in wins_and_defeats]
    )

def main():
    main_group_by_civ_reducer(
        append_to_wins_and_defeats_by_civ,
        append_to_data_to_send
    )

if __name__ == '__main__':
    main()
