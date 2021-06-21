from reducer import main_group_by_civ_reducer

FROM_JOIN_PLAYER_CIV_INDEX = 1

def append_to_times_by_civ(times_by_civ, player_columns):
    player_civ = player_columns[FROM_JOIN_PLAYER_CIV_INDEX]
    times_used_of_civ = times_by_civ.get(
        player_civ, 0)
    times_by_civ[player_civ] = times_used_of_civ + 1


def append_to_data_to_send(data_to_send, civ, times_used):
    data_to_send.append(
        [civ, str(times_used)]
    )

def main():
    main_group_by_civ_reducer(
        append_to_times_by_civ,
        append_to_data_to_send
    )

if __name__ == '__main__':
    main()
