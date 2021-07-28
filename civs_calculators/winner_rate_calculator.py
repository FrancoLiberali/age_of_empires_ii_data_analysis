from civs_calculators_arq.civ_calculator import main_civ_calculator, write_to_new_data
from communications.constants import WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME
from communications.rabbitmq_interface import split_columns_into_list, split_rows_into_list

FROM_GROUP_BY_CIV_CIV_INDEX = 0
FROM_GROUP_BY_CIV_WINS_INDEX = 1
FROM_GROUP_BY_CIV_DEFEATS_INDEX = 2

WINS_INDEX = 0
DEFEATS_INDEX = 1

def get_group_wins_and_defeats_by_civ_function(wins_and_defeats_by_civ):
    def group_wins_and_defeats_by_civ(queue, received_string, _, actual_hash):
        for civ_wins_and_defeats in split_rows_into_list(received_string, skip_header=True):
            columns = split_columns_into_list(civ_wins_and_defeats)
            civ = columns[FROM_GROUP_BY_CIV_CIV_INDEX]
            wins_and_defeats_of_civ = wins_and_defeats_by_civ.get(
                civ, [0, 0])
            wins_and_defeats_of_civ[WINS_INDEX] += int(columns[FROM_GROUP_BY_CIV_WINS_INDEX])
            wins_and_defeats_of_civ[DEFEATS_INDEX] += int(columns[FROM_GROUP_BY_CIV_DEFEATS_INDEX])
            wins_and_defeats_by_civ[civ] = wins_and_defeats_of_civ
        write_to_new_data(wins_and_defeats_by_civ, actual_hash)
    return group_wins_and_defeats_by_civ


def calculate_winner_rate(logger, wins_and_defeats_by_civ):
    logger.info('All wins and defeats received, calculating winner rate')
    logger.debug(f'wins_and_defeats_by_civ: {wins_and_defeats_by_civ}')
    winner_rates = {}
    for civ, wins_and_defeats in wins_and_defeats_by_civ.items():
        wins = wins_and_defeats[WINS_INDEX]
        defeats = wins_and_defeats[DEFEATS_INDEX]
        winner_rates[civ] = str(wins / (wins + defeats) * 100)

    return sorted(winner_rates.items(), key=lambda x: x[1], reverse=True)

def main():
    main_civ_calculator(
        WINNER_RATE_CALCULATOR_TO_CLIENT_QUEUE_NAME,
        get_group_wins_and_defeats_by_civ_function,
        calculate_winner_rate
    )

if __name__ == '__main__':
    main()
