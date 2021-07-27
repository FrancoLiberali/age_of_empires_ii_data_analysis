import yaml
import sys

def get_id(i):
    return str(i+1)

class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True

""" 
n1: Reducers amount of group by match
n2: Reducers amount of join matches 1v1 and players
n3: Reducers amount of group players of matches 1v1 by civ
n4: Reducers amount of join team matches and players
n5: Reducers amount of group players of team matches by civ
n6: Supervisors
"""

def generate_compose_yaml(n1, n2, n3, n4, n5, n6):
    compose_config={}
    compose_config["version"] = "3.5"
    compose_config["services"] = {}

    node = "client"
    compose_config["services"][node] = {
        "container_name": node,
        "build": {
            "context": ".",
            "dockerfile": "./client/client.dockerfile"
        },
        "volumes": [
            "./client:/client",
            "./communications:/client/communications",
            "./config:/client/config",
            "./logger:/client/logger",
            "./last_hash/client:/last_hash"
        ],
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "CHUCKSIZE_IN_LINES=100",
            "ENTRY_MATCH_TOKEN_INDEX=0",
            "ENTRY_MATCH_AVERAGE_RATING_INDEX=5",
            "ENTRY_MATCH_SERVER_INDEX=9",
            "ENTRY_MATCH_DURATION_INDEX=10",
            "ENTRY_MATCH_LADDER_INDEX=3",
            "ENTRY_MATCH_MAP_INDEX=6",
            "ENTRY_MATCH_MIRROR_INDEX=2",
            "ENTRY_PLAYER_TOKEN_INDEX=0",
            "ENTRY_PLAYER_MATCH_INDEX=1",
            "ENTRY_PLAYER_RATING_INDEX=2",
            "ENTRY_PLAYER_WINNER_INDEX=6",
            "ENTRY_PLAYER_CIV_INDEX=4",
            "MATCHES_FILE_NAME=matches_reduced.csv",
            "PLAYERS_FILE_NAME=match_players_reduced.csv"
        ],
        "networks": [
            "age_of_empires_net"
        ],
        "depends_on": [
            "filter_by_ladder_map_and_mirror",
            "join_master_1v1",
            "filter_by_rating",
            "filter_by_avr_rating_duration_and_server",
            "group_by_match_master"
        ]
    }

    nodes = []

    node = "filter_by_avr_rating_duration_and_server"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./filters:/age_of_empires_ii_da",
            "./communications:/age_of_empires_ii_da/communications",
            "./config:/age_of_empires_ii_da/config",
            "./logger:/age_of_empires_ii_da/logger"
        ],
        "entrypoint": "python3 /age_of_empires_ii_da/filter_by_avr_rating_duration_and_server.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "MINIMUM_AVERAGE_RATING=2000",
            "MINIMUM_DURATION=2",
            "DURATION_FORMAT=%H:%M:%S",
            "KOREA_CENTRAL_SERVER=koreacentral",
            "SOUTH_EAST_ASIA_SERVER=southeastasia",
            "EAST_US_SERVER=eastus"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    node = "filter_by_rating"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./filters:/age_of_empires_ii_da",
            "./communications:/age_of_empires_ii_da/communications",
            "./config:/age_of_empires_ii_da/config",
            "./logger:/age_of_empires_ii_da/logger"
        ],
        "entrypoint": "python3 /age_of_empires_ii_da/filter_by_rating.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "OUTPUT_EXCHANGE_NAME=filter_by_rating_to_team_join_master_exchange",
            "MIN_RATING=2000"
        ],
        "networks": [
            "age_of_empires_net"
        ],
        "depends_on": [
            "join_master_team"
        ]
    }

    node = "filter_by_ladder_map_and_mirror"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./filters:/age_of_empires_ii_da",
            "./communications:/age_of_empires_ii_da/communications",
            "./config:/age_of_empires_ii_da/config",
            "./logger:/age_of_empires_ii_da/logger"
        ],
        "entrypoint": "python3 /age_of_empires_ii_da/filter_by_ladder_map_and_mirror.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "LADDER_1V1=RM_1v1",
            "MAP_ARENA=arena",
            "NO_MIRROR=False",
            "LADDER_TEAM=RM_TEAM",
            "MAP_ISLANDS=islands",
            "OUTPUT_EXCHANGE_NAME_1V1=1v1_to_join_master_exchange",
            "OUTPUT_EXCHANGE_NAME_TEAM=team_to_join_master_exchange"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    node = "group_by_match_master"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./group_by_match:/age_of_empires_ii_da",
            "./communications:/age_of_empires_ii_da/communications",
            "./master_reducers_arq:/age_of_empires_ii_da/master_reducers_arq",
            "./config:/age_of_empires_ii_da/config",
            "./logger:/age_of_empires_ii_da/logger"
        ],
        "entrypoint": "python3 /age_of_empires_ii_da/master.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "REDUCERS_AMOUNT=" + str(n1),
            "ROWS_CHUNK_SIZE=100",
            "REDUCERS_QUEUE_PREFIX=group_by_match_reducer_"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    node = "join_master_1v1"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./join_matches_and_players:/join_matches_and_players",
            "./communications:/join_matches_and_players/communications",
            "./master_reducers_arq:/join_matches_and_players/master_reducers_arq",
            "./config:/join_matches_and_players/config",
            "./logger:/join_matches_and_players/logger",
            "../age_of_empires_servers_data/join_master_1v1:/data"
        ],
        "entrypoint": "python3 /join_matches_and_players/master.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "REDUCERS_AMOUNT=" + str(n2),
            "ROWS_CHUNK_SIZE=100",
            "PLAYERS_INPUT_EXCHANGE_NAME=players_fanout_exchange",
            "MATCHES_INPUT_EXCHANGE_NAME=1v1_to_join_master_exchange",
            "OUTPUT_EXCHANGE_NAME=1v1_join_master_to_reducers_exchange",
            "BARRIER_QUEUE_NAME=1v1_join_reducers_barrier",
            "INPUT_QUEUE_NAME=1v1_join_master_input_queue",
            "REDUCERS_OUTPUT_QUEUE_NAME=1v1_join_reducers_to_group_by_civ_master_queue",
            "REDUCERS_QUEUE_PREFIX=join_reducer_1v1_"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }


    node = "join_master_team"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./join_matches_and_players:/join_matches_and_players",
            "./communications:/join_matches_and_players/communications",
            "./master_reducers_arq:/join_matches_and_players/master_reducers_arq",
            "./config:/join_matches_and_players/config",
            "./logger:/join_matches_and_players/logger"
        ],
        "entrypoint": "python3 /join_matches_and_players/master.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "REDUCERS_AMOUNT=" + str(n4),
            "ROWS_CHUNK_SIZE=100",
            "PLAYERS_INPUT_EXCHANGE_NAME=filter_by_rating_to_team_join_master_exchange",
            "MATCHES_INPUT_EXCHANGE_NAME=team_to_join_master_exchange",
            "OUTPUT_EXCHANGE_NAME=team_join_master_to_reducers_exchange",
            "BARRIER_QUEUE_NAME=team_join_reducers_barrier",
            "INPUT_QUEUE_NAME=team_join_master_input_queue",
            "REDUCERS_OUTPUT_QUEUE_NAME=team_join_reducers_to_group_by_civ_master_queue",
            "REDUCERS_QUEUE_PREFIX=join_reducer_team_"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    node = "group_by_civ_master_1v1"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./group_by_civ:/group_by_civ",
            "./communications:/group_by_civ/communications",
            "./master_reducers_arq:/group_by_civ/master_reducers_arq",
            "./config:/group_by_civ/config",
            "./logger:/group_by_civ/logger"
        ],
        "entrypoint": "python3 /group_by_civ/master.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "REDUCERS_AMOUNT=" + str(n3),
            "ROWS_CHUNK_SIZE=100",
            "PLAYERS_INPUT_QUEUE_NAME=1v1_join_reducers_to_group_by_civ_master_queue",
            "OUTPUT_EXCHANGE_NAME=1v1_group_by_civ_master_to_reducers_exchange",
            "BARRIER_QUEUE_NAME=1v1_group_by_civ_reducers_barrier",
            "REDUCERS_OUTPUT_QUEUE_NAME=1v1_group_by_civ_reducers_to_winner_rate_calculador_queue",
            "REDUCERS_QUEUE_PREFIX=group_by_civ_reducer_1v1_"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    node = "group_by_civ_master_team"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./group_by_civ:/group_by_civ",
            "./communications:/group_by_civ/communications",
            "./master_reducers_arq:/group_by_civ/master_reducers_arq",
            "./config:/group_by_civ/config",
            "./logger:/group_by_civ/logger"
        ],
        "entrypoint": "python3 /group_by_civ/master.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "REDUCERS_AMOUNT=" + str(n5),
            "ROWS_CHUNK_SIZE=100",
            "PLAYERS_INPUT_QUEUE_NAME=team_join_reducers_to_group_by_civ_master_queue",
            "OUTPUT_EXCHANGE_NAME=team_group_by_civ_master_to_reducers_exchange",
            "BARRIER_QUEUE_NAME=team_group_by_civ_reducers_barrier",
            "REDUCERS_OUTPUT_QUEUE_NAME=team_group_by_civ_reducers_to_top_5_calculador_queue",
            "REDUCERS_QUEUE_PREFIX=group_by_civ_reducer_team_"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    node = "winner_rate_calculator"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./civs_calculators:/winner_rate_calculator",
            "./communications:/winner_rate_calculator/communications",
            "./config:/winner_rate_calculator/config",
            "./logger:/winner_rate_calculator/logger"
        ],
        "entrypoint": "python3 /winner_rate_calculator/winner_rate_calculator.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "INPUT_QUEUE_NAME=1v1_group_by_civ_reducers_to_winner_rate_calculador_queue"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    node = "top_5_times_used_calculator"
    nodes.append(node)
    compose_config["services"][node] = {
        "container_name": node,
        "image": "rabbitmq-python-base:0.0.1",
        "volumes": [
            "./civs_calculators:/top_5_times_used_calculator",
            "./communications:/top_5_times_used_calculator/communications",
            "./config:/top_5_times_used_calculator/config",
            "./logger:/top_5_times_used_calculator/logger"
        ],
        "entrypoint": "python3 /top_5_times_used_calculator/top_5_times_used_calculator.py",
        "environment": [
            "TZ=America/Argentina/Buenos_Aires",
            "RABBITMQ_HOST=rabbitmq",
            "INPUT_QUEUE_NAME=team_group_by_civ_reducers_to_top_5_calculador_queue"
        ],
        "networks": [
            "age_of_empires_net"
        ]
    }

    for i in range(n1):
        id = get_id(i)
        node = "group_by_match_reducer_" + id
        compose_config["services"][node] = {
            "container_name": node,
            "image": "rabbitmq-python-base:0.0.1",
            "volumes": [
                "./group_by_match:/age_of_empires_ii_da",
                "./communications:/age_of_empires_ii_da/communications",
                "./master_reducers_arq:/age_of_empires_ii_da/master_reducers_arq",
                "./config:/age_of_empires_ii_da/config",
                "./logger:/age_of_empires_ii_da/logger",
                "../age_of_empires_servers_data/" + node + ":/data",
            ],
            "entrypoint": "python3 /age_of_empires_ii_da/reducer.py",
            "environment": [
                "TZ=America/Argentina/Buenos_Aires",
                "RABBITMQ_HOST=rabbitmq",
                "MINIMUM_RATING=1000",
                "MINIMUM_RATING_PORCENTAGE_DIFF=30",
                "INPUT_QUEUE_NAME=" + node,
                "REDUCER_ID=" + id,
            ],
            "networks": [
                "age_of_empires_net"
            ]
        }

    for i in range(n3):
        id = get_id(i)
        node = "group_by_civ_reducer_1v1_" + id
        nodes.append(node)
        compose_config["services"][node] = {
            "container_name": node,
            "image": "rabbitmq-python-base:0.0.1",
            "volumes": [
                "./group_by_civ:/group_by_civ",
                "./communications:/group_by_civ/communications",
                "./master_reducers_arq:/group_by_civ/master_reducers_arq",
                "./config:/group_by_civ/config",
                "./logger:/group_by_civ/logger"
            ],
            "entrypoint": "python3 /group_by_civ/count_wins_and_defeats_reducer.py",
            "environment": [
                "TZ=America/Argentina/Buenos_Aires",
                "RABBITMQ_HOST=rabbitmq",
                "BARRIER_QUEUE_NAME=1v1_group_by_civ_reducers_barrier",
                "OUTPUT_QUEUE_NAME=1v1_group_by_civ_reducers_to_winner_rate_calculador_queue",
                "INPUT_QUEUE_NAME=" + node,
                "REDUCER_ID=" + id,
            ],
            "networks": [
                "age_of_empires_net"
            ]
        }

    for i in range(n5):
        id = get_id(i)
        node = "group_by_civ_reducer_team_" + id
        nodes.append(node)
        compose_config["services"][node] = {
            "container_name": node,
            "image": "rabbitmq-python-base:0.0.1",
            "volumes": [
                "./group_by_civ:/group_by_civ",
                "./communications:/group_by_civ/communications",
                "./master_reducers_arq:/group_by_civ/master_reducers_arq",
                "./config:/group_by_civ/config",
                "./logger:/group_by_civ/logger"
            ],
            "entrypoint": "python3 /group_by_civ/count_times_used_reducer.py",
            "environment": [
                "TZ=America/Argentina/Buenos_Aires",
                "RABBITMQ_HOST=rabbitmq",
                "BARRIER_QUEUE_NAME=team_group_by_civ_reducers_barrier",
                "OUTPUT_QUEUE_NAME=team_group_by_civ_reducers_to_top_5_calculador_queue",
                "INPUT_QUEUE_NAME=" + node,
                "REDUCER_ID=" + id,
            ],
            "networks": [
                "age_of_empires_net"
            ]
        }

    for i in range(n2):
        id = get_id(i)
        node = "join_reducer_1v1_" + id
        nodes.append(node)
        compose_config["services"][node] = {
            "container_name": node,
            "image": "rabbitmq-python-base:0.0.1",
            "volumes": [
                "./join_matches_and_players:/join_matches_and_players",
                "./communications:/join_matches_and_players/communications",
                "./master_reducers_arq:/join_matches_and_players/master_reducers_arq",
                "./config:/join_matches_and_players/config",
                "./logger:/join_matches_and_players/logger",
                "../age_of_empires_servers_data/" + node + ":/data"
            ],
            "entrypoint": "python3 /join_matches_and_players/reducer.py",
            "environment": [
                "TZ=America/Argentina/Buenos_Aires",
                "RABBITMQ_HOST=rabbitmq",
                "BARRIER_QUEUE_NAME=1v1_join_reducers_barrier",
                "OUTPUT_QUEUE_NAME=1v1_join_reducers_to_group_by_civ_master_queue",
                "INPUT_QUEUE_NAME=" + node,
                "REDUCER_ID=" + id,
            ],
            "networks": [
                "age_of_empires_net"
            ]
        }

    for i in range(n4):
        id = get_id(i)
        node = "join_reducer_team_" + id
        nodes.append(node)
        compose_config["services"][node] = {
            "container_name": node,
            "image": "rabbitmq-python-base:0.0.1",
            "volumes": [
                "./join_matches_and_players:/join_matches_and_players",
                "./communications:/join_matches_and_players/communications",
                "./master_reducers_arq:/join_matches_and_players/master_reducers_arq",
                "./config:/join_matches_and_players/config",
                "./logger:/join_matches_and_players/logger"
            ],
            "entrypoint": "python3 /join_matches_and_players/reducer.py",
            "environment": [
                "TZ=America/Argentina/Buenos_Aires",
                "RABBITMQ_HOST=rabbitmq",
                "BARRIER_QUEUE_NAME=team_join_reducers_barrier",
                "OUTPUT_QUEUE_NAME=team_join_reducers_to_group_by_civ_master_queue",
                "INPUT_QUEUE_NAME=" + node,
                "REDUCER_ID=" + id,
            ],
            "networks": [
                "age_of_empires_net"
            ]
        }

    node = "supervisor"
    supervisors = []
    for i in range(n6):
        id = get_id(i)
        supervisors.append(node + id)
    for i in range(n6):
        id = get_id(i)
        compose_config["services"][node + id] = {
            "container_name": node + id,
            "image": "supervisor:0.0.1",
            "volumes": [
                "./supervisor:/supervisor",
                "./ring:/supervisor/ring",
                "./healthcheck:/supervisor/healthcheck",
                "./logger:/supervisor/logger",
                "./config:/supervisor/config",
                "/var/run/docker.sock:/var/run/docker.sock"
            ],
            "entrypoint": "python3 /supervisor/supervisor.py",
            "environment": [
                "TZ=America/Argentina/Buenos_Aires",
                "SUPERVISOR_NAME=" + node + id,
                "SUPERVISORS=" + ','.join(supervisors),
                "NODES=" + ','.join(nodes),
            ],
            "depends_on": nodes,
            "networks": [
                "age_of_empires_net"
            ]
        }

    compose_config["networks"] = {
        "age_of_empires_net": {
            "external": {
                "name": "age_of_empires_net"
            }
        }
    }
  
    with open("docker-compose-client-and-servers.yaml", "w") as file:
        yaml.dump(compose_config, file, default_flow_style=False, sort_keys=False,Dumper=NoAliasDumper)

if __name__ == '__main__':
    args = sys.argv
    generate_compose_yaml(int(args[1]),int(args[2]),int(args[3]),int(args[4]),int(args[5]),int(args[6]))
