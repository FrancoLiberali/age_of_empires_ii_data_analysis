#!/bin/bash
error () {
	echo >&2 "$@"
	exit 1
}

[ "$#" -eq 5 ] || error "Reducers amounter is required. Usage: ./start_up.sh n1 n2 n3 n4 n5

n1: Reducers amount of group by match
n2: Reducers amount of join matches 1v1 and players
n3: Reducers amount of group players of matches 1v1 by civ
n4: Reducers amount of join team matches and players
n5: Reducers amount of group players of team matches by civ"

docker-compose -f docker-compose-rabbit.yaml up -d --remove-orphans
sleep 25

export REDUCERS_AMOUNT_GROUP_BY_MATCH=$1
export REDUCERS_AMOUNT_JOIN_1V1=$2
export REDUCERS_AMOUNT_GROUP_BY_CIV_1V1=$3
export REDUCERS_AMOUNT_JOIN_TEAM=$4
export REDUCERS_AMOUNT_GROUP_BY_CIV_TEAM=$5
docker-compose -f docker-compose-client-and-servers.yaml up -d client filter_by_avr_rating_duration_and_server filter_by_rating filter_by_ladder_map_and_mirror group_by_match_master join_master_1v1 join_master_team group_by_civ_master_1v1 group_by_civ_master_team winner_rate_calculator top_5_times_used_calculator supervisor1 supervisor2 supervisor3 supervisor4 supervisor5

for i in $(seq 1 $(($1 - 1)))
do
	REDUCER_ID=$i docker-compose -f docker-compose-client-and-servers.yaml up --scale group_by_match_reducer=$i --no-start --no-recreate group_by_match_reducer;
done
REDUCER_ID=$1 docker-compose -f docker-compose-client-and-servers.yaml up -d --scale group_by_match_reducer=$1 --no-recreate group_by_match_reducer;
for i in $(seq 1 $(($2 - 1)))
do
	REDUCER_ID=$i docker-compose -f docker-compose-client-and-servers.yaml up --scale join_reducer_1v1=$i --no-start --no-recreate join_reducer_1v1;
done
REDUCER_ID=$2 docker-compose -f docker-compose-client-and-servers.yaml up -d --scale join_reducer_1v1=$2 --no-recreate join_reducer_1v1;
for i in $(seq 1 $(($3 - 1)))
do
	REDUCER_ID=$i docker-compose -f docker-compose-client-and-servers.yaml up --scale group_by_civ_reducer_1v1=$i --no-start --no-recreate group_by_civ_reducer_1v1;
done
REDUCER_ID=$3 docker-compose -f docker-compose-client-and-servers.yaml up -d --scale group_by_civ_reducer_1v1=$3 --no-recreate group_by_civ_reducer_1v1;
for i in $(seq 1 $(($4 - 1)))
do
	REDUCER_ID=$i docker-compose -f docker-compose-client-and-servers.yaml up --scale join_reducer_team=$i --no-start --no-recreate join_reducer_team;
done
REDUCER_ID=$4 docker-compose -f docker-compose-client-and-servers.yaml up -d --scale join_reducer_team=$4 --no-recreate join_reducer_team;
for i in $(seq 1 $5)
do
	REDUCER_ID=$i docker-compose -f docker-compose-client-and-servers.yaml up --scale group_by_civ_reducer_team=$i --no-start --no-recreate group_by_civ_reducer_team;
done
REDUCER_ID=$5 docker-compose -f docker-compose-client-and-servers.yaml up -d --scale group_by_civ_reducer_team=$5 --no-recreate group_by_civ_reducer_team;

docker-compose -f docker-compose-client-and-servers.yaml logs -f