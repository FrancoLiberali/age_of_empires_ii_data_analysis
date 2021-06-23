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

docker-compose -f docker-compose-rabbit.yaml up -d
sleep 5
export REDUCERS_AMOUNT_GROUP_BY_MATCH=$1
export REDUCERS_AMOUNT_JOIN_1V1=$2
export REDUCERS_AMOUNT_GROUP_BY_CIV_1V1=$3
export REDUCERS_AMOUNT_JOIN_TEAM=$4
export REDUCERS_AMOUNT_GROUP_BY_CIV_TEAM=$5
docker-compose -f docker-compose-client-and-servers.yaml up -d --scale group_by_match_reducer=$1 --scale join_reducer_1v1=$2 --scale group_by_civ_reducer_1v1=$3 --scale join_reducer_team=$4 --scale group_by_civ_reducer_team=$5
docker-compose -f docker-compose-client-and-servers.yaml logs -f