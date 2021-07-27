#!/bin/bash
error () {
	echo >&2 "$@"
	exit 1
}

[ "$#" -eq 6 ] || error "Reducers amounter is required. Usage: ./start_up.sh n1 n2 n3 n4 n5 n6

n1: Reducers amount of group by match
n2: Reducers amount of join matches 1v1 and players
n3: Reducers amount of group players of matches 1v1 by civ
n4: Reducers amount of join team matches and players
n5: Reducers amount of group players of team matches by civ
n6: Amount of supervisors"

python3 generate_compose_yaml.py $1 $2 $3 $4 $5 $6
docker-compose -f docker-compose-rabbit.yaml up -d --remove-orphans
sleep 25

docker-compose -f docker-compose-client-and-servers.yaml up -d
docker-compose -f docker-compose-client-and-servers.yaml logs -f