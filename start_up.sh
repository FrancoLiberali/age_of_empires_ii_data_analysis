#!/bin/bash
error () {
	echo >&2 "$@"
	exit 1
}

[ "$#" -eq 6 ] || error "Reducers amount is required. Usage: ./start_up.sh n1 n2 n3 n4 n5 n6

n1: Amount of Reducers for group by match
n2: Amount of Reducers for join matches 1v1 and players
n3: Amount of Reducers for group players of matches 1v1 by civ
n4: Amount of Reducers for join team matches and players
n5: Amount of Reducers for group players of team matches by civ
n6: Amount of supervisors"

sudo apt-get install python3-venv
python3 -m venv age_of_empires_ii_da
source age_of_empires_ii_da/bin/activate
pip3 install pyyaml==5.4.1
python3 generate_compose_yaml.py $1 $2 $3 $4 $5 $6
docker-compose -f docker-compose-rabbit.yaml up -d --remove-orphans
sleep 25

docker-compose -f docker-compose-client-and-servers.yaml up -d
docker-compose -f docker-compose-client-and-servers.yaml logs -f