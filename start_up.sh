#!/bin/bash
error () {
	echo >&2 "$@"
	exit 1
}

[ "$#" -eq 1 ] || error "Reducers amounter is required. Usage: ./start_up.sh <reducers_amount>"

docker-compose -f docker-compose-rabbit.yaml up -d
sleep 5
export REDUCERS_AMOUNT=$1
docker-compose -f docker-compose-client-and-servers.yaml up -d --scale group_by_match_reducer_server=$1
docker-compose -f docker-compose-client-and-servers.yaml logs -f