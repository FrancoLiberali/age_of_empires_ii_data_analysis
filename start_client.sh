#!/bin/bash
error () {
	echo >&2 "$@"
	exit 1
}

[ "$#" -eq 2 ] || error "Datasets names required. Usage: ./start_client.sh matches_file_name players_file_name"

MATCHES_FILE_NAME=$1 PLAYERS_FILE_NAME=$2 docker-compose -f docker-compose-client.yaml run client
docker-compose -f docker-compose-client.yaml logs -f