#!/bin/bash
docker-compose -f docker-compose-rabbit.yaml up -d
sleep 5
export REDUCERS_AMOUNT=6
docker-compose -f docker-compose-client-and-servers.yaml up -d --scale group_by_match_server=6
docker-compose -f docker-compose-client-and-servers.yaml logs -f