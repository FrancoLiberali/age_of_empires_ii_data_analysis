#!/bin/bash
./stop_servers.sh
docker-compose -f docker-compose-rabbit.yaml stop
docker-compose -f docker-compose-rabbit.yaml down