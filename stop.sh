#!/bin/bash
docker-compose -f docker-compose-client-and-servers.yaml stop
docker-compose -f docker-compose-client-and-servers.yaml down
docker-compose -f docker-compose-rabbit.yaml stop
docker-compose -f docker-compose-rabbit.yaml down