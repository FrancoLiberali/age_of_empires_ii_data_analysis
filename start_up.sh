#!/bin/bash
docker-compose -f docker-compose-rabbit.yaml up -d
sleep 5
docker-compose -f docker-compose-client-and-servers.yaml up -d
docker-compose -f docker-compose-client-and-servers.yaml logs -f