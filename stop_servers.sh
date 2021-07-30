#!/bin/bash
docker-compose -f docker-compose-servers.yaml stop
docker-compose -f docker-compose-servers.yaml down
sudo rm -rf last_hash/
sudo rm -rf ../age_of_empires_servers_data/