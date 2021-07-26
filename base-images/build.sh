#!/bin/bash
docker build -f python-base.dockerfile -t rabbitmq-python-base:0.0.1 .
docker build -f supervisor.dockerfile -t supervisor:0.0.1 .