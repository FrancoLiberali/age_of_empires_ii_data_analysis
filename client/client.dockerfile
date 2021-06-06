FROM rabbitmq-python-base:0.0.1

COPY ./datasets/matches.csv /matches.csv
CMD /client/main.py