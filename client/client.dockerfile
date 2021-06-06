FROM rabbitmq-python-base:0.0.1

COPY ./datasets/matches.csv /matches.csv
COPY ./datasets/match_players.csv /match_players.csv
CMD /client/main.py