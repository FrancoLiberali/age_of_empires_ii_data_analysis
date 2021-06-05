FROM rabbitmq-python-base:0.0.1

COPY long_matches_server.py /long_matches_server.py
CMD /long_matches_server.py