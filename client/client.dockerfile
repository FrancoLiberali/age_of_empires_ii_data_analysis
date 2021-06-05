FROM rabbitmq-python-base:0.0.1

COPY client.py /client.py
CMD /client.py