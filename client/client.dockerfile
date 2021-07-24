FROM rabbitmq-python-base:0.0.1

COPY ./datasets /
CMD /client/main.py