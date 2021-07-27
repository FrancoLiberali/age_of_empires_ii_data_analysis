FROM python:3.8-alpine
RUN apk update
RUN apk add docker
COPY . /src
WORKDIR /src
ENTRYPOINT python3 -u main.py
