# syntax=docker/dockerfile:1
FROM python:3.9.9-bullseye
WORKDIR /home
COPY requirements.txt requirements.txt
RUN apt update && apt install libpq-dev -y
RUN python3 -m pip install --upgrade pip && pip3 install -r requirements.txt
COPY . .
RUN chmod +x ./start.sh
CMD ./start.sh
