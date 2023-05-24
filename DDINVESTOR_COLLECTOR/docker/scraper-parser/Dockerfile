FROM ubuntu:18.04

RUN apt-get update

RUN apt-get -y install python3.6 &&\
    apt-get -y install python3-pip

RUN apt-get install -y locales locales-all
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8

COPY /data_collector/requirements.txt /data_collector/requirements.txt

RUN pip3 install -r /data_collector/requirements.txt

COPY /data_collector /data_collector

WORKDIR /data_collector

