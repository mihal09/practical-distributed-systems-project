FROM debian:bullseye-slim

RUN apt-get update && \
	apt-get install -y --no-install-recommends openjdk-11-jre-headless maven wget

WORKDIR "/opt/kafka"

RUN wget https://archive.apache.org/dist/kafka/3.8.0/kafka_2.12-3.8.0.tgz

RUN tar xvzf kafka_2.12-3.8.0.tgz -C /opt/kafka --strip-components=1

RUN rm kafka_2.12-3.8.0.tgz

COPY ["processor/", "/opt/processor"]

WORKDIR "/opt/processor"

RUN mnv clean package

ENTRYPOINT ["/bin/sh", "run.sh"]

