FROM maven:3.9-amazoncorretto-17-debian
RUN apt-get update && \
    apt-get install -yq wget procps && \
    wget https://archive.apache.org/dist/pulsar/pulsar-2.11.1/apache-pulsar-2.11.1-bin.tar.gz && \
    tar xvfz apache-pulsar-2.11.1-bin.tar.gz && \
    cd apache-pulsar-2.11.1 && \
    pwd