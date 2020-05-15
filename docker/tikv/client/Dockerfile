FROM rust:latest
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*

COPY ./tikv-cli /opt/tikv-cli
RUN mkdir /opt/prometheus
WORKDIR /opt/prometheus
RUN wget https://github.com/prometheus/prometheus/releases/download/v2.18.1/prometheus-2.18.1.linux-amd64.tar.gz
RUN tar -xzf prometheus-2.18.1.linux-amd64.tar.gz

EXPOSE 9090


CMD ["sleep", "999"]
