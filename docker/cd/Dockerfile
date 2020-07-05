FROM rust as builder

WORKDIR /opt/monolith

RUN env USER=root cargo init .

# Install golang
# Download Go 1.14.4 and install it to /usr/local/go
RUN wget -q https://golang.org/dl/go1.14.4.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.14.4.linux-amd64.tar.gz

# Let's people find our Go binaries
ENV PATH $PATH:/usr/local/go/bin
ENV PWD /opt/monolith

RUN go version

# Install cmake
RUN apt-get update && \
    apt-get upgrade -y && \
    apt install -y libprotobuf-dev protobuf-compiler && \
    apt-get install -y gcc make cmake

COPY Cargo.toml .
COPY Cargo.lock .

#RUN mkdir .cargo
#RUN cargo vendor > .cargo/config

COPY src /opt/monolith/src
COPY benches /opt/monolith/benches

RUN cd /opt/monolith/ && \
    cargo build --bins --release
