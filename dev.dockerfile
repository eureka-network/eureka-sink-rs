FROM rust:latest
RUN apt update
RUN apt install -y libpq-dev
RUN cargo install diesel_cli --no-default-features --features postgres
WORKDIR /eureka-substreams-sink
COPY . .
RUN cargo install --path .
CMD bash -c "diesel setup"
