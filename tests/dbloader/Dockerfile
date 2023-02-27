FROM rust:latest
RUN apt update
RUN apt install -y libpq-dev
RUN cargo install diesel_cli --no-default-features --features postgres
WORKDIR /eureka-substreams-sink
ENV DATABASE_URL postgres://dev-node:insecure-change-me-in-prod@postgres:5432/dev-node?sslmode=disable
EXPOSE 5432
COPY . .
RUN apt-get install protobuf-compiler -y
CMD ["cd eureka-sink-postgres"]
RUN cargo build

FROM postgres:latest
