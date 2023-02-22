# Eureka Sink
A Rust library to synchronize Substreams manifest data ingestion to PostgreSQL.

## Getting Started

### Setting up PostGreSQL

Requires `docker` and `docker-compose` installed.

Run PostGreSQL and gpweb as a monitoring tool:
```
docker compose up
```
Open `http://localhost:8081` to see the PostgreSQL

## Building
We depend on `.proto` files from StreamingFast, to load these initialise git submodules:
```
  git submodule init
  git submodule update
```
then Cargo will generate the necessary protobuf definitions before building automatically
```
  cargo build
```

todo: setup demo folder with first integrated test
## Testing (incomplete information)
Generate a Substreams .spkg file, using a specific manifest specification and using the command

`substreams pack substreams_manifest.yaml`.

Copy and paste the generated .spkg file to the current repo source folder.
