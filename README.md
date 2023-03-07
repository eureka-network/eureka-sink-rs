# Eureka Sink
A Rust library to synchronize Substreams manifest data ingestion to PostgreSQL.

## Connect to a firehose instance (for now hosted by Streaming Fast)

copy your `STREAMINGFAST_KEY` into a `.env` file. (You can get an API key on https://app.streamingfast.io/)

then run `source ./firehose_token.sh` to get `SUBSTREAMS_API_TOKEN` set in your local shell.

## Getting Started

### Setting up PostGreSQL
Requires `docker` and `docker-compose` installed.

Run PostGreSQL and gpweb as a monitoring tool:
```
docker compose up
```
Open `http://localhost:8081` to see the PostgreSQL

## Building
```
  cargo build
```

todo: setup demo folder with first integrated test
## Testing (incomplete information)
Generate a Substreams .spkg file, using a specific manifest specification and using the command

`substreams pack substreams_manifest.yaml`.

Copy and paste the generated .spkg file to the current repo source folder.
