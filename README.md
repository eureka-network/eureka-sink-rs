# Substreams Sink PostgreSQL
A Rust library to synchronize Substreams manifest data ingestion to PostgreSQL.

## Connect to a firehose instance (for now hosted by Streaming Fast)

copy your `STREAMINGFAST_KEY` into a `.env` file. (You can get an API key on https://app.streamingfast.io/)

then run `source ./firehouse_token.sh` to get `SUBSTREAMS_API_TOKEN` set in your local shell.

## Testing
Generate a Substreams .spkg file, using a specific manifest specification and using the command

`substreams pack substreams_manifest.yaml`.

Copy and paste the generated .spkg file to the current repo source folder.
