# Substreams Sink PostgreSQL
A Rust library to synchronize Substreams manifest data ingestion to PostgreSQL.

## Testing
Generate a Substreams .spkg file, using a specific manifest specification and using the command

`substreams pack substreams_manifest.yaml`.

Copy and paste the generated .spkg file to the current repo source folder.
