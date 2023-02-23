# Test manifest

## Build
- install `protoc` compiler (https://github.com/protocolbuffers/protobuf/releases)
- `cargo build --target wasm32-unknown-unknown --release`

## Pack
`substreams pack`

## Test
`substreams run -e <firehose> eth-balance-v0.1.0.spkg ingest --stop-block +10 --start-block 110 -p`