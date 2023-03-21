# Extract events into Poseidon tree

This package will build a Poseidon tree for all events in an ethereum block.

## Getting started

- run a firehose endpoint that extracts the blockchain information of the ethereum-compatible chain.
- have a postgres instance which will function to anchor records from the chain. (run `docker compose up` from root)
- have cli built in `/bin/node/cli` with `cargo build -p eureka-cli --release`
- fill out a config file in `./config.toml` and run
    ```
        ../../../../target/release/eureka-cli -c ./config.toml
    ```

## To rebuild the package

Requirements:
- install `protoc` compiler (https://github.com/protocolbuffers/protobuf/releases) (for building manifest)
- install or have `substreams` available
- Plonky2 requires Rust nightly, so code nightly should be set on toolchain

then build:
-  the manifest to ingest block headers with
    ```
        cargo build --target wasm32-unknown-unknown --release
    ```
- the substream package
    ```
        substreams pack
    ```
