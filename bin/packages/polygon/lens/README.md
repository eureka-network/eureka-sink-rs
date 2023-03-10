# Ethereum blocks package

This package will stream [Lens Protocol Contract](https://docs.lens.xyz/docs/deployed-contract-addresses) data.

## Getting started

- run a firehose endpoint that extracts the blockchain information of the ethereum-compatible chain.
- have a postgres instance which will function to anchor records from the chain. (run `docker compose up` from root)
- have cli built in `/bin/node/cli` with `cargo build --release`
- fill out a config file in `./config.toml` and run
    ```
        ../../../../target/release/eureka-cli -c ./config.toml
    ```

## To rebuild the package

Requirements:
- install `protoc` compiler (https://github.com/protocolbuffers/protobuf/releases) (for building manifest)
- install or have `substreams` available

then build:
-  the manifest to ingest block headers with
    ```
        cargo build --target wasm32-unknown-unknown --release
    ```
- the substream package
    ```
        substreams pack
    ```
