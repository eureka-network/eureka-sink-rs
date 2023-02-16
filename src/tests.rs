use substreams::pb::substreams::Modules;
use substreams::proto::decode;

use std::fs;

#[test]
fn decode_manifest_to_modules() {
    let filename = "./substreams.yaml".to_string();
    let contents = fs::read(filename).unwrap();
    let modules = decode::<Modules>(&contents).unwrap();
}
