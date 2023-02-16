use std::fs;

use substreams::proto::decode;

use crate::Manifest;

#[test]
fn decode_manifest_to_modules() {
    let filename = "./substreams.yaml".to_string();
    let contents = fs::read(filename).unwrap();
    println!("contents = {:?}", contents);
    let _modules = decode::<Manifest>(&contents).unwrap();
}
