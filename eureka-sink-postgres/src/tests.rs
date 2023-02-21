use std::fs;

use crate::{decode, pb::substreams::Manifest};

#[test]
fn decode_manifest_to_modules() {
    let filename = "./test-v0.1.0.spkg".to_string();
    let contents = fs::read(filename).unwrap();
    let _manifest = decode::<Manifest>(&contents).unwrap();
}
