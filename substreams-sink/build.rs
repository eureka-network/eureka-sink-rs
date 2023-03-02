// credit to and adapted from https://gist.github.com/danburkert/02f8010714a8f86cabcb854a5a2d9e09
extern crate curl;
extern crate prost_build;
extern crate tempdir;

use std::{env, fs};
use std::ffi::OsString;
use std::fs::read_dir;
use std::io;
use std::io::{Write, ErrorKind};
use std::path::{Path, PathBuf};

use curl::easy::Easy;

const TAG: &'static str = "v0.2.0";
const URL_BASE: &'static str = "https://raw.githubusercontent.com/streamingfast/substreams";

const PROTO_IMPORT: &'static str = "proto/import-substreams";
const PROTOS: &[&str] = &[
    "sf/substreams/v1/substreams.proto",
    "sf/substreams/v1/package.proto",
    // imported dependencies
    "sf/substreams/v1/modules.proto",
    "sf/substreams/v1/modules.proto",
    "sf/substreams/v1/clock.proto",
    "google/protobuf/descriptor.proto",
    "google/protobuf/any.proto",
    "google/protobuf/timestamp.proto",
];

fn main() {
    let root_path = match get_project_root() {
        Ok(path) => path,
        Err(e) => panic!("Could not get project root: {}", e),
    };

    optional_download_protos_if_not_exist(&root_path);

    let imported_protos = PROTOS.iter()
                       .map(|proto| root_path.join(PROTO_IMPORT).join(proto))
                       .collect::<Vec<_>>();

    // prost_build::compile_protos(&imported_protos, "TODO: INCLUDES NOW OK?"").unwrap();
}

// return the path of the root of the current repository
// by looking for rust-toolchain.toml
fn get_project_root() -> io::Result<PathBuf> {
    let path = env::current_dir()?;
    let mut path_ancestors = path.as_path().ancestors();

    while let Some(p) = path_ancestors.next() {
        let has_toolchain =
            read_dir(p)?
                .into_iter()
                .any(|p| p.unwrap().file_name() == OsString::from("rust-toolchain.toml"));
        if has_toolchain {
            return Ok(PathBuf::from(p))
        }
    }
    Err(io::Error::new(ErrorKind::NotFound, "Ran out of places to find rust-toolchain.toml"))
}

// if the download directory for substream proto files does not
// yet exist, then create the dir and download the proto files;
// otherwise do no-op
fn optional_download_protos_if_not_exist(root_path: &Path) {
    let mut path = root_path.to_owned();
    path.push(PROTO_IMPORT); // "proto/import-substreams"
    fs::create_dir_all(&path).unwrap();
    
    for proto in PROTOS {
        let proto_path = path.join(proto);
        if !proto_path.exists() {
            download_proto(proto, &proto_path);
        } else {
            println!("proto file already exists or could not access: {}",
                proto_path.display());
        }
    }
}

// download the proto file from github
// todo: is it better to download files first in a temp directory
//       and then move them to the target directory?
fn download_proto(proto: &&str, target: &Path) {
    let mut data = Vec::new();
    let mut handle = Easy::new();

    handle.url(&format!("{url_base}/{tag}/{proto}",
        url_base=URL_BASE,
        tag=TAG,
        proto=proto))
        .expect("failed to configure github URL");
    handle.follow_location(true)
        .expect("failed to configure follow location");

    {
        let mut transfer = handle.transfer();
        transfer.write_function(|new_data| {
            data.extend_from_slice(new_data);
            Ok(new_data.len())
        }).expect("failed to write download data");
        transfer.perform().expect("failed to download proto file from github");
    }

    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::File::create(target).unwrap().write_all(&data).unwrap();

}
