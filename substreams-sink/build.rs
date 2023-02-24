// credit to https://gist.github.com/danburkert/02f8010714a8f86cabcb854a5a2d9e09
extern crate curl;
extern crate prost_build;
extern crate tempdir;

use std::{env, fs};
use std::ffi::OsString;
use std::fs::read_dir;
use std::io;
use std::io::{Write, ErrorKind};
use std::path::{Path, PathBuf};

// use tempdir::TempDir;

const TAG: &'static str = "v0.2.0";
const URL_BASE: &'static str = "https://raw.githubusercontent.com/streamingfast/substreams";

const PROTOS: &[&str] = &[
    "proto/sf/substreams/v1/substreams.proto",
    "proto/sf/substreams/v1/package.proto",
];

fn main() {
    // let kudu_home = match env::var("KUDU_HOME") {
    //     Ok(kudu_home) => PathBuf::from(kudu_home),
    //     Err(_) => {
    //         let kudu_home =
    //             PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR environment variable not set"))
    //                     .join(format!("apache-kudu-{}", VERSION));

    //         if !kudu_home.exists() {
    //             download_protos(&kudu_home);
    //         }
    //         kudu_home
    //     },
    // };

    let root_path = match get_project_root() {
        Ok(path) => path,
        Err(e) => panic!("Could not get project root: {}", e),
    };

    optional_download_proto_if_not_exist(&root_path);

    // download_protofiles(&root_path);

    println!("root path: {}", root_path.display());

    // root_path.join(format!("substreams-{}", TAG));

    // let protos = PROTOS.iter()
    //                    .map(|proto| kudu_home.join("src").join("kudu").join(proto))
    //                    .collect::<Vec<_>>();

    // prost_build::compile_protos(&protos, &[kudu_home.join("src")]).unwrap();
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
fn optional_download_proto_if_not_exist(root_path: &Path) {
    let mut path = root_path.to_owned();
    path.push("proto/import-substreams");
    fs::create_dir_all(&path).unwrap();
    
}

// downloads proto file from github into a path
// fn download_protofiles(target: &Path) {
//     // let tempdir = TempDir::new_in(target.parent().unwrap(), "proto_download").unwrap();
//     let mut path = target.to_owned();
//     path.push("proto2");
//     fs::create_dir(&path).unwrap();
//     path.push("download-test");
//     fs::create_dir(&path).unwrap();
// }

// fn download_protos(target: &Path) {
//     let tempdir = TempDir::new_in(target.parent().unwrap(), "proto-download").unwrap();
//     let mut path = tempdir.path().to_owned();
//     path.push("src");
//     fs::create_dir(&path).unwrap();
//     path.push("kudu");
//     fs::create_dir(&path).unwrap();

//     let mut data = Vec::new();
//     for proto in PROTOS {
//         data.clear();
//         let proto_path = path.join(proto);
//         fs::create_dir_all(proto_path.parent().unwrap()).unwrap();

//         let mut handle = Easy::new();

//         handle.url(&format!("{url_base}/{version}/src/kudu/{proto}",
//                             url_base=URL_BASE,
//                             version=VERSION,
//                             proto=proto))
//               .expect("failed to configure Kudu URL");
//         handle.follow_location(true)
//               .expect("failed to configure follow location");
//         {
//             let mut transfer = handle.transfer();
//             transfer.write_function(|new_data| {
//                 data.extend_from_slice(new_data);
//                 Ok(new_data.len())
//             }).expect("failed to write download data");
//             transfer.perform().expect("failed to download Kudu source tarball");
//         }

//         fs::File::create(proto_path).unwrap().write_all(&data).unwrap();
//     }

//     fs::rename(&tempdir.into_path(), target).expect("unable to move temporary directory");
// }
