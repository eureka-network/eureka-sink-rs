#[macro_use]
extern crate log;

mod db_resolver_state;
mod link_resolvers;
mod resolver;
mod wasm_parser;
pub use link_resolvers::{https::HTTPSLinkResolver, ipfs::IPFSLinkResolver};
pub use resolver::{ContentParser, LinkResolver, Resolver, TaskState};
pub use wasm_parser::WasmParser;
