#[macro_use]
extern crate log;

mod db_resolver_state;
mod link_resolvers;
pub mod resolver;
pub mod wasm;
pub use link_resolvers::{
    arweave::ArweaveLinkResolver, https::HTTPSLinkResolver, ipfs::IpfsLinkResolver,
};
pub use resolver::{ContentParser, LinkResolver, Message, ResolveTask, Resolver, TaskState};
pub use wasm::Parser;
