create table if not exists ethereummainnet.eth_block_headers
(
    id          text not null constraint eth_block_headers_pk primary key,
    at          text,
    number      integer,
    hash        text,
    parent_hash text,
    timestamp   text
);
