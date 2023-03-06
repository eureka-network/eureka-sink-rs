create table if not exists ethereummainnet.eth_blockheaders
(
    id          text not null constraint eth_blockheaders_pk primary key,
    number      integer,
    hash        text,
    parent_hash text,
    timestamp   text
);
