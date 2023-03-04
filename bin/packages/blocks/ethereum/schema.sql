create table if not exists block_meta
(
    id          text not null constraint block_meta_pk primary key,
    at          text,
    number      integer,
    hash        text,
    parent_hash text,
    timestamp   text
);
