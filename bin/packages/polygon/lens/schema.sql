create table if not exists polygonmainnet.lens_posts
(
    id          text not null constraint lens_posts_pk primary key,
    profile_id  text,
    content_uri text,
    timestamp   bigint
);

create table if not exists polygonmainnet.lens_posts_offchain
(
    uri     text,
    app_id  text,
    name    text,
    content text,
    PRIMARY KEY (uri, app_id)
);

