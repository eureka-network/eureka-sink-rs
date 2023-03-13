create table if not exists polygonmainnet.lens_posts
(
    id          text not null constraint lens_posts_pk primary key,
    profile_id  text,
    content_uri text,
    timestamp   bigint
);
