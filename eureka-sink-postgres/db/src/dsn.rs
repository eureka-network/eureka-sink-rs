pub struct DSN {
    original: String,
    host: String,
    port: u64,
    username: String,
    password: String,
    database: String,
    schema: String,
    options: Vec<String>,
}

// impl DSN {
//     fn parse_dsn(dsn: String) -> Self {

//     }
// }
