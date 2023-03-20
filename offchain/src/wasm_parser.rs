use anyhow::{anyhow, Result};
use futures::executor::block_on;
use wasmer::{
    imports, Cranelift, Function, FunctionEnv, FunctionEnvMut, Instance, Memory, Module, Store,
};

use crate::{resolver::ResolveTask, ContentParser};
use int_enum::IntEnum;
use prost::Message;
use sqlx::{PgPool, Postgres, QueryBuilder};
use substreams_sink::{pb, OffchainDataContent, OffchainDataRecord, OffchainDataRecords};

struct MyEnv {
    memory: Option<Memory>,
    connection_pool: PgPool,
}

/// Wasm parser
pub struct WasmParser {
    store: Store,
    env: FunctionEnv<MyEnv>,
    _module: Module,
    instance: Instance,
}

impl WasmParser {
    pub fn new(code: &[u8], connection_pool: PgPool) -> Result<Self> {
        let mut store = Store::new(Cranelift::default());
        let env = FunctionEnv::new(
            &mut store,
            MyEnv {
                memory: None,
                connection_pool: connection_pool.clone(),
            },
        );
        let module = Module::new(&store, code).unwrap();

        fn logger(_env: FunctionEnvMut<MyEnv>, _ptr: i32, _len: i32) {
            debug!("Calling logger");
        }

        fn output(env: FunctionEnvMut<MyEnv>, ptr: i32, len: i32) {
            let mut buf = vec![0; len as usize];
            let memory = env.data().memory.as_ref().unwrap().clone();
            memory.view(&env).read(ptr as u64, &mut buf).unwrap();
            let records: OffchainDataRecords = Message::decode(buf.as_slice()).unwrap();
            debug!("Received result {} {} {:?}", ptr, len, records);

            block_on(async move {
                if records.records.len() == 0 {
                    let _ = sqlx::query!(
                        "UPDATE resolver_tasks SET state = $1 WHERE uri = $2 AND manifest = $3",
                        crate::TaskState::ParsingFailed.int_value(),
                        records.uri,
                        records.manifest,
                    )
                    .execute(&mut env.data().connection_pool.acquire().await.unwrap())
                    .await;
                } else {
                    for record in &records.records {
                        match build_query(&records.manifest, record) {
                            Ok(mut query) => {
                                let query = query.build();
                                let _ = query
                                    .execute(
                                        &mut env.data().connection_pool.acquire().await.unwrap(),
                                    )
                                    .await;
                            }
                            Err(_e) => {
                                error!("Failed to insert content");
                                let _ = sqlx::query!(
                                    "UPDATE resolver_tasks SET state = $1 WHERE uri = $2 AND manifest = $3",
                                    crate::TaskState::ParsingFailed.int_value(),
                                    records.uri,
                                    records.manifest,
                                )
                                .execute(&mut env.data().connection_pool.acquire().await.unwrap())
                                .await;
                            }
                        }
                    }
                }
            })
        }

        fn register_panic(
            _env: FunctionEnvMut<MyEnv>,
            _msg_ptr: i32,
            _msg_len: i32,
            _file_ptr: i32,
            _file_len: i32,
            _line: i32,
            _column: i32,
        ) {
            debug!("Calling register_panic");
        }

        let imports = imports! {
            "logger" => {
                "println" => Function::new_typed_with_env(&mut store, &env, logger),
            },
            "env" => {
                "logger" => Function::new_typed_with_env(&mut store, &env, logger),
                "println" => Function::new_typed_with_env(&mut store, &env, logger),
                "output" => Function::new_typed_with_env(&mut store, &env, output),
                "register_panic" => Function::new_typed_with_env(&mut store, &env, register_panic),
            },
        };

        let instance = Instance::new(&mut store, &module, &imports).unwrap();
        let memory = instance.exports.get_memory("memory").unwrap();
        env.as_mut(&mut store).memory = Some(memory.clone());
        //let memory_view = memory.view(&store);

        Ok(Self {
            store,
            _module: module,
            env,
            instance,
        })
    }
}
impl ContentParser for WasmParser {
    fn parse(&mut self, task: &ResolveTask, content: Vec<u8>) -> Result<()> {
        let content = OffchainDataContent {
            uri: task.request.uri.clone(),
            manifest: task.manifest.clone(),
            content: String::from_utf8(content).expect("failed to decode content"),
        };
        let msg = content.encode_to_vec();

        let memory = self
            .env
            .as_ref(&self.store)
            .memory
            .as_ref()
            .unwrap()
            .clone();
        let memory_view = memory.view(&self.store);

        memory_view.write(0, msg.as_slice()).unwrap();
        let map_content_uri = self
            .instance
            .exports
            .get_function(&task.request.handler)
            .unwrap();
        map_content_uri
            .call(
                &mut self.store,
                &[wasmer::Value::I32(0), wasmer::Value::I32(msg.len() as i32)],
            )
            .unwrap();
        Ok(())
    }
}

fn build_query<'args>(
    manifest: &str,
    record: &'args OffchainDataRecord,
) -> Result<QueryBuilder<'args, Postgres>> {
    // Collect column names and args
    let mut names: Vec<String> = Vec::new();
    let mut args: Vec<&pb::Field> = Vec::new();
    let mut query =
        QueryBuilder::<Postgres>::new(format!("INSERT INTO {}.{} (", manifest, record.record));
    let mut separated = query.separated(", ");
    for field in &record.fields {
        let typed = field.new_value.as_ref().unwrap().typed.to_owned().unwrap();
        match typed {
            pb::value::Typed::Offchaindata(_) | pb::value::Typed::Array(_) => {
                return Err(anyhow!("not supported"));
            }
            _ => {
                names.push(field.name.clone());
                separated.push(field.name.clone());
                args.push(&field);
            }
        };
    }
    separated.push_unseparated(") VALUES (");

    // Bind args
    let mut bound = query.separated(", ");
    use substreams_sink::pb::value::Typed;
    for v in args {
        let typed = v.new_value.as_ref().unwrap().typed.to_owned().unwrap();
        match typed {
            Typed::Int32(v) => {
                bound.push_bind(v);
            }
            Typed::Uint32(v) => {
                bound.push_bind(i32::try_from(v)?);
            }
            Typed::Int64(v) => {
                bound.push_bind(v);
            }
            Typed::Uint64(v) => {
                bound.push_bind(i64::try_from(v)?);
            }
            Typed::Bigdecimal(v) => {
                bound.push_bind(v.to_owned());
            }
            Typed::Bytes(v) => {
                bound.push_bind(v);
            }
            Typed::Bool(v) => {
                bound.push_bind(v);
            }
            Typed::String(v) => {
                bound.push_bind(v);
            }
            _ => panic!("shouldn't get here"),
        };
    }
    bound.push_unseparated(")");
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;
    use substreams_sink::pb::{value::Typed, Value};

    #[test]
    fn test_output_callback() -> anyhow::Result<()> {
        let test = pb::OffchainDataRecord {
            record: "table".to_string(),
            fields: vec![
                pb::Field {
                    name: "test".to_string(),
                    new_value: Some(Value {
                        typed: Some(Typed::String("test".to_string())),
                    }),
                    old_value: None,
                },
                pb::Field {
                    name: "state".to_string(),
                    new_value: Some(Value {
                        typed: Some(Typed::Int32(1)),
                    }),
                    old_value: None,
                },
            ],
        };
        assert_eq!(
            build_query("manifest", &test)?.into_sql(),
            "INSERT INTO manifest.table (test, state) VALUES ($1, $2)"
        );
        Ok(())
    }
}
