use serde::{Deserialize, Serialize};

use super::error::ParseError;
use super::{PostAttribute, PostMedia};
use crate::pb::{
    value::Typed, Field, OffchainDataContent, OffchainDataRecord, OffchainDataRecords, Value,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(non_snake_case)]
pub struct Post {
    appId: String,
    version: String,
    metadata_id: String,
    name: String,
    content: String,
    attributes: Vec<PostAttribute>,
    image: Option<String>,
    imageMimeType: Option<String>,
    media: Vec<PostMedia>,
}

pub fn parse(content: &OffchainDataContent) -> Result<OffchainDataRecords, ParseError> {
    let data: Post = serde_json::from_str(&content.content)
        .map_err(|e| ParseError::FormatError(e.to_string()))?;

    Ok(OffchainDataRecords {
        manifest: content.manifest.clone(),
        uri: content.uri.clone(),
        records: vec![OffchainDataRecord {
            record: "lens_posts_offchain".to_string(),
            fields: vec![
                Field {
                    name: "uri".to_string(),
                    new_value: Some(Value {
                        typed: Some(Typed::String(content.uri.clone())),
                    }),
                    old_value: None,
                },
                Field {
                    name: "app_id".to_string(),
                    new_value: Some(Value {
                        typed: Some(Typed::String(data.appId)),
                    }),
                    old_value: None,
                },
                Field {
                    name: "name".to_string(),
                    new_value: Some(Value {
                        typed: Some(Typed::String(data.name)),
                    }),
                    old_value: None,
                },
                Field {
                    name: "content".to_string(),
                    new_value: Some(Value {
                        typed: Some(Typed::String(data.content)),
                    }),
                    old_value: None,
                },
                 // todo: add fields
            ],
        }],
    })
}
