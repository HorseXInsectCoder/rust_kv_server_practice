pub mod abi;

use http::StatusCode;
use abi::*;
use crate::command_request::RequestData;
use crate::errors::KvError;

impl CommandRequest {
    /// 创建 HSET 命令
    pub fn new_hset(table: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        Self {
            request_data: Some(RequestData::Hset(Hset {
                table: table.into(),
                pair: Some(KvPair::new(key, value))
            }))
        }
    }

    /// 创建 HSET 命令
    pub fn new_hget(table: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hget(Hget {
                table: table.into(),
                key: key.into()
            }))
        }
    }

    /// 创建 HGETALL 命令
    pub fn new_hgetall(table: impl Into<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hgetall(Hgetall {
                table: table.into(),
            }))
        }
    }

    /// 创建 HMGET 命令
    pub fn new_hmget(table: impl Into<String>, keys: Vec<String>) -> Self {
        Self {
            request_data: Some(RequestData::Hmget(Hmget {
                table: table.into(),
                keys,
            }))
        }
    }
}

impl KvPair {
    /// 创建一个新的 kv pair
    pub fn new(key: impl Into<String>, value: Value) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
}

/// 从 String 转换成 Value
impl From<String> for Value {
    fn from(s: String) -> Self {
        Self {
            value: Some(value::Value::String(s))
        }
    }
}

/// 从 &str 转换成 Value
impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self {
            value: Some(value::Value::String(s.into()))
        }
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Self {
            value: Some(value::Value::Integer(i))
        }
    }
}

/// 从 Value 转换成 CommandResponse
impl From<Value> for CommandResponse {
    fn from(v: Value) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,           // 新用法，这里status本来要接受u32，但StatusCode只要转成u16
            values: vec![v],
            ..Default::default()
        }
    }
}

/// 从 Vec<Kvpair> 转换成 CommandResponse
impl From<Vec<KvPair>> for CommandResponse {
    fn from(pairs: Vec<KvPair>) -> Self {
        Self {
            status: StatusCode::OK.as_u16() as _,
            pairs,
            ..Default::default()
        }
    }
}


/// 从 KvError 转换成 CommandResponse
impl From<KvError> for CommandResponse {
    fn from(e: KvError) -> Self {
        let mut result = Self {
            status: StatusCode::INTERNAL_SERVER_ERROR.as_u16() as _,
            message: e.to_string(),
            ..Default::default()
        };

        match e {
            KvError::NotFound(_, _) => result.status = StatusCode::NOT_FOUND.as_u16() as _,
            KvError::InvalidCommand(_) => result.status = StatusCode::BAD_REQUEST.as_u16() as _,
            _ => {}
        }

        result
    }
}

impl From<Vec<Value>> for CommandResponse {
    fn from(_: Vec<Value>) -> Self {
        todo!()
    }
}

// #[derive(Debug)]
// struct KvVecString(Vec<String>);
//
// impl ToString for KvVecString {
//     fn to_string(&self) -> String {
//         format!("{:?}", self.0)
//     }
// }

#[derive(Debug)]
pub struct StringWrapper(pub(crate) String);

impl From<Vec<String>> for StringWrapper {
    fn from(sw: Vec<String>) -> Self {
        Self {
            0: format!("{:?}", sw)
        }
    }
}