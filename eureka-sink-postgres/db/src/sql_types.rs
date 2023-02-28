use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::convert::TryFrom;

use crate::error::DBError;

pub trait Sql {
    type T;
    type Inner;
    fn get_inner(&self) -> &Self::Inner;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Bool {
    pub inner: bool,
}

impl Sql for Bool {
    type T = diesel::sql_types::Bool;
    type Inner = bool;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SmallInt {
    pub inner: u16,
}

impl Sql for SmallInt {
    type T = diesel::sql_types::SmallInt;
    type Inner = u16;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type Int2 = SmallInt;

#[derive(Debug, Clone, PartialEq)]
pub struct Integer {
    pub inner: u32,
}

impl Sql for Integer {
    type T = diesel::sql_types::Integer;
    type Inner = u32;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type Int4 = Integer;

#[derive(Debug, Clone, PartialEq)]
pub struct BigInt {
    pub inner: u64,
}

impl Sql for BigInt {
    type T = diesel::sql_types::BigInt;
    type Inner = u64;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type Int8 = BigInt;

#[derive(Debug, Clone, PartialEq)]
pub struct Float {
    pub inner: f32,
}

impl Sql for Float {
    type T = diesel::sql_types::Float;
    type Inner = f32;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type Float4 = Float;

#[derive(Debug, Clone, PartialEq)]
pub struct Double {
    pub inner: f64,
}

impl Sql for Double {
    type T = diesel::sql_types::Double;
    type Inner = f64;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type Float8 = Double;

#[derive(Debug, Clone, PartialEq)]
pub struct Numeric {
    pub inner: BigDecimal,
}

impl Sql for Numeric {
    type T = diesel::sql_types::Numeric;
    type Inner = BigDecimal;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type Decimal = Numeric;

#[derive(Debug, Clone, PartialEq)]
pub struct Text {
    pub inner: String,
}

impl Sql for Text {
    type T = diesel::sql_types::Text;
    type Inner = String;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type VarChar = Text;

pub type Char = Text;

pub type TinyText = Text;

pub type MediumText = Text;

pub type LongText = Text;

#[derive(Debug, Clone, PartialEq)]
pub struct Binary {
    pub inner: Vec<u8>,
}

impl Sql for Binary {
    type T = diesel::sql_types::Binary;
    type Inner = Vec<u8>;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

pub type TinyBlob = Binary;

pub type Blob = Binary;

pub type MediumBlob = Binary;

pub type LongBlob = Binary;

pub type Varbinary = Binary;

pub type Bit = Binary;

#[derive(Debug, Clone, PartialEq)]
pub struct Date {
    pub inner: NaiveDate,
}

impl Sql for Date {
    type T = diesel::sql_types::Date;
    type Inner = NaiveDate;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Timestamp {
    pub inner: NaiveDateTime,
}

impl Sql for Timestamp {
    type T = diesel::sql_types::Timestamp;
    type Inner = NaiveDateTime;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Time {
    pub inner: NaiveTime,
}

impl Sql for Time {
    type T = diesel::sql_types::Time;
    type Inner = NaiveTime;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Interval {
    pub inner: pg_interval::Interval,
}

impl Sql for Interval {
    type T = diesel::sql_types::Interval;
    type Inner = pg_interval::Interval;
    fn get_inner(&self) -> &Self::Inner {
        &self.inner
    }
}

/// A native enumeration for diesel SQL types
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum SqlType {
    Bool(Bool),
    SmallInt(SmallInt),
    Int2(Int2),
    Integer(Integer),
    Int4(Int4),
    BigInt(BigInt),
    Int8(Int8),
    Float(Float),
    Float4(Float4),
    Double(Double),
    Float8(Float8),
    Numeric(Numeric),
    Decimal(Decimal),
    Text(Text),
    VarChar(VarChar),
    Char(Char),
    TinyText(TinyText),
    MediumText(MediumText),
    LongText(LongText),
    Binary(Binary),
    TinyBlob(TinyBlob),
    Blob(Blob),
    MediumBlob(MediumBlob),
    LongBlob(LongBlob),
    Varbinary(Varbinary),
    Bit(Bit),
    Date(Date),
    Interval(Interval),
    Time(Time),
    Timestamp(Timestamp),
}

impl SqlType {
    #[allow(dead_code)]
    pub fn to_string(&self) -> String {
        match self {
            Self::Bool(b) => format!("{}", b.get_inner()),
            Self::SmallInt(i) => format!("{}", i.get_inner()),
            Self::Int2(i) => format!("{}", i.get_inner()),
            Self::Integer(i) => format!("{}", i.get_inner()),
            Self::Int4(i) => format!("{}", i.get_inner()),
            Self::BigInt(i) => format!("{}", i.get_inner()),
            Self::Int8(i) => format!("{}", i.get_inner()),
            Self::Float(f) => format!("{}", f.get_inner()),
            Self::Float4(f) => format!("{}", f.get_inner()),
            Self::Double(d) => format!("{}", d.get_inner()),
            Self::Float8(f) => format!("{}", f.get_inner()),
            Self::Numeric(n) => format!("{}", n.get_inner().to_string()),
            Self::Decimal(d) => format!("{}", d.get_inner().to_string()),
            Self::Text(t) => format!("'{}'", t.get_inner()),
            Self::VarChar(v) => format!("'{}'", v.get_inner()),
            Self::Char(c) => format!("'{}'", c.get_inner()),
            Self::TinyText(t) => format!("'{}'", t.get_inner()),
            Self::MediumText(t) => format!("'{}'", t.get_inner()),
            Self::LongText(t) => format!("'{}'", t.get_inner()),
            Self::Binary(b) => format!("{:?}", b.get_inner()),
            Self::TinyBlob(b) => format!("{:?}", b.get_inner()),
            Self::Blob(b) => format!("{:?}", b.get_inner()),
            Self::MediumBlob(b) => format!("{:?}", b.get_inner()),
            Self::LongBlob(b) => format!("{:?}", b.get_inner()),
            Self::Varbinary(b) => format!("{:?}", b.get_inner()),
            Self::Bit(b) => format!("{:?}", b.get_inner()),
            Self::Date(d) => format!("'{}'", d.get_inner()),
            Self::Interval(_) => panic!("Not implemented!"),
            Self::Time(t) => format!("'{}'", t.get_inner()),
            Self::Timestamp(t) => format!("'{}'", t.get_inner()),
        }
    }

    pub fn parse_type(sql_type: SqlTypeMap, value: String) -> Result<Self, DBError> {
        Ok(match sql_type {
            SqlTypeMap::Bool => SqlType::Bool(crate::sql_types::Bool {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::SmallInt => SqlType::SmallInt(crate::sql_types::SmallInt {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Int2 => SqlType::Int2(crate::sql_types::Int2 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::BigInt => SqlType::BigInt(crate::sql_types::BigInt {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Integer => SqlType::Integer(crate::sql_types::Integer {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Int4 => SqlType::Int4(crate::sql_types::Int4 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Int8 => SqlType::Int8(crate::sql_types::Int8 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Float => SqlType::Float(crate::sql_types::Float {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Float4 => SqlType::Float4(crate::sql_types::Float4 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Double => SqlType::Double(crate::sql_types::Double {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Float8 => SqlType::Float8(crate::sql_types::Float8 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Numeric => SqlType::Numeric(crate::sql_types::Numeric {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Decimal => SqlType::Decimal(crate::sql_types::Decimal {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Text => SqlType::Text(crate::sql_types::Text {
                inner: value.clone(),
            }),
            SqlTypeMap::VarChar => SqlType::VarChar(crate::sql_types::VarChar {
                inner: value.clone(),
            }),
            SqlTypeMap::Char => SqlType::Char(crate::sql_types::Char {
                inner: value.clone(),
            }),
            SqlTypeMap::TinyText => SqlType::TinyText(crate::sql_types::TinyText {
                inner: value.clone(),
            }),
            SqlTypeMap::MediumText => SqlType::MediumText(crate::sql_types::MediumText {
                inner: value.clone(),
            }),
            SqlTypeMap::LongText => SqlType::LongText(crate::sql_types::LongText {
                inner: value.clone(),
            }),
            SqlTypeMap::Bit => SqlType::Bit(crate::sql_types::Bit {
                inner: value.as_bytes().to_vec(),
            }),
            SqlTypeMap::Binary => SqlType::Binary(crate::sql_types::Binary {
                inner: value.as_bytes().to_vec(),
            }),
            SqlTypeMap::Blob => SqlType::Blob(crate::sql_types::Blob {
                inner: value.as_bytes().to_vec(),
            }),
            SqlTypeMap::TinyBlob => SqlType::TinyBlob(crate::sql_types::TinyBlob {
                inner: value.as_bytes().to_vec(),
            }),
            SqlTypeMap::MediumBlob => SqlType::MediumBlob(crate::sql_types::MediumBlob {
                inner: value.as_bytes().to_vec(),
            }),
            SqlTypeMap::LongBlob => SqlType::LongBlob(crate::sql_types::LongBlob {
                inner: value.as_bytes().to_vec(),
            }),
            SqlTypeMap::Varbinary => SqlType::Varbinary(crate::sql_types::Varbinary {
                inner: value.as_bytes().to_vec(),
            }),
            SqlTypeMap::Date => SqlType::Date(crate::sql_types::Date {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Time => SqlType::Time(crate::sql_types::Time {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Timestamp => SqlType::Timestamp(crate::sql_types::Timestamp {
                inner: NaiveDateTime::parse_from_str(value.as_str(), "%Y-%m-%d %H:%M:%S")
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            SqlTypeMap::Interval => panic!("Not implemented!"),
        })
    }
}

/// A native enumeration for diesel SQL types
#[derive(Debug, Clone, PartialEq)]
pub enum SqlTypeMap {
    Bool,
    SmallInt,
    Int2,
    Integer,
    Int4,
    BigInt,
    Int8,
    Float,
    Float4,
    Double,
    Float8,
    Numeric,
    Decimal,
    Text,
    VarChar,
    Char,
    TinyText,
    MediumText,
    LongText,
    Binary,
    TinyBlob,
    Blob,
    MediumBlob,
    LongBlob,
    Varbinary,
    Bit,
    Date,
    Interval,
    Time,
    Timestamp,
}

impl TryFrom<&str> for SqlTypeMap {
    type Error = DBError;

    fn try_from(value: &str) -> Result<Self, DBError> {
        let value = value.to_lowercase();
        let sql_type = match value.as_str() {
            "bool" => Self::Bool,
            "smallint" => Self::SmallInt,
            "int2" => Self::Int2,
            "integer" => Self::Integer,
            "int4" => Self::Int4,
            "bigint" => Self::BigInt,
            "int8" => Self::Int8,
            "float" => Self::Float,
            "float4" => Self::Float4,
            "double" => Self::Double,
            "float8" => Self::Float8,
            "numeric" => Self::Numeric,
            "decimal" => Self::Decimal,
            "text" => Self::Text,
            "varchar" => Self::VarChar,
            "char" => Self::Char,
            "tinytext" => Self::TinyText,
            "mediumtext" => Self::MediumText,
            "longtext" => Self::LongText,
            "binary" => Self::Binary,
            "tinyblob" => Self::TinyBlob,
            "blob" => Self::Blob,
            "mediumblob" => Self::MediumBlob,
            "longblob" => Self::LongBlob,
            "varbinary" => Self::Varbinary,
            "bit" => Self::Bit,
            "date" => Self::Date,
            "interval" => Self::Interval,
            "time" => Self::Time,
            "timestamp" => Self::Timestamp,
            _ => return Err(DBError::InvalidFieldType),
        };

        Ok(sql_type)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn sql_type_to_string() {
        let sql_bool = SqlType::Bool(Bool { inner: true });
        assert_eq!(sql_bool.to_string(), "true".to_string());

        let sql_small_int = SqlType::SmallInt(SmallInt { inner: 1_u16 });
        assert_eq!(sql_small_int.to_string(), "1".to_string());

        let sql_int2 = SqlType::Int2(Int2 { inner: 1_u16 });
        assert_eq!(sql_int2.to_string(), "1".to_string());

        let sql_integer = SqlType::Integer(Integer { inner: 1_u32 });
        assert_eq!(sql_integer.to_string(), "1".to_string());

        let sql_int4 = SqlType::Int4(Int4 { inner: 1_u32 });
        assert_eq!(sql_int4.to_string(), "1".to_string());

        let sql_big_int = SqlType::BigInt(BigInt { inner: 1_u64 });
        assert_eq!(sql_big_int.to_string(), "1".to_string());

        let sql_int8 = SqlType::Int8(Int8 { inner: 1_u64 });
        assert_eq!(sql_int8.to_string(), "1".to_string());

        let sql_float = SqlType::Float(Float { inner: 1.2 });
        assert_eq!(sql_float.to_string(), "1.2".to_string());

        let sql_float4 = SqlType::Float4(Float { inner: 1.2 });
        assert_eq!(sql_float4.to_string(), "1.2".to_string());

        let sql_double = SqlType::Double(Double { inner: 1.4 });
        assert_eq!(sql_double.to_string(), "1.4".to_string());

        let sql_float8 = SqlType::Float8(Float8 { inner: 1.4 });
        assert_eq!(sql_float8.to_string(), "1.4".to_string());

        let sql_numeric = SqlType::Numeric(Numeric {
            inner: BigDecimal::from_str("3.1415").unwrap(),
        });
        assert_eq!(sql_numeric.to_string(), "3.1415".to_string());

        let sql_decimal = SqlType::Decimal(Decimal {
            inner: BigDecimal::from_str("3.1415").unwrap(),
        });
        assert_eq!(sql_decimal.to_string(), "3.1415".to_string());

        let sql_text = SqlType::Text(Text {
            inner: "a".to_string(),
        });
        assert_eq!(sql_text.to_string(), "'a'".to_string());

        let sql_varchar = SqlType::VarChar(VarChar {
            inner: "a".to_string(),
        });
        assert_eq!(sql_varchar.to_string(), "'a'".to_string());

        let sql_char = SqlType::Char(Char {
            inner: "a".to_string(),
        });
        assert_eq!(sql_char.to_string(), "'a'".to_string());

        let sql_tiny_text = SqlType::TinyText(TinyText {
            inner: "a".to_string(),
        });
        assert_eq!(sql_tiny_text.to_string(), "'a'".to_string());

        let sql_medium_text = SqlType::MediumText(MediumText {
            inner: "a".to_string(),
        });
        assert_eq!(sql_medium_text.to_string(), "'a'".to_string());

        let sql_long_text = SqlType::LongText(LongText {
            inner: "a".to_string(),
        });
        assert_eq!(sql_long_text.to_string(), "'a'".to_string());

        let sql_binary = SqlType::Binary(Binary {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_binary.to_string(), "[0, 1, 2]".to_string());

        let sql_tiny_blob = SqlType::TinyBlob(TinyBlob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_tiny_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_blob = SqlType::Blob(Blob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_medium_blob = SqlType::MediumBlob(MediumBlob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_medium_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_long_blob = SqlType::LongBlob(LongBlob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_long_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_var_binary = SqlType::Varbinary(Varbinary {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_var_binary.to_string(), "[0, 1, 2]".to_string());

        let sql_bit = SqlType::Bit(Bit {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_bit.to_string(), "[0, 1, 2]".to_string());

        let sql_date = SqlType::Date(Date {
            inner: NaiveDate::from_ymd_opt(2023, 2, 22).unwrap(),
        });
        assert_eq!(sql_date.to_string(), "'2023-02-22'");

        // let sql_interval = SqlType::Interval(Interval {
        //     pub inner: pg_interval::Interval::from_postgres("1 years 1 months 1 days 1 hours").unwrap(),
        // });
        // assert_eq!(
        //     sql_interval.to_string(),
        //     "interval '1 years 1 months 1 days 1 hours'"
        // );

        let sql_time = SqlType::Time(Time {
            inner: NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        });
        assert_eq!(sql_time.to_string(), "'23:59:59'");

        let sql_timestamp = SqlType::Timestamp(Timestamp {
            inner: NaiveDate::from_ymd_opt(2016, 7, 8)
                .unwrap()
                .and_hms_opt(9, 10, 11)
                .unwrap(),
        });
        assert_eq!(sql_timestamp.to_string(), "'2016-07-08 09:10:11'");
    }

    #[test]
    fn test_sql_type_map_from_string() {
        assert_eq!(SqlTypeMap::try_from("bool").unwrap(), SqlTypeMap::Bool);
        assert_eq!(
            SqlTypeMap::try_from("smallint").unwrap(),
            SqlTypeMap::SmallInt
        );
        assert_eq!(SqlTypeMap::try_from("int2").unwrap(), SqlTypeMap::Int2);
        assert_eq!(
            SqlTypeMap::try_from("integer").unwrap(),
            SqlTypeMap::Integer
        );
        assert_eq!(SqlTypeMap::try_from("int4").unwrap(), SqlTypeMap::Int4);
        assert_eq!(SqlTypeMap::try_from("bigint").unwrap(), SqlTypeMap::BigInt);
        assert_eq!(SqlTypeMap::try_from("int8").unwrap(), SqlTypeMap::Int8);
        assert_eq!(SqlTypeMap::try_from("float").unwrap(), SqlTypeMap::Float);
        assert_eq!(SqlTypeMap::try_from("float4").unwrap(), SqlTypeMap::Float4);
        assert_eq!(SqlTypeMap::try_from("double").unwrap(), SqlTypeMap::Double);
        assert_eq!(SqlTypeMap::try_from("float8").unwrap(), SqlTypeMap::Float8);
        assert_eq!(
            SqlTypeMap::try_from("numeric").unwrap(),
            SqlTypeMap::Numeric
        );
        assert_eq!(
            SqlTypeMap::try_from("decimal").unwrap(),
            SqlTypeMap::Decimal
        );
        assert_eq!(SqlTypeMap::try_from("text").unwrap(), SqlTypeMap::Text);
        assert_eq!(
            SqlTypeMap::try_from("varchar").unwrap(),
            SqlTypeMap::VarChar
        );
        assert_eq!(SqlTypeMap::try_from("char").unwrap(), SqlTypeMap::Char);
        assert_eq!(
            SqlTypeMap::try_from("decimal").unwrap(),
            SqlTypeMap::Decimal
        );
        assert_eq!(
            SqlTypeMap::try_from("tinytext").unwrap(),
            SqlTypeMap::TinyText
        );
        assert_eq!(
            SqlTypeMap::try_from("mediumtext").unwrap(),
            SqlTypeMap::MediumText
        );
        assert_eq!(
            SqlTypeMap::try_from("longtext").unwrap(),
            SqlTypeMap::LongText
        );
        assert_eq!(SqlTypeMap::try_from("binary").unwrap(), SqlTypeMap::Binary);
        assert_eq!(
            SqlTypeMap::try_from("tinyblob").unwrap(),
            SqlTypeMap::TinyBlob
        );
        assert_eq!(
            SqlTypeMap::try_from("mediumblob").unwrap(),
            SqlTypeMap::MediumBlob
        );
        assert_eq!(
            SqlTypeMap::try_from("longblob").unwrap(),
            SqlTypeMap::LongBlob
        );
        assert_eq!(
            SqlTypeMap::try_from("varbinary").unwrap(),
            SqlTypeMap::Varbinary
        );
        assert_eq!(SqlTypeMap::try_from("bit").unwrap(), SqlTypeMap::Bit);
        assert_eq!(SqlTypeMap::try_from("date").unwrap(), SqlTypeMap::Date);
        assert_eq!(
            SqlTypeMap::try_from("interval").unwrap(),
            SqlTypeMap::Interval
        );
        assert_eq!(SqlTypeMap::try_from("time").unwrap(), SqlTypeMap::Time);
        assert_eq!(
            SqlTypeMap::try_from("timestamp").unwrap(),
            SqlTypeMap::Timestamp
        );
    }

    #[test]
    fn it_works_parse_sql_types() {
        let x = "10".to_string();
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::SmallInt, x.clone()).unwrap(),
            SqlType::SmallInt(SmallInt { inner: 10 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Int2, x.clone()).unwrap(),
            SqlType::Int2(Int2 { inner: 10 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Int4, x.clone()).unwrap(),
            SqlType::Int4(Int4 { inner: 10 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Int8, x.clone()).unwrap(),
            SqlType::Int8(Int8 { inner: 10 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Integer, x.clone()).unwrap(),
            SqlType::Integer(Integer { inner: 10 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::BigInt, x.clone()).unwrap(),
            SqlType::BigInt(BigInt { inner: 10 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::SmallInt, x.clone()).unwrap(),
            SqlType::SmallInt(SmallInt { inner: 10 })
        );

        let x = "3.3152345".to_string();
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Float, x.clone()).unwrap(),
            SqlType::Float(Float { inner: 3.3152345 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Float4, x.clone()).unwrap(),
            SqlType::Float4(Float4 { inner: 3.3152345 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Float8, x.clone()).unwrap(),
            SqlType::Float8(Float8 { inner: 3.3152345 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Double, x.clone()).unwrap(),
            SqlType::Double(Double { inner: 3.3152345 })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Numeric, x.clone()).unwrap(),
            SqlType::Numeric(Numeric {
                inner: BigDecimal::from_str("3.3152345").unwrap()
            })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Decimal, x.clone()).unwrap(),
            SqlType::Decimal(Decimal {
                inner: BigDecimal::from_str("3.3152345").unwrap()
            })
        );

        let x = "dasdjofdafsd".to_string();
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Text, x.clone()).unwrap(),
            SqlType::Text(Text {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::VarChar, x.clone()).unwrap(),
            SqlType::VarChar(VarChar {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Char, x.clone()).unwrap(),
            SqlType::Char(Char {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::VarChar, x.clone()).unwrap(),
            SqlType::VarChar(VarChar {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::TinyText, x.clone()).unwrap(),
            SqlType::TinyText(TinyText {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::MediumText, x.clone()).unwrap(),
            SqlType::MediumText(MediumText {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::LongText, x.clone()).unwrap(),
            SqlType::LongText(LongText {
                inner: "dasdjofdafsd".to_string()
            })
        );

        let x = "2023-01-01".to_string();
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Date, x).unwrap(),
            SqlType::Date(Date {
                inner: NaiveDate::from_str("2023-01-01").unwrap()
            })
        );

        let x = "23:59:59".to_string();
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Time, x).unwrap(),
            SqlType::Time(Time {
                inner: NaiveTime::from_str("23:59:59").unwrap()
            })
        );

        let x = "2023-01-01 23:59:59".to_string();
        assert_eq!(
            SqlType::parse_type(SqlTypeMap::Timestamp, x).unwrap(),
            SqlType::Timestamp(Timestamp {
                inner: NaiveDateTime::parse_from_str("2023-01-01 23:59:59", "%Y-%m-%d %H:%M:%S")
                    .unwrap()
            })
        );
    }
}
