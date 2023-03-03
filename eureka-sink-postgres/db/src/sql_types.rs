use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use std::convert::TryFrom;

use crate::error::DBError;

/// Trait to express interface to deal with our custom diesel
/// type versions.
pub trait Sql {
    type T;
    type Inner;
    fn get_inner(&self) -> &Self::Inner;
    fn set_inner(inner: Self::Inner) -> Self;
}

macro_rules! sql_type_impl {
    ($typ:ty, $ty:ty, $inner:ty) => {
        impl Sql for $typ {
            type T = $ty;
            type Inner = $inner;
            fn get_inner(&self) -> &Self::Inner {
                &self.inner
            }
            fn set_inner(inner: Self::Inner) -> Self {
                Self { inner }
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Bool`] instance.
pub struct Bool {
    inner: bool,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`SmallInt`] instance.
pub struct SmallInt {
    inner: i16,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Integer`] instance.
pub struct Integer {
    inner: i32,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`BigInt`] instance.
pub struct BigInt {
    inner: i64,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Float`] instance.
pub struct Float {
    inner: f32,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Double`] instance.
pub struct Double {
    inner: f64,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Numeric`] instance.
pub struct Numeric {
    inner: BigDecimal,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Text`] instance.
pub struct Text {
    inner: String,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Binary`] instance.
pub struct Binary {
    inner: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Date`] instance.
pub struct Date {
    inner: NaiveDate,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Timestamp`] instance.
pub struct Timestamp {
    inner: NaiveDateTime,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Time`] instance.
pub struct Time {
    inner: NaiveTime,
}

#[derive(Debug, Clone, PartialEq)]
/// A diesel compatible [`Interval`] instance.
pub struct Interval {
    inner: pg_interval::Interval,
}

sql_type_impl!(Bool, diesel::sql_types::Bool, bool);
sql_type_impl!(SmallInt, diesel::sql_types::SmallInt, i16);
sql_type_impl!(Integer, diesel::sql_types::Integer, i32);
sql_type_impl!(BigInt, diesel::sql_types::BigInt, i64);
sql_type_impl!(Float, diesel::sql_types::Float, f32);
sql_type_impl!(Double, diesel::sql_types::Double, f64);
sql_type_impl!(Numeric, diesel::sql_types::Numeric, BigDecimal);
sql_type_impl!(Text, diesel::sql_types::Text, String);
sql_type_impl!(Binary, diesel::sql_types::Binary, Vec<u8>);
sql_type_impl!(Date, diesel::sql_types::Date, NaiveDate);
sql_type_impl!(Timestamp, diesel::sql_types::Timestamp, NaiveDateTime);
sql_type_impl!(Time, diesel::sql_types::Time, NaiveTime);
sql_type_impl!(Interval, diesel::sql_types::Interval, pg_interval::Interval);

/// A diesel compatible [`Int2`] instance.
pub type Int2 = SmallInt;

/// A diesel compatible [`Int4`] instance.
pub type Int4 = Integer;

/// A diesel compatible [`Int8`] instance.
pub type Int8 = BigInt;

/// A diesel compatible [`Float4`] instance.
pub type Float4 = Float;

/// A diesel compatible [`Float8`] instance.
pub type Float8 = Double;

/// A diesel compatible [`Decimal`] instance.
pub type Decimal = Numeric;

/// A diesel compatible [`VarChar`] instance.
pub type VarChar = Text;

/// A diesel compatible [`Char`] instance.
pub type Char = Text;

/// A diesel compatible [`TinyText`] instance.
pub type TinyText = Text;

/// A diesel compatible [`MediumText`] instance.
pub type MediumText = Text;

/// A diesel compatible [`LongText`] instance.
pub type LongText = Text;

/// A diesel compatible [`TinyBlob`] instance.
pub type TinyBlob = Binary;

/// A diesel compatible [`Blob`] instance.
pub type Blob = Binary;

/// A diesel compatible [`MediumBlob`] instance.
pub type MediumBlob = Binary;

/// A diesel compatible [`LongBlob`] instance.
pub type LongBlob = Binary;

/// A diesel compatible [`Varbinary`] instance.
pub type Varbinary = Binary;

/// A diesel compatible [`Bit`] instance.
pub type Bit = Binary;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
/// [`ColumnValue`] encapsulates a list of our custom diesel
/// compatible types.
pub enum ColumnValue {
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

impl ColumnValue {
    #[allow(dead_code)]
    /// Given an instance of [`ColumnValue`] it produces
    /// a string with its value, ready to be formatted
    /// in a SQL query.
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

    /// Given a [`ColumnType`] and a value of type [`String`], it tries to parse
    /// the correct value as a [`ColumnValue`] instance.
    pub fn parse_type(sql_type: ColumnType, value: String) -> Result<Self, DBError> {
        Ok(match sql_type {
            ColumnType::Bool => ColumnValue::Bool(crate::sql_types::Bool {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::SmallInt => ColumnValue::SmallInt(crate::sql_types::SmallInt {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Int2 => ColumnValue::Int2(crate::sql_types::Int2 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::BigInt => ColumnValue::BigInt(crate::sql_types::BigInt {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Integer => ColumnValue::Integer(crate::sql_types::Integer {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Int4 => ColumnValue::Int4(crate::sql_types::Int4 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Int8 => ColumnValue::Int8(crate::sql_types::Int8 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Float => ColumnValue::Float(crate::sql_types::Float {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Float4 => ColumnValue::Float4(crate::sql_types::Float4 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Double => ColumnValue::Double(crate::sql_types::Double {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Float8 => ColumnValue::Float8(crate::sql_types::Float8 {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Numeric => ColumnValue::Numeric(crate::sql_types::Numeric {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Decimal => ColumnValue::Decimal(crate::sql_types::Decimal {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Text => ColumnValue::Text(crate::sql_types::Text {
                inner: value.clone(),
            }),
            ColumnType::VarChar => ColumnValue::VarChar(crate::sql_types::VarChar {
                inner: value.clone(),
            }),
            ColumnType::Char => ColumnValue::Char(crate::sql_types::Char {
                inner: value.clone(),
            }),
            ColumnType::TinyText => ColumnValue::TinyText(crate::sql_types::TinyText {
                inner: value.clone(),
            }),
            ColumnType::MediumText => ColumnValue::MediumText(crate::sql_types::MediumText {
                inner: value.clone(),
            }),
            ColumnType::LongText => ColumnValue::LongText(crate::sql_types::LongText {
                inner: value.clone(),
            }),
            ColumnType::Bit => ColumnValue::Bit(crate::sql_types::Bit {
                inner: value.as_bytes().to_vec(),
            }),
            ColumnType::Binary => ColumnValue::Binary(crate::sql_types::Binary {
                inner: value.as_bytes().to_vec(),
            }),
            ColumnType::Blob => ColumnValue::Blob(crate::sql_types::Blob {
                inner: value.as_bytes().to_vec(),
            }),
            ColumnType::TinyBlob => ColumnValue::TinyBlob(crate::sql_types::TinyBlob {
                inner: value.as_bytes().to_vec(),
            }),
            ColumnType::MediumBlob => ColumnValue::MediumBlob(crate::sql_types::MediumBlob {
                inner: value.as_bytes().to_vec(),
            }),
            ColumnType::LongBlob => ColumnValue::LongBlob(crate::sql_types::LongBlob {
                inner: value.as_bytes().to_vec(),
            }),
            ColumnType::Varbinary => ColumnValue::Varbinary(crate::sql_types::Varbinary {
                inner: value.as_bytes().to_vec(),
            }),
            ColumnType::Date => ColumnValue::Date(crate::sql_types::Date {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Time => ColumnValue::Time(crate::sql_types::Time {
                inner: value
                    .parse()
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Timestamp => ColumnValue::Timestamp(crate::sql_types::Timestamp {
                inner: NaiveDateTime::parse_from_str(value.as_str(), "%Y-%m-%d %H:%M:%S")
                    .map_err(|_| DBError::FailedParseString(value.clone()))?,
            }),
            ColumnType::Interval => panic!("Not implemented!"),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
/// A custom enumeration for diesel SQL types
pub enum ColumnType {
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

impl TryFrom<&str> for ColumnType {
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
        let sql_bool = ColumnValue::Bool(Bool { inner: true });
        assert_eq!(sql_bool.to_string(), "true".to_string());

        let sql_small_int = ColumnValue::SmallInt(SmallInt { inner: 1_i16 });
        assert_eq!(sql_small_int.to_string(), "1".to_string());

        let sql_int2 = ColumnValue::Int2(Int2 { inner: 1_i16 });
        assert_eq!(sql_int2.to_string(), "1".to_string());

        let sql_integer = ColumnValue::Integer(Integer { inner: 1_i32 });
        assert_eq!(sql_integer.to_string(), "1".to_string());

        let sql_int4 = ColumnValue::Int4(Int4 { inner: 1_i32 });
        assert_eq!(sql_int4.to_string(), "1".to_string());

        let sql_big_int = ColumnValue::BigInt(BigInt { inner: 1_i64 });
        assert_eq!(sql_big_int.to_string(), "1".to_string());

        let sql_int8 = ColumnValue::Int8(Int8 { inner: 1_i64 });
        assert_eq!(sql_int8.to_string(), "1".to_string());

        let sql_float = ColumnValue::Float(Float { inner: 1.2 });
        assert_eq!(sql_float.to_string(), "1.2".to_string());

        let sql_float4 = ColumnValue::Float4(Float { inner: 1.2 });
        assert_eq!(sql_float4.to_string(), "1.2".to_string());

        let sql_double = ColumnValue::Double(Double { inner: 1.4 });
        assert_eq!(sql_double.to_string(), "1.4".to_string());

        let sql_float8 = ColumnValue::Float8(Float8 { inner: 1.4 });
        assert_eq!(sql_float8.to_string(), "1.4".to_string());

        let sql_numeric = ColumnValue::Numeric(Numeric {
            inner: BigDecimal::from_str("3.1415").unwrap(),
        });
        assert_eq!(sql_numeric.to_string(), "3.1415".to_string());

        let sql_decimal = ColumnValue::Decimal(Decimal {
            inner: BigDecimal::from_str("3.1415").unwrap(),
        });
        assert_eq!(sql_decimal.to_string(), "3.1415".to_string());

        let sql_text = ColumnValue::Text(Text {
            inner: "a".to_string(),
        });
        assert_eq!(sql_text.to_string(), "'a'".to_string());

        let sql_varchar = ColumnValue::VarChar(VarChar {
            inner: "a".to_string(),
        });
        assert_eq!(sql_varchar.to_string(), "'a'".to_string());

        let sql_char = ColumnValue::Char(Char {
            inner: "a".to_string(),
        });
        assert_eq!(sql_char.to_string(), "'a'".to_string());

        let sql_tiny_text = ColumnValue::TinyText(TinyText {
            inner: "a".to_string(),
        });
        assert_eq!(sql_tiny_text.to_string(), "'a'".to_string());

        let sql_medium_text = ColumnValue::MediumText(MediumText {
            inner: "a".to_string(),
        });
        assert_eq!(sql_medium_text.to_string(), "'a'".to_string());

        let sql_long_text = ColumnValue::LongText(LongText {
            inner: "a".to_string(),
        });
        assert_eq!(sql_long_text.to_string(), "'a'".to_string());

        let sql_binary = ColumnValue::Binary(Binary {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_binary.to_string(), "[0, 1, 2]".to_string());

        let sql_tiny_blob = ColumnValue::TinyBlob(TinyBlob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_tiny_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_blob = ColumnValue::Blob(Blob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_medium_blob = ColumnValue::MediumBlob(MediumBlob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_medium_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_long_blob = ColumnValue::LongBlob(LongBlob {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_long_blob.to_string(), "[0, 1, 2]".to_string());

        let sql_var_binary = ColumnValue::Varbinary(Varbinary {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_var_binary.to_string(), "[0, 1, 2]".to_string());

        let sql_bit = ColumnValue::Bit(Bit {
            inner: vec![0u8, 1, 2],
        });
        assert_eq!(sql_bit.to_string(), "[0, 1, 2]".to_string());

        let sql_date = ColumnValue::Date(Date {
            inner: NaiveDate::from_ymd_opt(2023, 2, 22).unwrap(),
        });
        assert_eq!(sql_date.to_string(), "'2023-02-22'");

        // let sql_interval = ColumnValue::Interval(Interval {
        //     pub inner: pg_interval::Interval::from_postgres("1 years 1 months 1 days 1 hours").unwrap(),
        // });
        // assert_eq!(
        //     sql_interval.to_string(),
        //     "interval '1 years 1 months 1 days 1 hours'"
        // );

        let sql_time = ColumnValue::Time(Time {
            inner: NaiveTime::from_hms_opt(23, 59, 59).unwrap(),
        });
        assert_eq!(sql_time.to_string(), "'23:59:59'");

        let sql_timestamp = ColumnValue::Timestamp(Timestamp {
            inner: NaiveDate::from_ymd_opt(2016, 7, 8)
                .unwrap()
                .and_hms_opt(9, 10, 11)
                .unwrap(),
        });
        assert_eq!(sql_timestamp.to_string(), "'2016-07-08 09:10:11'");
    }

    #[test]
    fn test_sql_type_map_from_string() {
        assert_eq!(ColumnType::try_from("bool").unwrap(), ColumnType::Bool);
        assert_eq!(
            ColumnType::try_from("smallint").unwrap(),
            ColumnType::SmallInt
        );
        assert_eq!(ColumnType::try_from("int2").unwrap(), ColumnType::Int2);
        assert_eq!(
            ColumnType::try_from("integer").unwrap(),
            ColumnType::Integer
        );
        assert_eq!(ColumnType::try_from("int4").unwrap(), ColumnType::Int4);
        assert_eq!(ColumnType::try_from("bigint").unwrap(), ColumnType::BigInt);
        assert_eq!(ColumnType::try_from("int8").unwrap(), ColumnType::Int8);
        assert_eq!(ColumnType::try_from("float").unwrap(), ColumnType::Float);
        assert_eq!(ColumnType::try_from("float4").unwrap(), ColumnType::Float4);
        assert_eq!(ColumnType::try_from("double").unwrap(), ColumnType::Double);
        assert_eq!(ColumnType::try_from("float8").unwrap(), ColumnType::Float8);
        assert_eq!(
            ColumnType::try_from("numeric").unwrap(),
            ColumnType::Numeric
        );
        assert_eq!(
            ColumnType::try_from("decimal").unwrap(),
            ColumnType::Decimal
        );
        assert_eq!(ColumnType::try_from("text").unwrap(), ColumnType::Text);
        assert_eq!(
            ColumnType::try_from("varchar").unwrap(),
            ColumnType::VarChar
        );
        assert_eq!(ColumnType::try_from("char").unwrap(), ColumnType::Char);
        assert_eq!(
            ColumnType::try_from("decimal").unwrap(),
            ColumnType::Decimal
        );
        assert_eq!(
            ColumnType::try_from("tinytext").unwrap(),
            ColumnType::TinyText
        );
        assert_eq!(
            ColumnType::try_from("mediumtext").unwrap(),
            ColumnType::MediumText
        );
        assert_eq!(
            ColumnType::try_from("longtext").unwrap(),
            ColumnType::LongText
        );
        assert_eq!(ColumnType::try_from("binary").unwrap(), ColumnType::Binary);
        assert_eq!(
            ColumnType::try_from("tinyblob").unwrap(),
            ColumnType::TinyBlob
        );
        assert_eq!(
            ColumnType::try_from("mediumblob").unwrap(),
            ColumnType::MediumBlob
        );
        assert_eq!(
            ColumnType::try_from("longblob").unwrap(),
            ColumnType::LongBlob
        );
        assert_eq!(
            ColumnType::try_from("varbinary").unwrap(),
            ColumnType::Varbinary
        );
        assert_eq!(ColumnType::try_from("bit").unwrap(), ColumnType::Bit);
        assert_eq!(ColumnType::try_from("date").unwrap(), ColumnType::Date);
        assert_eq!(
            ColumnType::try_from("interval").unwrap(),
            ColumnType::Interval
        );
        assert_eq!(ColumnType::try_from("time").unwrap(), ColumnType::Time);
        assert_eq!(
            ColumnType::try_from("timestamp").unwrap(),
            ColumnType::Timestamp
        );
    }

    #[test]
    fn it_works_parse_sql_types() {
        let x = "10".to_string();
        assert_eq!(
            ColumnValue::parse_type(ColumnType::SmallInt, x.clone()).unwrap(),
            ColumnValue::SmallInt(SmallInt { inner: 10 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Int2, x.clone()).unwrap(),
            ColumnValue::Int2(Int2 { inner: 10 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Int4, x.clone()).unwrap(),
            ColumnValue::Int4(Int4 { inner: 10 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Int8, x.clone()).unwrap(),
            ColumnValue::Int8(Int8 { inner: 10 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Integer, x.clone()).unwrap(),
            ColumnValue::Integer(Integer { inner: 10 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::BigInt, x.clone()).unwrap(),
            ColumnValue::BigInt(BigInt { inner: 10 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::SmallInt, x.clone()).unwrap(),
            ColumnValue::SmallInt(SmallInt { inner: 10 })
        );

        let x = "3.3152345".to_string();
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Float, x.clone()).unwrap(),
            ColumnValue::Float(Float { inner: 3.3152345 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Float4, x.clone()).unwrap(),
            ColumnValue::Float4(Float4 { inner: 3.3152345 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Float8, x.clone()).unwrap(),
            ColumnValue::Float8(Float8 { inner: 3.3152345 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Double, x.clone()).unwrap(),
            ColumnValue::Double(Double { inner: 3.3152345 })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Numeric, x.clone()).unwrap(),
            ColumnValue::Numeric(Numeric {
                inner: BigDecimal::from_str("3.3152345").unwrap()
            })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Decimal, x.clone()).unwrap(),
            ColumnValue::Decimal(Decimal {
                inner: BigDecimal::from_str("3.3152345").unwrap()
            })
        );

        let x = "dasdjofdafsd".to_string();
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Text, x.clone()).unwrap(),
            ColumnValue::Text(Text {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::VarChar, x.clone()).unwrap(),
            ColumnValue::VarChar(VarChar {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Char, x.clone()).unwrap(),
            ColumnValue::Char(Char {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::VarChar, x.clone()).unwrap(),
            ColumnValue::VarChar(VarChar {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::TinyText, x.clone()).unwrap(),
            ColumnValue::TinyText(TinyText {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::MediumText, x.clone()).unwrap(),
            ColumnValue::MediumText(MediumText {
                inner: "dasdjofdafsd".to_string()
            })
        );
        assert_eq!(
            ColumnValue::parse_type(ColumnType::LongText, x.clone()).unwrap(),
            ColumnValue::LongText(LongText {
                inner: "dasdjofdafsd".to_string()
            })
        );

        let x = "2023-01-01".to_string();
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Date, x).unwrap(),
            ColumnValue::Date(Date {
                inner: NaiveDate::from_str("2023-01-01").unwrap()
            })
        );

        let x = "23:59:59".to_string();
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Time, x).unwrap(),
            ColumnValue::Time(Time {
                inner: NaiveTime::from_str("23:59:59").unwrap()
            })
        );

        let x = "2023-01-01 23:59:59".to_string();
        assert_eq!(
            ColumnValue::parse_type(ColumnType::Timestamp, x).unwrap(),
            ColumnValue::Timestamp(Timestamp {
                inner: NaiveDateTime::parse_from_str("2023-01-01 23:59:59", "%Y-%m-%d %H:%M:%S")
                    .unwrap()
            })
        );
    }
}
