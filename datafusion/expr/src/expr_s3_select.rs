use crate::Expr;
use arrow::datatypes::{DataType, Field};
use chrono::{SecondsFormat, TimeZone, Utc};
use datafusion_common::scalar::ScalarValueGroup;
use datafusion_common::{DataFusionError, ScalarValue};

use crate::Operator;
use chrono_tz::Tz;
use std::fmt::Write;

pub fn map_to_s3_select_data_type(
    data_type: &DataType,
) -> datafusion_common::Result<String> {
    match data_type {
        DataType::Null => Ok("NULL".into()),
        DataType::Boolean => Ok("BOOL".into()),
        DataType::Int8 => Ok("INT".into()),
        DataType::Int16 => Ok("INT".into()),
        DataType::Int32 => Ok("INT".into()),
        DataType::Int64 => Ok("INT".into()),
        DataType::UInt8 => Ok("INT".into()),
        DataType::UInt16 => Ok("INT".into()),
        DataType::UInt32 => Ok("INT".into()),
        DataType::UInt64 => Ok("DECIMAL".into()),
        DataType::Float16 => Ok("FLOAT".into()),
        DataType::Float32 => Ok("FLOAT".into()),
        DataType::Float64 => Ok("FLOAT".into()),
        DataType::Timestamp(_, _) => Ok("TIMESTAMP".into()),
        DataType::Date32 => Ok("TIMESTAMP".into()),
        DataType::Date64 => Ok("TIMESTAMP".into()),
        DataType::Utf8 => Ok("STRING".into()),
        DataType::LargeUtf8 => Ok("STRING".into()),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Not supported DataType: {}",
            data_type
        ))),
    }
}

pub fn to_s3_select_literal(
    scalar_value: &ScalarValue,
) -> datafusion_common::Result<String> {
    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-glacier-select-sql-reference-data-types.html
    match scalar_value.get_scalar_value_group() {
        ScalarValueGroup::Null => Ok(scalar_value.to_string()),
        ScalarValueGroup::Boolean => Ok(scalar_value.to_string().to_uppercase()),
        ScalarValueGroup::Float => match scalar_value {
            // FIXME: check if it works fine
            // `CAST` a value to `FLOAT` because S3 Select tries to treat it as Decimal.
            ScalarValue::Float32(Some(v)) => Ok(format!("CAST({} AS FLOAT)", v)),
            ScalarValue::Float64(Some(v)) => Ok(format!("CAST({} AS FLOAT)", v)),
            _ => Ok(scalar_value.to_string()),
        },
        ScalarValueGroup::Int => Ok(scalar_value.to_string()),
        ScalarValueGroup::UInt => Ok(scalar_value.to_string()),
        ScalarValueGroup::Utf8 => match scalar_value {
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                Ok(format!("'{}'", v.replace('\'', "''")))
            }
            _ => Ok(scalar_value.to_string()),
        },
        ScalarValueGroup::Date | ScalarValueGroup::Timestamp => {
            let ts = match scalar_value {
                ScalarValue::Date32(Some(i)) => {
                    let secs = 60 * 60 * 24 * (i.to_owned() as i64);
                    Some(
                        Utc.timestamp(secs, 0)
                            .to_rfc3339_opts(SecondsFormat::Secs, true),
                    )
                }
                ScalarValue::Date64(Some(i)) => {
                    let secs = i / 1000;
                    let nsecs = ((i % 1000) * 1000 * 1000) as u32;
                    Some(
                        Utc.timestamp(secs, nsecs)
                            .to_rfc3339_opts(SecondsFormat::Millis, true),
                    )
                }
                ScalarValue::TimestampSecond(Some(i), tz_opt) => {
                    let secs = i.to_owned();
                    let nsecs = 0u32;
                    let tz: Tz = tz_opt
                        .to_owned()
                        .unwrap_or_else(|| "UTC".to_owned())
                        .parse()
                        .map_err(|_| DataFusionError::Internal("".into()))?;
                    Some(
                        Utc.timestamp(secs, nsecs)
                            .with_timezone(&tz)
                            .to_rfc3339_opts(SecondsFormat::Secs, true),
                    )
                }
                ScalarValue::TimestampMillisecond(Some(i), tz_opt) => {
                    let div = 1000;
                    let secs = i / div;
                    let nsecs = ((i % div) * 1000 * 1000) as u32;
                    let tz: Tz = tz_opt
                        .to_owned()
                        .unwrap_or_else(|| "UTC".to_owned())
                        .parse()
                        .map_err(|_| DataFusionError::Internal("".into()))?;
                    Some(
                        Utc.timestamp(secs, nsecs)
                            .with_timezone(&tz)
                            .to_rfc3339_opts(SecondsFormat::Millis, true),
                    )
                }
                ScalarValue::TimestampMicrosecond(Some(i), tz_opt) => {
                    let div = 1000 * 1000;
                    let secs = i / div;
                    let nsecs = ((i % div) * 1000) as u32;
                    let tz: Tz = tz_opt
                        .to_owned()
                        .unwrap_or_else(|| "UTC".to_owned())
                        .parse()
                        .map_err(|_| DataFusionError::Internal("".into()))?;
                    Some(
                        Utc.timestamp(secs, nsecs)
                            .with_timezone(&tz)
                            .to_rfc3339_opts(SecondsFormat::Micros, true),
                    )
                }
                ScalarValue::TimestampNanosecond(Some(i), tz_opt) => {
                    let div = 1000 * 1000 * 1000;
                    let secs = i / div;
                    let nsecs = (i % div) as u32;
                    let tz: Tz = tz_opt
                        .to_owned()
                        .unwrap_or_else(|| "UTC".to_owned())
                        .parse()
                        .map_err(|_| DataFusionError::Internal("".into()))?;
                    Some(
                        Utc.timestamp(secs, nsecs)
                            .with_timezone(&tz)
                            .to_rfc3339_opts(SecondsFormat::Nanos, true),
                    )
                }
                _ => None,
            };

            match ts {
                Some(v) => Ok(format!("CAST('{}' AS TIMESTAMP)", v)),
                None => Ok(scalar_value.to_string()),
            }
        }
        _ => Err(DataFusionError::NotImplemented("".into())),
    }
}

pub fn generate_s3_select_where_condition(
    expr: Option<&Expr>,
    fields: &Vec<Field>,
    anonymous_field_name: bool,
) -> String {
    match expr {
        Some(expr) => format!(
            "WHERE {}",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name,)
                .unwrap_or_else(|_| "TRUE".to_string())
        ),
        None => "".to_string(),
    }
}

fn _generate_s3_select_where_condition(
    expr: &Expr,
    fields: &Vec<Field>,
    anonymous_field_name: bool,
) -> datafusion_common::Result<String> {
    match expr {
        Expr::Column(c) => {
            let field = fields.iter().find(|x| *x.name() == c.name).ok_or_else(|| {
                DataFusionError::Execution(format!("Unknown field name: {}", c.name))
            })?;
            let data_type = map_to_s3_select_data_type(field.data_type())?;

            // rename anonymous column `_column1` to `_1`
            let name = match anonymous_field_name {
                true => c.name.replace("column", ""),
                false => c.name.to_owned(),
            }
            .replace('\"', "\"\"");

            Ok(format!("CAST(s.\"{}\" AS {})", name, data_type))
        }
        Expr::Literal(v) => to_s3_select_literal(v),
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
            ..
        } => {
            let mut name = "CASE ".to_string();
            if let Some(e) = expr {
                let e =
                    _generate_s3_select_where_condition(e, fields, anonymous_field_name)?;
                let _ = write!(name, "{} ", e);
            }
            for (w, t) in when_then_expr {
                let when =
                    _generate_s3_select_where_condition(w, fields, anonymous_field_name)?;
                let then =
                    _generate_s3_select_where_condition(t, fields, anonymous_field_name)?;
                let _ = write!(name, "WHEN {} THEN {} ", when, then);
            }
            if let Some(e) = else_expr {
                let e =
                    _generate_s3_select_where_condition(e, fields, anonymous_field_name)?;
                let _ = write!(name, "ELSE {} ", e);
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast { expr, data_type } => Ok(format!(
            "CAST({} AS {})",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?,
            map_to_s3_select_data_type(data_type)?
        )),
        Expr::Not(expr) => Ok(format!(
            "NOT {}",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::Negative(expr) => Ok(format!(
            "(- {})",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsNull(expr) => Ok(format!(
            "{} IS NULL",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsNotNull(expr) => Ok(format!(
            "{} IS NOT NULL",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsUnknown(expr) => Ok(format!(
            "{} IS NULL",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsNotUnknown(expr) => Ok(format!(
            "{} IS NOT NULL",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsTrue(expr) => Ok(format!(
            "{} IS TRUE",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsFalse(expr) => Ok(format!(
            "{} IS FALSE",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsNotTrue(expr) => Ok(format!(
            "{} IS NOT TRUE",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::IsNotFalse(expr) => Ok(format!(
            "{} IS NOT FALSE",
            _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?
        )),
        Expr::BinaryExpr { left, op, right } => {
            let is_boolean_op = matches!(op, Operator::And | Operator::Or);

            let left =
                _generate_s3_select_where_condition(left, fields, anonymous_field_name)
                    .or_else(|e| match is_boolean_op {
                    true => Ok("TRUE".to_string()),
                    false => Err(e),
                })?;
            let right =
                _generate_s3_select_where_condition(right, fields, anonymous_field_name)
                    .or_else(|e| match is_boolean_op {
                        true => Ok("TRUE".to_string()),
                        false => Err(e),
                    })?;

            Ok(format!("{} {} {}", left, op, right))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            if *negated {
                Ok(format!(
                    "{} NOT BETWEEN {} AND {}",
                    _generate_s3_select_where_condition(
                        expr,
                        fields,
                        anonymous_field_name
                    )?,
                    _generate_s3_select_where_condition(
                        low,
                        fields,
                        anonymous_field_name
                    )?,
                    _generate_s3_select_where_condition(
                        high,
                        fields,
                        anonymous_field_name
                    )?
                ))
            } else {
                Ok(format!(
                    "{} BETWEEN {} AND {}",
                    _generate_s3_select_where_condition(
                        expr,
                        fields,
                        anonymous_field_name
                    )?,
                    _generate_s3_select_where_condition(
                        low,
                        fields,
                        anonymous_field_name
                    )?,
                    _generate_s3_select_where_condition(
                        high,
                        fields,
                        anonymous_field_name
                    )?
                ))
            }
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
        } => {
            let s = format!(
                "{} {} {} {}",
                _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?,
                if *negated { "NOT LIKE" } else { "LIKE" },
                _generate_s3_select_where_condition(
                    pattern,
                    fields,
                    anonymous_field_name
                )?,
                if let Some(char) = escape_char {
                    format!("ESCAPE '{}'", char)
                } else {
                    "".to_string()
                }
            );
            Ok(s)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr =
                _generate_s3_select_where_condition(expr, fields, anonymous_field_name)?;
            let list = list
                .iter()
                .map(|x| {
                    _generate_s3_select_where_condition(x, fields, anonymous_field_name)
                })
                .collect::<datafusion_common::Result<Vec<String>>>()?
                .join(", ");

            match *negated {
                true => Ok(format!("{} NOT IN ({})", expr, list)),
                false => Ok(format!("{} IN ({})", expr, list)),
            }
        }
        e => Err(DataFusionError::NotImplemented(format!(
            "Not supported operation for S3 Select: {:?}",
            e
        ))),
    }
}

#[cfg(test)]
mod test {

    use crate::expr_s3_select::{
        generate_s3_select_where_condition, to_s3_select_literal,
    };
    use crate::{BuiltinScalarFunction, Expr, Operator};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{Column, ScalarValue};

    #[test]
    fn scalar_value_rewrite() {
        assert_eq!(to_s3_select_literal(&ScalarValue::Null).unwrap(), "NULL");

        assert_eq!(
            to_s3_select_literal(&ScalarValue::Boolean(Some(true))).unwrap(),
            "TRUE"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Boolean(Some(false))).unwrap(),
            "FALSE"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Boolean(None)).unwrap(),
            "NULL"
        );

        assert_eq!(
            to_s3_select_literal(&ScalarValue::Float32(Some(1.1))).unwrap(),
            "CAST(1.1 AS FLOAT)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Float32(Some(-1e-10))).unwrap(),
            "CAST(-0.0000000001 AS FLOAT)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Float32(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Float64(Some(1.1))).unwrap(),
            "CAST(1.1 AS FLOAT)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Float64(Some(-1e-10))).unwrap(),
            "CAST(-0.0000000001 AS FLOAT)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Float64(None)).unwrap(),
            "NULL"
        );

        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int8(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int8(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int16(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int16(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int32(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int32(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int64(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Int64(None)).unwrap(),
            "NULL"
        );

        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt8(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt8(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt16(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt16(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt32(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt32(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt64(Some(1))).unwrap(),
            "1"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::UInt64(None)).unwrap(),
            "NULL"
        );

        assert_eq!(
            to_s3_select_literal(&ScalarValue::Utf8(Some("it's me!".into()))).unwrap(),
            "'it''s me!'"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Utf8(None)).unwrap(),
            "NULL"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::LargeUtf8(Some("it's me!".into())))
                .unwrap(),
            "'it''s me!'"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::LargeUtf8(None)).unwrap(),
            "NULL"
        );

        // Date32: Days since UNIX epoch
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Date32(Some(19274))).unwrap(),
            "CAST('2022-10-09T00:00:00Z' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Date32(None)).unwrap(),
            "NULL"
        );
        // Date32: Milliseconds since UNIX epoch
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Date64(Some(1665293233492))).unwrap(),
            "CAST('2022-10-09T05:27:13.492Z' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::Date64(None)).unwrap(),
            "NULL"
        );
        // TimestampSecond: Seconds since UNIX epoch
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampSecond(Some(1665293233), None))
                .unwrap(),
            "CAST('2022-10-09T05:27:13Z' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampSecond(
                Some(1665293233),
                Some("Asia/Tokyo".into())
            ))
            .unwrap(),
            "CAST('2022-10-09T14:27:13+09:00' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampSecond(None, None)).unwrap(),
            "NULL"
        );
        // TimestampMillisecond: Milliseconds since UNIX epoch
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampMillisecond(
                Some(1665293233492),
                None
            ))
            .unwrap(),
            "CAST('2022-10-09T05:27:13.492Z' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampMillisecond(
                Some(1665293233492),
                Some("Asia/Tokyo".into())
            ))
            .unwrap(),
            "CAST('2022-10-09T14:27:13.492+09:00' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampMillisecond(None, None)).unwrap(),
            "NULL"
        );
        // TimestampMicrosecond: Microseconds since UNIX epoch
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampMicrosecond(
                Some(1665293233492123),
                None
            ))
            .unwrap(),
            "CAST('2022-10-09T05:27:13.492123Z' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampMicrosecond(
                Some(1665293233492123),
                Some("Asia/Tokyo".into())
            ))
            .unwrap(),
            "CAST('2022-10-09T14:27:13.492123+09:00' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampMicrosecond(None, None)).unwrap(),
            "NULL"
        );
        // TimestampNanosecond: Nanoseconds since UNIX epoch
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampNanosecond(
                Some(1665293233492123456),
                None
            ))
            .unwrap(),
            "CAST('2022-10-09T05:27:13.492123456Z' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampNanosecond(
                Some(1665293233492123456),
                Some("Asia/Tokyo".into())
            ))
            .unwrap(),
            "CAST('2022-10-09T14:27:13.492123456+09:00' AS TIMESTAMP)"
        );
        assert_eq!(
            to_s3_select_literal(&ScalarValue::TimestampNanosecond(None, None)).unwrap(),
            "NULL"
        );
    }

    #[test]
    fn where_condition() {
        let fields = vec![
            Field::new("int", DataType::Int64, true),
            Field::new("str", DataType::Utf8, true),
            Field::new("bool", DataType::Boolean, true),
            Field::new(
                "unsupported",
                DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ];

        // Nothing
        assert_eq!(generate_s3_select_where_condition(None, &fields, false), "");

        // Expr::Column
        let expr = Expr::Column(Column {
            relation: None,
            name: "bool".into(),
        });
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"bool\" AS BOOL)"
        );

        // Expr::Literal
        let expr = Expr::Literal(ScalarValue::Boolean(Some(false)));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE FALSE"
        );

        // Expr::Case
        let expr = Expr::Case {
            expr: Some(Box::new(Expr::Literal(ScalarValue::Int64(Some(10))))),
            when_then_expr: vec![(
                Box::new(Expr::Literal(ScalarValue::Int64(Some(11)))),
                Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)))),
            )],
            else_expr: Some(Box::new(Expr::Literal(ScalarValue::Boolean(Some(true))))),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CASE 10 WHEN 11 THEN FALSE ELSE TRUE END"
        );

        // Expr::Cast
        // Expr::BinaryExpr
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Cast {
                expr: Box::new(Expr::Column(Column {
                    relation: None,
                    name: "str".into(),
                })),
                data_type: DataType::Int64,
            }),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(CAST(s.\"str\" AS STRING) AS INT) = 1"
        );

        // Expr::Not
        let expr = Expr::Not(Box::new(Expr::Column(Column {
            relation: None,
            name: "bool".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE NOT CAST(s.\"bool\" AS BOOL)"
        );

        // Expr::Negative
        // Expr::BinaryExpr
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Negative(Box::new(Expr::Literal(ScalarValue::Int64(
                Some(1),
            ))))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE (- 1) = 1"
        );

        // Expr::IsNull
        let expr = Expr::IsNull(Box::new(Expr::Column(Column {
            relation: None,
            name: "int".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"int\" AS INT) IS NULL"
        );

        // Expr::IsNotNull
        let expr = Expr::IsNotNull(Box::new(Expr::Column(Column {
            relation: None,
            name: "int".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"int\" AS INT) IS NOT NULL"
        );

        // Expr::IsUnknown
        let expr = Expr::IsUnknown(Box::new(Expr::Column(Column {
            relation: None,
            name: "int".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"int\" AS INT) IS NULL"
        );

        // Expr::IsNotNull
        let expr = Expr::IsNotUnknown(Box::new(Expr::Column(Column {
            relation: None,
            name: "int".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"int\" AS INT) IS NOT NULL"
        );

        // Expr::IsTrue
        let expr = Expr::IsTrue(Box::new(Expr::Column(Column {
            relation: None,
            name: "bool".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"bool\" AS BOOL) IS TRUE"
        );

        // Expr::IsNotTrue
        let expr = Expr::IsNotTrue(Box::new(Expr::Column(Column {
            relation: None,
            name: "bool".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"bool\" AS BOOL) IS NOT TRUE"
        );

        // Expr::IsFalse
        let expr = Expr::IsFalse(Box::new(Expr::Column(Column {
            relation: None,
            name: "bool".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"bool\" AS BOOL) IS FALSE"
        );

        // Expr::IsNotFalse
        let expr = Expr::IsNotFalse(Box::new(Expr::Column(Column {
            relation: None,
            name: "bool".to_string(),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE CAST(s.\"bool\" AS BOOL) IS NOT FALSE"
        );

        // Expr::BinaryExpr with Unsupported type/operation
        // ABS(-1) = 1
        let expr = Box::new(Expr::BinaryExpr {
            left: Box::new(Expr::ScalarFunction {
                fun: BuiltinScalarFunction::Abs,
                args: vec![Expr::Literal(ScalarValue::Int64(Some(-1)))],
            }),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        });
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE TRUE"
        );
        // NOT (ABS(-1) = 1)
        let expr = Box::new(Expr::Not(Box::new(Expr::BinaryExpr {
            left: Box::new(Expr::ScalarFunction {
                fun: BuiltinScalarFunction::Abs,
                args: vec![Expr::Literal(ScalarValue::Int64(Some(-1)))],
            }),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE TRUE"
        );
        // unsupported <> unsupported
        let expr = Box::new(Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "unsupported".into(),
            })),
            op: Operator::NotEq,
            right: Box::new(Expr::Column(Column {
                relation: None,
                name: "unsupported".into(),
            })),
        });
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE TRUE"
        );
        // NOT (unsupported <> unsupported)
        let expr = Box::new(Expr::Not(Box::new(Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "unsupported".into(),
            })),
            op: Operator::NotEq,
            right: Box::new(Expr::Column(Column {
                relation: None,
                name: "unsupported".into(),
            })),
        })));
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE TRUE"
        );
        // (ABS(-1) = 1) AND CAST((unsupported <> unsupported) AS BOOL)
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::Abs,
                    args: vec![Expr::Literal(ScalarValue::Int64(Some(-1)))],
                }),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
            }),
            op: Operator::And,
            right: Box::new(Expr::Cast {
                expr: Box::new(Expr::BinaryExpr {
                    left: Box::new(Expr::Column(Column {
                        relation: None,
                        name: "unsupported".into(),
                    })),
                    op: Operator::NotEq,
                    right: Box::new(Expr::Column(Column {
                        relation: None,
                        name: "unsupported".into(),
                    })),
                }),
                data_type: DataType::Boolean,
            }),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE TRUE AND TRUE"
        );
        // NOT (ABS(-1) = 1) AND NOT CAST((unsupported <> unsupported) AS BOOL)
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Not(Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::ScalarFunction {
                    fun: BuiltinScalarFunction::Abs,
                    args: vec![Expr::Literal(ScalarValue::Int64(Some(-1)))],
                }),
                op: Operator::Eq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
            }))),
            op: Operator::And,
            right: Box::new(Expr::Not(Box::new(Expr::Cast {
                expr: Box::new(Expr::BinaryExpr {
                    left: Box::new(Expr::Column(Column {
                        relation: None,
                        name: "unsupported".into(),
                    })),
                    op: Operator::NotEq,
                    right: Box::new(Expr::Column(Column {
                        relation: None,
                        name: "unsupported".into(),
                    })),
                }),
                data_type: DataType::Boolean,
            }))),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE TRUE AND TRUE"
        );

        // Expr::Between
        let expr = Expr::Between {
            expr: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
            negated: true,
            low: Box::new(Expr::Literal(ScalarValue::Int64(Some(0)))),
            high: Box::new(Expr::Literal(ScalarValue::Int64(Some(2)))),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE 1 NOT BETWEEN 0 AND 2"
        );
        let expr = Expr::Between {
            expr: Box::new(Expr::Literal(ScalarValue::Int64(Some(1)))),
            negated: false,
            low: Box::new(Expr::Literal(ScalarValue::Int64(Some(0)))),
            high: Box::new(Expr::Literal(ScalarValue::Int64(Some(2)))),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE 1 BETWEEN 0 AND 2"
        );

        // Expr::Like
        let expr = Expr::Like {
            negated: true,
            expr: Box::new(Expr::Literal(ScalarValue::Utf8(Some("foo123".into())))),
            pattern: Box::new(Expr::Literal(ScalarValue::Utf8(Some("foo%".into())))),
            escape_char: None,
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE 'foo123' NOT LIKE 'foo%' "
        );
        // Expr::Like
        let expr = Expr::Like {
            negated: false,
            expr: Box::new(Expr::Literal(ScalarValue::Utf8(Some("foo123".into())))),
            pattern: Box::new(Expr::Literal(ScalarValue::Utf8(Some("foo%".into())))),
            escape_char: Some('$'),
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE 'foo123' LIKE 'foo%' ESCAPE '$'"
        );

        // Expr::InList
        let expr = Expr::InList {
            expr: Box::new(Expr::Literal(ScalarValue::Utf8(Some("a".into())))),
            list: vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".into()))),
                Expr::Literal(ScalarValue::Utf8(Some("b'c".into()))),
            ],
            negated: true,
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE 'a' NOT IN ('a', 'b''c')"
        );
        let expr = Expr::InList {
            expr: Box::new(Expr::Literal(ScalarValue::Utf8(Some("a".into())))),
            list: vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".into()))),
                Expr::Literal(ScalarValue::Utf8(Some("b'c".into()))),
            ],
            negated: false,
        };
        assert_eq!(
            generate_s3_select_where_condition(Some(&expr), &fields, false),
            "WHERE 'a' IN ('a', 'b''c')"
        );
    }
}
