use super::MysqlResult;
use mysql::Value;
use std::fmt::Write as FmtWrite;
use std::io::Write;

pub fn write_insert<W: Write>(
    writer: &mut W,
    table: &str,
    columns: &[String],
    values: Vec<Value>,
) -> MysqlResult<()> {
    if columns.len() != values.len() {
        return Err(format!(
            "column count ({}) does not match value count ({}) for table {}",
            columns.len(),
            values.len(),
            table
        )
        .into());
    }

    let column_list = columns
        .iter()
        .map(|column| quote_ident(column))
        .collect::<Vec<_>>()
        .join(", ");
    let value_list = values
        .into_iter()
        .map(format_value)
        .collect::<MysqlResult<Vec<_>>>()?
        .join(", ");

    writeln!(
        writer,
        "INSERT INTO {} ({}) VALUES ({});",
        quote_ident(table),
        column_list,
        value_list
    )?;

    Ok(())
}

pub fn quote_ident(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

pub fn quote_string(value: &str) -> String {
    format!("'{}'", escape_string(value.as_bytes()))
}

pub fn format_value(value: Value) -> MysqlResult<String> {
    match value {
        Value::NULL => Ok("NULL".to_owned()),
        Value::Bytes(bytes) => Ok(quote_bytes(&bytes)),
        Value::Int(value) => Ok(value.to_string()),
        Value::UInt(value) => Ok(value.to_string()),
        Value::Float(value) => Ok(format_float(value.into())),
        Value::Double(value) => Ok(format_float(value)),
        Value::Date(year, month, day, hour, minute, second, micros) => {
            if hour == 0 && minute == 0 && second == 0 && micros == 0 {
                Ok(format!("'{year:04}-{month:02}-{day:02}'"))
            } else if micros == 0 {
                Ok(format!(
                    "'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}'"
                ))
            } else {
                Ok(format!(
                    "'{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{micros:06}'"
                ))
            }
        }
        Value::Time(is_negative, days, hours, minutes, seconds, micros) => {
            let total_hours = days * 24 + u32::from(hours);
            let sign = if is_negative { "-" } else { "" };

            if micros == 0 {
                Ok(format!(
                    "{sign}'{total_hours:02}:{minutes:02}:{seconds:02}'"
                ))
            } else {
                Ok(format!(
                    "{sign}'{total_hours:02}:{minutes:02}:{seconds:02}.{micros:06}'"
                ))
            }
        }
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        "'NaN'".to_owned()
    } else if value == f64::INFINITY {
        "'inf'".to_owned()
    } else if value == f64::NEG_INFINITY {
        "'-inf'".to_owned()
    } else {
        value.to_string()
    }
}

fn quote_bytes(bytes: &[u8]) -> String {
    if std::str::from_utf8(bytes).is_ok() {
        quote_string_bytes(bytes)
    } else {
        let mut hex = String::with_capacity(bytes.len() * 2 + 2);
        hex.push_str("0x");
        for byte in bytes {
            write!(&mut hex, "{byte:02x}").expect("writing to String cannot fail");
        }
        hex
    }
}

fn quote_string_bytes(bytes: &[u8]) -> String {
    format!("'{}'", escape_string(bytes))
}

fn escape_string(bytes: &[u8]) -> String {
    let mut escaped = String::with_capacity(bytes.len());

    for byte in bytes {
        match byte {
            0 => escaped.push_str("\\0"),
            b'\'' => escaped.push_str("\\'"),
            b'"' => escaped.push_str("\\\""),
            b'\n' => escaped.push_str("\\n"),
            b'\r' => escaped.push_str("\\r"),
            b'\\' => escaped.push_str("\\\\"),
            0x1a => escaped.push_str("\\Z"),
            _ => escaped.push(char::from(*byte)),
        }
    }

    escaped
}
