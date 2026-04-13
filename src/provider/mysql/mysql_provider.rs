use crate::cli::Cli;
use crate::provider::{DbObject, ObjectKind};
use chrono::Local;
use mysql::{Conn, OptsBuilder, Row, Value, params, prelude::Queryable};
use std::error::Error;
use std::fmt::Write as FmtWrite;
use std::io::Write;

type MysqlResult<T> = Result<T, Box<dyn Error>>;

pub struct MysqlProvider {
    connect: Conn,
}

impl MysqlProvider {
    pub fn new(cli: &Cli) -> MysqlResult<Self> {
        let opts = OptsBuilder::new()
            .ip_or_hostname(Some(cli.host.clone()))
            .tcp_port(cli.port)
            .user(Some(cli.user.clone()))
            .pass(cli.password.clone())
            .db_name(Some(cli.database.clone()));
        let connect = Conn::new(opts)?;

        Ok(Self { connect })
    }

    pub fn query_drop(&mut self, query: &str) -> Result<(), mysql::Error> {
        self.connect.query_drop(query)
    }

    pub fn get_info_tables(&mut self, cli: &Cli) -> Result<Vec<(String, String)>, mysql::Error> {
        let rows: Vec<(String, String)> = self.connect.exec(
            r#"
        SELECT TABLE_NAME, TABLE_TYPE
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = :database
        ORDER BY TABLE_NAME
        "#,
            params! {
                "database" => &cli.database,
            },
        )?;

        Ok(rows)
    }

    pub fn dump_database<W: Write>(
        &mut self,
        cli: &Cli,
        objects: &[DbObject],
        writer: &mut W,
    ) -> MysqlResult<()> {
        self.write_header(writer, cli)?;

        if !cli.no_create_info {
            writeln!(
                writer,
                "CREATE DATABASE IF NOT EXISTS {};",
                self.quote_ident(&cli.database)
            )?;
            writeln!(writer, "USE {};", self.quote_ident(&cli.database))?;
            writeln!(writer)?;
        }

        for object in objects {
            match object.kind {
                ObjectKind::BaseTable => self.dump_table(cli, object, writer)?,
                ObjectKind::View => self.dump_view(cli, object, writer)?,
            }
        }

        self.write_footer(writer)?;
        Ok(())
    }

    fn write_header<W: Write>(&self, writer: &mut W, cli: &Cli) -> MysqlResult<()> {
        let now = Local::now().format("%Y-%m-%d %H:%M:%S %z");

        writeln!(writer, "-- sqldump {}", env!("CARGO_PKG_VERSION"))?;
        writeln!(
            writer,
            "-- Host: {}    Database: {}",
            cli.host, cli.database
        )?;
        writeln!(writer, "-- Dumped at: {now}")?;
        writeln!(writer)?;
        writeln!(
            writer,
            "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;"
        )?;
        writeln!(
            writer,
            "/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;"
        )?;
        writeln!(
            writer,
            "/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;"
        )?;
        writeln!(writer, "/*!40101 SET NAMES utf8mb4 */;")?;
        writeln!(writer, "/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;")?;
        writeln!(writer, "/*!40103 SET TIME_ZONE='+00:00' */;")?;
        writeln!(
            writer,
            "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;"
        )?;
        writeln!(
            writer,
            "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;"
        )?;
        writeln!(
            writer,
            "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;"
        )?;
        writeln!(writer)?;

        Ok(())
    }

    fn write_footer<W: Write>(&self, writer: &mut W) -> MysqlResult<()> {
        writeln!(writer, "/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;")?;
        writeln!(
            writer,
            "/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;"
        )?;
        writeln!(writer, "/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;")?;
        writeln!(writer, "/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;")?;
        writeln!(
            writer,
            "/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;"
        )?;
        writeln!(
            writer,
            "/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;"
        )?;
        writeln!(
            writer,
            "/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;"
        )?;
        writeln!(writer, "-- Dump completed")?;

        Ok(())
    }

    fn dump_table<W: Write>(
        &mut self,
        cli: &Cli,
        object: &DbObject,
        writer: &mut W,
    ) -> MysqlResult<()> {
        writeln!(writer, "--")?;
        writeln!(
            writer,
            "-- Table structure for table {}",
            self.quote_string(&object.name)
        )?;
        writeln!(writer, "--")?;
        writeln!(writer)?;

        if !cli.no_create_info {
            if !cli.skip_add_drop_table {
                writeln!(
                    writer,
                    "DROP TABLE IF EXISTS {};",
                    self.quote_ident(&object.name)
                )?;
            }

            let create_sql = self.show_create("TABLE", &object.name)?;
            writeln!(writer, "{create_sql};")?;
            writeln!(writer)?;
        }

        if !cli.no_data {
            self.dump_table_data(cli, &object.name, writer)?;
        }

        Ok(())
    }

    fn dump_view<W: Write>(
        &mut self,
        cli: &Cli,
        object: &DbObject,
        writer: &mut W,
    ) -> MysqlResult<()> {
        if cli.no_create_info {
            return Ok(());
        }

        writeln!(writer, "--")?;
        writeln!(
            writer,
            "-- View structure for view {}",
            self.quote_string(&object.name)
        )?;
        writeln!(writer, "--")?;
        writeln!(writer)?;

        if !cli.skip_add_drop_table {
            writeln!(
                writer,
                "DROP VIEW IF EXISTS {};",
                self.quote_ident(&object.name)
            )?;
        }

        let create_sql = self.show_create("VIEW", &object.name)?;
        writeln!(writer, "{create_sql};")?;
        writeln!(writer)?;

        Ok(())
    }

    fn dump_table_data<W: Write>(
        &mut self,
        cli: &Cli,
        table: &str,
        writer: &mut W,
    ) -> MysqlResult<()> {
        let columns = self.table_columns(&cli.database, table)?;

        writeln!(writer, "--")?;
        writeln!(
            writer,
            "-- Dumping data for table {}",
            self.quote_string(table)
        )?;
        writeln!(writer, "--")?;
        writeln!(writer)?;

        let lock_tables = !cli.skip_lock_tables && !cli.single_transaction;

        if lock_tables {
            writeln!(writer, "LOCK TABLES {} WRITE;", self.quote_ident(table))?;
        }

        let query = format!("SELECT * FROM {}", self.quote_ident(table));
        let result = self.connect.query_iter(query)?;

        for row in result {
            let values = row?.unwrap();
            write_insert(writer, table, &columns, values)?;
        }

        if lock_tables {
            writeln!(writer, "UNLOCK TABLES;")?;
        }

        writeln!(writer)?;
        Ok(())
    }

    fn table_columns(&mut self, database: &str, table: &str) -> MysqlResult<Vec<String>> {
        let columns = self.connect.exec_map(
            r#"
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table
        ORDER BY ORDINAL_POSITION
        "#,
            params! {
                "database" => database,
                "table" => table,
            },
            |column: String| column,
        )?;

        Ok(columns)
    }

    fn show_create(&mut self, object_type: &str, object_name: &str) -> MysqlResult<String> {
        let query = format!(
            "SHOW CREATE {} {}",
            object_type,
            self.quote_ident(object_name)
        );
        let row = self.connect.query_first::<Row, _>(query)?.ok_or_else(|| {
            format!("SHOW CREATE {object_type} returned no row for {object_name}")
        })?;

        row.get::<String, usize>(1).ok_or_else(|| {
            format!("SHOW CREATE {object_type} returned an unexpected row shape").into()
        })
    }

    fn quote_ident(&self, value: &str) -> String {
        quote_ident(value)
    }

    fn quote_string(&self, value: &str) -> String {
        quote_string(value)
    }

    // fn format_float(&self, value: f64) -> String {
    //     format_float(value)
    // }
    //
    // fn quote_bytes(&self, bytes: &[u8]) -> String {
    //     quote_bytes(bytes)
    // }
    //
    // fn quote_string_bytes(&self, bytes: &[u8]) -> String {
    //     quote_string_bytes(bytes)
    // }
    //
    // fn escape_string(&self, bytes: &[u8]) -> String {
    //     escape_string(bytes)
    // }
}

fn write_insert<W: Write>(
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

fn quote_ident(value: &str) -> String {
    format!("`{}`", value.replace('`', "``"))
}

fn quote_string(value: &str) -> String {
    format!("'{}'", escape_string(value.as_bytes()))
}

fn format_value(value: Value) -> MysqlResult<String> {
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
