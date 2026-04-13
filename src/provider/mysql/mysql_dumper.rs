use super::MysqlResult;
use super::sql_format::{quote_ident, quote_string, write_insert};
use crate::cli::Cli;
use crate::provider::{DbObject, ObjectKind};
use chrono::Local;
use mysql::{Conn, Row, params, prelude::Queryable};
use std::io::Write;

pub fn dump_database<W: Write>(
    conn: &mut Conn,
    cli: &Cli,
    objects: &[DbObject],
    writer: &mut W,
) -> MysqlResult<()> {
    write_header(writer, cli)?;

    if !cli.no_create_info {
        writeln!(
            writer,
            "CREATE DATABASE IF NOT EXISTS {};",
            quote_ident(&cli.database)
        )?;
        writeln!(writer, "USE {};", quote_ident(&cli.database))?;
        writeln!(writer)?;
    }

    for object in objects {
        match object.kind {
            ObjectKind::BaseTable => dump_table(conn, cli, object, writer)?,
            ObjectKind::View => dump_view(conn, cli, object, writer)?,
        }
    }

    write_footer(writer)?;
    Ok(())
}

fn write_header<W: Write>(writer: &mut W, cli: &Cli) -> MysqlResult<()> {
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

fn write_footer<W: Write>(writer: &mut W) -> MysqlResult<()> {
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
    conn: &mut Conn,
    cli: &Cli,
    object: &DbObject,
    writer: &mut W,
) -> MysqlResult<()> {
    writeln!(writer, "--")?;
    writeln!(
        writer,
        "-- Table structure for table {}",
        quote_string(&object.name)
    )?;
    writeln!(writer, "--")?;
    writeln!(writer)?;

    if !cli.no_create_info {
        if !cli.skip_add_drop_table {
            writeln!(
                writer,
                "DROP TABLE IF EXISTS {};",
                quote_ident(&object.name)
            )?;
        }

        let create_sql = show_create(conn, "TABLE", &object.name)?;
        writeln!(writer, "{create_sql};")?;
        writeln!(writer)?;
    }

    if !cli.no_data {
        dump_table_data(conn, cli, &object.name, writer)?;
    }

    Ok(())
}

fn dump_view<W: Write>(
    conn: &mut Conn,
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
        quote_string(&object.name)
    )?;
    writeln!(writer, "--")?;
    writeln!(writer)?;

    if !cli.skip_add_drop_table {
        writeln!(writer, "DROP VIEW IF EXISTS {};", quote_ident(&object.name))?;
    }

    let create_sql = show_create(conn, "VIEW", &object.name)?;
    writeln!(writer, "{create_sql};")?;
    writeln!(writer)?;

    Ok(())
}

fn dump_table_data<W: Write>(
    conn: &mut Conn,
    cli: &Cli,
    table: &str,
    writer: &mut W,
) -> MysqlResult<()> {
    let columns = table_columns(conn, &cli.database, table)?;

    writeln!(writer, "--")?;
    writeln!(writer, "-- Dumping data for table {}", quote_string(table))?;
    writeln!(writer, "--")?;
    writeln!(writer)?;

    let lock_tables = !cli.skip_lock_tables && !cli.single_transaction;

    if lock_tables {
        writeln!(writer, "LOCK TABLES {} WRITE;", quote_ident(table))?;
    }

    let query = format!("SELECT * FROM {}", quote_ident(table));
    let result = conn.query_iter(query)?;

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

fn table_columns(conn: &mut Conn, database: &str, table: &str) -> MysqlResult<Vec<String>> {
    let columns = conn.exec_map(
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

fn show_create(conn: &mut Conn, object_type: &str, object_name: &str) -> MysqlResult<String> {
    let query = format!("SHOW CREATE {} {}", object_type, quote_ident(object_name));
    let row = conn
        .query_first::<Row, _>(query)?
        .ok_or_else(|| format!("SHOW CREATE {object_type} returned no row for {object_name}"))?;

    row.get::<String, usize>(1)
        .ok_or_else(|| format!("SHOW CREATE {object_type} returned an unexpected row shape").into())
}
