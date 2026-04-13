use super::MysqlResult;
use super::mysql_dumper;
use crate::cli::Cli;
use crate::provider::DbObject;
use mysql::{Conn, OptsBuilder, params, prelude::Queryable};
use std::io::Write;

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
        mysql_dumper::dump_database(&mut self.connect, cli, objects, writer)
    }
}
