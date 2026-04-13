pub mod mysql;
pub mod table_type;

use crate::cli::{Cli, TypeProvider};
use crate::provider::mysql::MysqlProvider;
use std::io::Write;
use table_type::TableType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectKind {
    BaseTable,
    View,
}

#[derive(Debug, Clone)]
pub struct DbObject {
    pub name: String,
    pub kind: ObjectKind,
}

pub struct Provider {
    backend: ProviderBackend,
}

enum ProviderBackend {
    MySql(MysqlProvider),
}

impl Provider {
    pub fn new(cli: &Cli) -> Result<Self, Box<dyn std::error::Error>> {
        let backend = match cli.type_provider {
            TypeProvider::Mysql => ProviderBackend::MySql(MysqlProvider::new(cli)?),
        };

        Ok(Self { backend })
    }

    pub fn start_single_transaction(
        &mut self,
        cli: &Cli,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if cli.single_transaction {
            self.query_drop("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")?;
            self.query_drop("START TRANSACTION /*!40100 WITH CONSISTENT SNAPSHOT */")?;
        }

        Ok(())
    }

    pub fn commit_transaction(&mut self, cli: &Cli) -> Result<(), Box<dyn std::error::Error>> {
        if cli.single_transaction {
            self.query_drop("COMMIT")?;
        }

        Ok(())
    }

    pub fn query_drop(&mut self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        match &mut self.backend {
            ProviderBackend::MySql(mysql) => mysql.query_drop(query)?,
        }

        Ok(())
    }

    pub fn get_info_tables(
        &mut self,
        cli: &Cli,
    ) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
        match &mut self.backend {
            ProviderBackend::MySql(mysql) => Ok(mysql.get_info_tables(cli)?),
        }
    }

    pub fn dump_database<W: Write>(
        &mut self,
        cli: &Cli,
        objects: &[DbObject],
        writer: &mut W,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match &mut self.backend {
            ProviderBackend::MySql(mysql) => mysql.dump_database(cli, objects, writer),
        }
    }

    pub fn get_table_type(&self, cli: &Cli, table_type: &str) -> Option<TableType> {
        match cli.type_provider {
            TypeProvider::Mysql => TableType::from_mysql(table_type),
        }
    }
}
