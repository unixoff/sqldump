use std::error::Error;

mod mysql_dumper;
pub mod mysql_provider;
mod sql_format;

pub use mysql_provider::MysqlProvider;

pub type MysqlResult<T> = Result<T, Box<dyn Error>>;
