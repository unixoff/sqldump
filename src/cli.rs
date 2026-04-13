use std::error::Error;
use std::path::PathBuf;

use clap::{ArgAction, Parser, ValueEnum};

type CliResult<T> = Result<T, Box<dyn Error>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum TypeProvider {
    Mysql,
}

#[derive(Debug, Parser)]
#[command(
    name = "sqldump",
    version,
    about = "Dump a MySQL/MariaDB database as SQL",
    long_about = None,
    disable_help_flag = true
)]
pub struct Cli {
    /// Print help.
    #[arg(long = "help", action = ArgAction::Help)]
    help: Option<bool>,

    /// Database provider type.
    #[arg(long = "type", value_enum, default_value_t = TypeProvider::Mysql)]
    pub type_provider: TypeProvider,

    /// MySQL host.
    #[arg(short = 'h', long = "host", default_value = "localhost")]
    pub host: String,

    /// MySQL TCP port.
    #[arg(short = 'P', long = "port", default_value_t = 3306)]
    pub port: u16,

    /// MySQL user.
    #[arg(short = 'u', long = "user", default_value = "root")]
    pub user: String,

    /// Password. If passed without a value, sqldump prompts for it.
    #[arg(
        short = 'p',
        long = "password",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = ""
    )]
    pub password: Option<String>,

    /// Read password from this environment variable.
    #[arg(long = "password-env")]
    pub password_env: Option<String>,

    /// Database to dump.
    pub database: String,

    /// Dump only these tables/views.
    pub tables: Vec<String>,

    /// Write dump to a file instead of stdout.
    #[arg(short = 'r', long = "result-file")]
    pub result_file: Option<PathBuf>,

    /// Dump schema only.
    #[arg(short = 'd', long = "no-data", action = ArgAction::SetTrue)]
    pub no_data: bool,

    /// Dump data only.
    #[arg(short = 't', long = "no-create-info", action = ArgAction::SetTrue)]
    pub no_create_info: bool,

    /// Do not emit DROP TABLE / DROP VIEW statements.
    #[arg(long = "skip-add-drop-table", action = ArgAction::SetTrue)]
    pub skip_add_drop_table: bool,

    /// Do not wrap table dumps with LOCK TABLES.
    #[arg(long = "skip-lock-tables", action = ArgAction::SetTrue)]
    pub skip_lock_tables: bool,

    /// Use one transaction for a consistent InnoDB snapshot.
    #[arg(long = "single-transaction", action = ArgAction::SetTrue)]
    pub single_transaction: bool,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    pub fn resolve_password(&mut self) -> CliResult<()> {
        if let Some(env_name) = &self.password_env {
            let value = std::env::var(env_name)
                .map_err(|_| format!("environment variable {env_name} is not set"))?;
            self.password = Some(value);
            return Ok(());
        }

        if self.password.as_deref() == Some("") {
            let prompt = format!("Enter password for {}: ", self.user);
            self.password = Some(rpassword::prompt_password(prompt)?);
        }

        Ok(())
    }

    pub fn validate(&self) -> CliResult<()> {
        if self.no_data && self.no_create_info {
            return Err("--no-data and --no-create-info cannot be used together".into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_password_flag_without_consuming_database() {
        let cli = Cli::try_parse_from([
            "sqldump",
            "-h",
            "127.0.0.1",
            "-P",
            "3306",
            "-u",
            "root",
            "-p",
            "ubntshop",
        ])
        .unwrap();

        assert_eq!(cli.password.as_deref(), Some(""));
        assert_eq!(cli.database, "ubntshop");
    }

    #[test]
    fn parses_inline_password() {
        let cli = Cli::try_parse_from(["sqldump", "-u", "root", "--password=secret", "ubntshop"])
            .unwrap();

        assert_eq!(cli.password.as_deref(), Some("secret"));
        assert_eq!(cli.database, "ubntshop");
    }

    #[test]
    fn parses_type_provider_as_enum() {
        let cli = Cli::try_parse_from(["sqldump", "--type", "mysql", "ubntshop"]).unwrap();

        assert_eq!(cli.type_provider, TypeProvider::Mysql);
    }

    #[test]
    fn resolve_password_keeps_inline_password() {
        let mut cli =
            Cli::try_parse_from(["sqldump", "-u", "root", "--password=secret", "ubntshop"])
                .unwrap();

        cli.resolve_password().unwrap();

        assert_eq!(cli.password.as_deref(), Some("secret"));
    }

    #[test]
    fn rejects_schema_only_and_data_only_together() {
        let cli =
            Cli::try_parse_from(["sqldump", "--no-data", "--no-create-info", "ubntshop"]).unwrap();

        assert!(cli.validate().is_err());
    }
}
