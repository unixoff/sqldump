use crate::cli::Cli;
use crate::provider::{DbObject, ObjectKind, Provider};
use std::fs::File;
use std::io::{self, BufWriter, Write};

pub struct App {
    cli: Cli,
    provider: Provider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableType {
    BaseTable,
    View,
}

impl TableType {
    fn from_mysql(value: &str) -> Option<Self> {
        match value {
            "BASE TABLE" => Some(Self::BaseTable),
            "VIEW" => Some(Self::View),
            _ => None,
        }
    }

    fn object_kind(self) -> ObjectKind {
        match self {
            Self::BaseTable => ObjectKind::BaseTable,
            Self::View => ObjectKind::View,
        }
    }
}

impl App {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let mut cli = Cli::parse_args();
        cli.validate()?;
        cli.resolve_password()?;

        let provider = Provider::new(&cli)?;

        Ok(Self { cli, provider })
    }

    pub fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.provider.start_single_transaction(&self.cli)?;
        let objects = self.load_objects()?;

        match &self.cli.result_file {
            Some(path) => {
                let file = File::create(path).map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("failed to create {}: {error}", path.display()),
                    )
                })?;
                let mut writer = BufWriter::new(file);
                self.provider
                    .dump_database(&self.cli, &objects, &mut writer)?;
                writer.flush()?;
            }
            None => {
                let stdout = io::stdout();
                let mut writer = BufWriter::new(stdout.lock());
                self.provider
                    .dump_database(&self.cli, &objects, &mut writer)?;
                writer.flush()?;
            }
        }
        self.provider.commit_transaction(&self.cli)?;

        Ok(())
    }

    fn load_objects(&mut self) -> Result<Vec<DbObject>, Box<dyn std::error::Error>> {
        let requested = if self.cli.tables.is_empty() {
            None
        } else {
            Some(self.cli.tables.clone())
        };

        let rows = self.provider.get_info_tables(&self.cli)?;
        let mut objects = Vec::new();

        for (name, table_type) in rows {
            if requested
                .as_ref()
                .is_some_and(|tables| !tables.iter().any(|table| table == &name))
            {
                continue;
            }

            let Some(table_type) = TableType::from_mysql(&table_type) else {
                continue;
            };
            let kind = table_type.object_kind();

            objects.push(DbObject { name, kind });
        }

        if let Some(requested) = requested {
            let missing = requested
                .iter()
                .filter(|table| !objects.iter().any(|object| object.name == **table))
                .cloned()
                .collect::<Vec<_>>();

            if !missing.is_empty() {
                return Err(format!(
                    "object(s) not found in {}: {}",
                    self.cli.database,
                    missing.join(", ")
                )
                .into());
            }
        }

        Ok(objects)
    }
}
