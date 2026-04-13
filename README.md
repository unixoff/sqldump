# sqldump

`sqldump` is a small MySQL/MariaDB dump utility with a `mysqldump`-style CLI.
It connects to a database and writes SQL to stdout or to a result file.

## Usage

```sh
cargo run -- --host 127.0.0.1 --port 3306 --user root -p my_database > dump.sql
```

Write directly to a file:

```sh
cargo run -- -h 127.0.0.1 -P 3306 -u root -p my_database --result-file dump.sql
```

Use an environment variable for the password:

```sh
MYSQL_PWD=secret cargo run -- --password-env MYSQL_PWD my_database > dump.sql
```

Pass a password inline:

```sh
cargo run -- -u root --password=secret my_database > dump.sql
```

Dump selected tables only:

```sh
cargo run -- -u root -p my_database users orders > dump.sql
```

Schema only:

```sh
cargo run -- -u root -p --no-data my_database > schema.sql
```

Consistent InnoDB snapshot:

```sh
cargo run -- -u root -p --single-transaction my_database > dump.sql
```

## Supported Output

- `CREATE DATABASE` and `USE`
- `DROP TABLE IF EXISTS` / `DROP VIEW IF EXISTS`
- `SHOW CREATE TABLE`
- `SHOW CREATE VIEW`
- `INSERT` statements for base table data
- session guards for charset, timezone, SQL mode, unique checks, and foreign keys

This is not a full replacement for every `mysqldump` option yet. Routines,
events, triggers, replication metadata, and tablespaces are not dumped.
