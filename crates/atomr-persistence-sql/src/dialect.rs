//! URL scheme detection for the supported SQL dialects.

use crate::config::SqlDialect;

pub fn detect_dialect(url: &str) -> Option<SqlDialect> {
    let lower = url.to_ascii_lowercase();
    if lower.starts_with("sqlite:") {
        Some(SqlDialect::Sqlite)
    } else if lower.starts_with("postgres:") || lower.starts_with("postgresql:") {
        Some(SqlDialect::Postgres)
    } else if lower.starts_with("mysql:") || lower.starts_with("mariadb:") {
        Some(SqlDialect::MySql)
    } else if lower.starts_with("mssql:") || lower.starts_with("sqlserver:") {
        Some(SqlDialect::MsSql)
    } else {
        None
    }
}

pub(crate) fn sqlite_migration() -> &'static str {
    include_str!("../migrations/sqlite/001_init.sql")
}

pub(crate) fn postgres_migration() -> &'static str {
    include_str!("../migrations/postgres/001_init.sql")
}

pub(crate) fn mysql_migration() -> &'static str {
    include_str!("../migrations/mysql/001_init.sql")
}

pub(crate) fn mssql_migration() -> &'static str {
    include_str!("../migrations/mssql/001_init.sql")
}

pub(crate) fn migration_for(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => sqlite_migration(),
        SqlDialect::Postgres => postgres_migration(),
        SqlDialect::MySql => mysql_migration(),
        SqlDialect::MsSql => mssql_migration(),
    }
}

/// Step `002_worm.sql` — additive WORM hash-chain + bitemporal columns
/// (FR-9 / FR-8). Applied after [`migration_for`] as a second statement
/// batch by [`crate::schema::ensure_schema`].
pub(crate) fn worm_migration_for(dialect: SqlDialect) -> &'static str {
    match dialect {
        SqlDialect::Sqlite => include_str!("../migrations/sqlite/002_worm.sql"),
        SqlDialect::Postgres => include_str!("../migrations/postgres/002_worm.sql"),
        SqlDialect::MySql => include_str!("../migrations/mysql/002_worm.sql"),
        SqlDialect::MsSql => include_str!("../migrations/mssql/002_worm.sql"),
    }
}

/// DDL that enforces WORM `deny_update_delete` for the given dialect.
///
/// Only SQLite is enforced (and tested) in-process via a `RAISE(ABORT)`
/// trigger. Other dialects return an empty string here because their
/// enforcement requires elevated privileges (REVOKE / DENY) or DDL that the
/// embedded migration runner cannot portably execute; those are documented
/// inline in each `002_worm.sql` for operators to apply out-of-band.
pub(crate) fn worm_deny_trigger_for(dialect: SqlDialect) -> &'static str {
    match dialect {
        // Trigger bodies contain inner `;` (after SELECT), so they are
        // returned as a single string split on the `@@` sentinel by the
        // installer rather than by the naive `;` splitter.
        SqlDialect::Sqlite => concat!(
            "CREATE TRIGGER IF NOT EXISTS event_journal_worm_no_update ",
            "BEFORE UPDATE ON event_journal BEGIN SELECT RAISE(ABORT, 'WORM: event_journal is append-only'); END",
            "@@",
            "CREATE TRIGGER IF NOT EXISTS event_journal_worm_no_delete ",
            "BEFORE DELETE ON event_journal BEGIN SELECT RAISE(ABORT, 'WORM: event_journal is append-only'); END",
        ),
        // Documented in each dialect's 002_worm.sql; applied out-of-band.
        SqlDialect::Postgres | SqlDialect::MySql | SqlDialect::MsSql => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_all_schemes() {
        assert_eq!(detect_dialect("sqlite::memory:"), Some(SqlDialect::Sqlite));
        assert_eq!(detect_dialect("postgres://a"), Some(SqlDialect::Postgres));
        assert_eq!(detect_dialect("postgresql://a"), Some(SqlDialect::Postgres));
        assert_eq!(detect_dialect("mysql://a"), Some(SqlDialect::MySql));
        assert_eq!(detect_dialect("mssql://a"), Some(SqlDialect::MsSql));
        assert_eq!(detect_dialect("https://x"), None);
    }

    #[test]
    fn migrations_embedded() {
        assert!(migration_for(SqlDialect::Sqlite).contains("event_journal"));
        assert!(migration_for(SqlDialect::Postgres).contains("event_journal"));
        assert!(migration_for(SqlDialect::MySql).contains("event_journal"));
    }
}
