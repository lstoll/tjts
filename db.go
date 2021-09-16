package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// migrations are run in the order presented here
var migrations = []migration{
	{
		Idx: 202109151300,
		SQL: `
			create table chunks (
				id integer primary key autoincrement,
				sequence integer not null,
				stream_id text not null,
				chunk_id text not null,
				duration real not null,
				fetched_at datetime not null,
				unique (stream_id, chunk_id)
			);
		`,
	},
	{
		Idx: 202109152220,
		SQL: `
			create table sessions (
				id text not null primary key,
				data text not null, -- serialized JSON
				created_at datetime not null default (datetime('now','utc')),
				updated_at datetime not null default (datetime('now','utc'))
			);
		`,
	},
}

func newDB(path string) (*sql.DB, error) {
	// remember, memory errors here just mean like bad dsn?
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("opening DB at %s: %v", path, err)
	}

	if err := integrityCheck(context.TODO(), db); err != nil {
		return nil, fmt.Errorf("integrity check of %s failed: %v", path, err)
	}

	if err := migrate(context.TODO(), db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrating db: %v", err)
	}

	return db, nil
}

type migration struct {
	// Idx is a unique identifier for this migration. Datestamp is a good idea
	Idx int64
	// SQL to execute as part of this migration
	SQL string
	// AfterFunc is run inside the migration transaction, if not nil. Runs
	// _after_ the associated SQL is executed. This should be self-contained
	// code, that has no dependencies on application structure to make sure it
	// passes the test of time. It should not commit or rollback the TX, the
	// migration framework will handle that
	AfterFunc func(context.Context, *sql.Tx) error
}

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(
		ctx,
		`create table if not exists migrations (
		idx integer primary key not null,
		at datetime not null
		);`,
	); err != nil {
		return err
	}

	if err := execTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		for _, mig := range migrations {
			var idx int64
			err := tx.QueryRowContext(ctx, `select idx from migrations where idx = $1;`, mig.Idx).Scan(&idx)
			if err == nil {
				// selected fine so we've already inserted migration, next
				// please.
				continue
			}
			if err != nil && err != sql.ErrNoRows {
				// genuine error
				return fmt.Errorf("checking for migration existence: %v", err)
			}

			if mig.SQL != "" {
				if _, err := tx.ExecContext(ctx, mig.SQL); err != nil {
					return err
				}
			}

			if mig.AfterFunc != nil {
				if err := mig.AfterFunc(ctx, tx); err != nil {
					return err
				}
			}

			if _, err := tx.ExecContext(ctx, `insert into migrations (idx, at) values ($1, datetime('now'));`, mig.Idx); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func execTx(ctx context.Context, db *sql.DB, f func(ctx context.Context, tx *sql.Tx) error) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := f(ctx, tx); err != nil {
		// Not much we can do about an error here, but at least the database will
		// eventually cancel it on its own if it fails
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func integrityCheck(ctx context.Context, conn *sql.DB) error {
	// https://www.sqlite.org/pragma.html#pragma_integrity_check
	rows, err := conn.QueryContext(ctx, "PRAGMA integrity_check;")
	if err != nil {
		return err
	}
	defer rows.Close()

	var res []string
	for rows.Next() {
		var val string
		if err := rows.Scan(&val); err != nil {
			return err
		}
		res = append(res, val)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(res) == 1 && res[0] == "ok" {
		return nil
	}

	return fmt.Errorf("integrity problems: %s", strings.Join(res, ", "))
}
