package main

import (
	"context"
	"database/sql"
	"fmt"
)

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

func (r *recorder) migrate(ctx context.Context) error {
	if _, err := r.db.ExecContext(
		ctx,
		`create table if not exists migrations (
		idx integer primary key not null,
		at datetime not null
		);`,
	); err != nil {
		return err
	}

	if err := r.execTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
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
}
