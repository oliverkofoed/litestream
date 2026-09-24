package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// TestDB_WALRestartDropsUnsyncedFrames covers silent replica data loss at a WAL
// restart.
//
// When the WAL is restarted, frames committed after litestream's last sync are
// backfilled into the database file and the WAL that held them is gone. verify()
// sees the salt change and asks detectFullCheckpoint whether a checkpoint
// happened. That function collects the frame salts still in the WAL and then
// removes every salt it already knows, so after an ordinary restart — where the
// only salts present are the new one and leftovers from the old, both known — it
// reports false. verify() concludes the restart was harmless, resumes
// incrementally at offset 32, and the frames the old WAL held past the sync
// offset are never replicated.
//
// The transaction that is lost here rewrites existing pages rather than
// appending. That distinction matters: when a commit grows the database,
// sync() fills the pages beyond the previous commit out of the database file,
// which happens to recover them. Pages modified in place get no such second
// chance — they are not in the new WAL and not beyond the old commit — so they
// keep their pre-update contents in the replica forever.
//
// The live database stays healthy throughout; only the replica is wrong, and
// nothing surfaces it until someone restores.
//
// The restart is performed by the application connection while litestream's read
// lock is released. That is not a contrived state: execCheckpoint releases the
// read lock around every checkpoint litestream issues, which is the window in
// which another connection can restart the WAL.
//
// A 48h soak with this state recorded took the losing branch 108 times across 34
// databases, and in 39 of them the database file had already diverged from the
// last replicated commit, by up to 568 pages.
func TestDB_WALRestartDropsUnsyncedFrames(t *testing.T) {
	db, sqldb, insertRows := openWALRestartTestDB(t)
	ctx := t.Context()

	const rowN = 200
	insertRows(rowN, 'a')
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	syncedEnd := db.syncState.lastSyncedWALOffset

	// Rewrite every row in place. The commit is durable in the database but not
	// yet replicated, and because the rows keep their size the database does not
	// grow, so these pages exist only in the WAL.
	if _, err := sqldb.ExecContext(ctx, `UPDATE kv SET v = ?;`,
		bytes.Repeat([]byte{'b'}, blobSize)); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(db.WALPath()); err != nil {
		t.Fatal(err)
	} else if fi.Size() <= syncedEnd {
		t.Fatalf("test setup: WAL size %d did not grow past synced end %d", fi.Size(), syncedEnd)
	}

	// Restart the WAL from the application connection, in the window litestream
	// opens around each of its own checkpoints. SQLite copies every frame into the
	// database file and starts a new WAL under a fresh salt.
	if err := db.releaseReadLock(); err != nil {
		t.Fatal(err)
	}
	var busy, walFrameN, checkpointed int
	if err := sqldb.QueryRowContext(ctx, `PRAGMA wal_checkpoint(RESTART);`).
		Scan(&busy, &walFrameN, &checkpointed); err != nil {
		t.Fatal(err)
	}
	if busy != 0 {
		t.Fatalf("test setup: checkpoint was refused (busy=%d), the WAL did not restart", busy)
	}
	if err := db.acquireReadLock(ctx); err != nil {
		t.Fatal(err)
	}

	// SQLite writes the new WAL header, and so the new salt, on the first write
	// after the restart rather than during the checkpoint itself. Commit a small
	// transaction so the WAL litestream reads next is genuinely a new generation.
	insertRows(1, 'b')

	// Every row now holds 'b' in the database. Replication must carry that to the
	// replica.
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Replica.Sync(ctx); err != nil {
		t.Fatal(err)
	}

	restored := restoreReplica(t, db)
	defer func() { _ = restored.Close() }()

	var stale int
	if err := restored.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM kv WHERE v = ?;`, bytes.Repeat([]byte{'a'}, blobSize)).Scan(&stale); err != nil {
		t.Fatal(err)
	}
	if stale != 0 {
		t.Fatalf("replica lost a committed transaction across the WAL restart: %d of %d rows still hold their pre-update contents",
			stale, rowN)
	}

	var ick string
	if err := restored.QueryRowContext(ctx, `PRAGMA integrity_check;`).Scan(&ick); err != nil {
		t.Fatal(err)
	}
	if ick != "ok" {
		t.Fatalf("restored database failed integrity_check: %s", ick)
	}
}

// restoreReplica restores the replica into a temporary file and opens it.
func restoreReplica(t *testing.T, db *DB) *sql.DB {
	t.Helper()

	restorePath := filepath.Join(t.TempDir(), "restored.db")
	if err := db.Replica.Restore(t.Context(), RestoreOptions{OutputPath: restorePath}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	restored, err := sql.Open("sqlite", restorePath)
	if err != nil {
		t.Fatal(err)
	}
	return restored
}

// blobSize keeps each row near a kilobyte so a few hundred rows span many pages.
const blobSize = 1000

// openWALRestartTestDB opens a replicated database with litestream's own monitors
// and checkpoints disabled, so the only WAL restart is the one the test performs,
// and returns a helper that commits n rows of the given filler byte.
func openWALRestartTestDB(t *testing.T) (*DB, *sql.DB, func(n int, fill byte)) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "db")
	db := NewDB(dbPath)
	db.MonitorInterval = 0
	db.CheckpointInterval = 0
	db.MinCheckpointPageN = 1000000 // never checkpoint: keep every frame in the WAL
	db.Replica = NewReplica(db)
	db.Replica.Client = &testReplicaClient{dir: t.TempDir()}
	db.Replica.MonitorEnabled = false
	db.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })

	sqldb, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	for _, q := range []string{
		`PRAGMA journal_mode = wal`,
		`PRAGMA busy_timeout = 5000`,
		`CREATE TABLE kv (id INTEGER PRIMARY KEY, v BLOB)`,
	} {
		if _, err := sqldb.Exec(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}

	insertRows := func(n int, fill byte) {
		t.Helper()
		tx, err := sqldb.Begin()
		if err != nil {
			t.Fatal(err)
		}
		blob := bytes.Repeat([]byte{fill}, blobSize)
		for range n {
			if _, err := tx.Exec(`INSERT INTO kv (v) VALUES (?)`, blob); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	return db, sqldb, insertRows
}
