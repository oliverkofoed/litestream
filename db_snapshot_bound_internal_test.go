package litestream

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/superfly/ltx"
)

// TestDB_SnapshotReaderExcludesCommitsAfterPosition commits transactions after
// the last sync so the live WAL extends past the extent recorded for the
// position, then takes a snapshot. The snapshot must advertise the synced
// position and be byte-identical to the database rebuilt from the L0 chain at
// that position: nothing from the newer WAL contents may leak in.
func TestDB_SnapshotReaderExcludesCommitsAfterPosition(t *testing.T) {
	db, insertRows := openSnapshotBoundTestDB(t)
	ctx := t.Context()

	insertRows(20)
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	syncedEnd := db.syncState.lastSyncedWALOffset

	// Commit more without syncing: the WAL now runs past the position's extent.
	insertRows(40)
	if fi, err := os.Stat(db.WALPath()); err != nil {
		t.Fatal(err)
	} else if fi.Size() <= syncedEnd {
		t.Fatalf("test setup: WAL size %d did not grow past synced end %d", fi.Size(), syncedEnd)
	}

	assertSnapshotMatchesPosition(t, db, pos, syncedEnd, 20)
}

// TestDB_SnapshotReaderBoundInsideTransaction reproduces #1490 through the
// public entry point. The synced WAL extent is moved one frame into a
// transaction committed after the position. Before the fix the snapshot read
// followed that transaction through to its commit frame and failed with
// "snapshot wal read exceeded bound", stalling the snapshot level for good.
//
// The bound is injected via syncState.lastSyncedWALOffset because the state
// that produces a mid-transaction extent in production is not yet pinned down
// (see the upstream report); everything downstream of that field is the real
// path: snapshotPosition, the chkMu handoff, snapshotReader, the bounded page
// map and the encoder.
func TestDB_SnapshotReaderBoundInsideTransaction(t *testing.T) {
	db, insertRows := openSnapshotBoundTestDB(t)
	ctx := t.Context()

	insertRows(20)
	if err := db.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	syncedEnd := db.syncState.lastSyncedWALOffset

	// Commit a multi-frame transaction without syncing.
	insertRows(40)
	frameSize := int64(db.pageSize) + WALFrameHeaderSize
	if fi, err := os.Stat(db.WALPath()); err != nil {
		t.Fatal(err)
	} else if fi.Size() < syncedEnd+2*frameSize {
		t.Fatalf("test setup: need a transaction of at least two frames past %d, WAL is %d bytes", syncedEnd, fi.Size())
	}

	// Move the recorded extent one frame into that transaction. The snapshot
	// must still describe the position: the crossing transaction is excluded,
	// not read through.
	bound := syncedEnd + frameSize
	db.syncState.lastSyncedWALOffset = bound

	assertSnapshotMatchesPosition(t, db, pos, bound, 20)
}

// openSnapshotBoundTestDB opens a replicated database with all monitors and
// checkpoints disabled, so every frame written stays in the WAL, and returns
// a helper that commits n ~1KB rows in a single transaction.
func openSnapshotBoundTestDB(t *testing.T) (*DB, func(n int)) {
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

	insertRows := func(n int) {
		t.Helper()
		tx, err := sqldb.Begin()
		if err != nil {
			t.Fatal(err)
		}
		blob := bytes.Repeat([]byte{0x5a}, 1000)
		for range n {
			if _, err := tx.Exec(`INSERT INTO kv (v) VALUES (?)`, blob); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	return db, insertRows
}

// assertSnapshotMatchesPosition takes a snapshot and requires it to advertise
// pos, keep its WAL extent within maxWALEnd, match the L0 chain rebuilt at pos
// byte for byte, and open as a consistent database holding wantRows rows.
func assertSnapshotMatchesPosition(t *testing.T, db *DB, pos ltx.Pos, maxWALEnd int64, wantRows int) {
	t.Helper()

	snapPos, r, err := db.SnapshotReader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapPos.TXID != pos.TXID {
		t.Fatalf("snapshot position %s, want synced position %s", snapPos.TXID, pos.TXID)
	}
	dec := ltx.NewDecoder(r)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatalf("decode snapshot header: %v", err)
	}
	hdr := dec.Header()
	if hdr.MaxTXID != pos.TXID {
		t.Fatalf("snapshot header MaxTXID=%s, want %s", hdr.MaxTXID, pos.TXID)
	}
	if end := hdr.WALOffset + hdr.WALSize; end > maxWALEnd {
		t.Fatalf("snapshot WAL extent %d exceeds bound %d", end, maxWALEnd)
	}
	snapshot := make([]byte, int64(hdr.Commit)*int64(hdr.PageSize))
	page := make([]byte, hdr.PageSize)
	for {
		var phdr ltx.PageHeader
		if err := dec.DecodePage(&phdr, page); err == io.EOF {
			break
		} else if err != nil {
			t.Fatalf("decode snapshot page: %v", err)
		}
		copy(snapshot[int64(phdr.Pgno-1)*int64(hdr.PageSize):], page)
	}
	if err := dec.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	// The L0 chain is the incremental record of exactly the transactions the
	// position covers; the snapshot must reproduce it exactly.
	truth := rebuildFromL0(t, db, pos.TXID)
	if len(truth) != len(snapshot) {
		t.Fatalf("snapshot is %d bytes, L0 rebuild is %d bytes", len(snapshot), len(truth))
	}
	pageSize := int(hdr.PageSize)
	for pgno := 1; pgno <= len(truth)/pageSize; pgno++ {
		off := (pgno - 1) * pageSize
		if !bytes.Equal(snapshot[off:off+pageSize], truth[off:off+pageSize]) {
			t.Fatalf("page %d differs between snapshot and L0 rebuild at %s", pgno, pos.TXID)
		}
	}

	snapPath := filepath.Join(t.TempDir(), "snapshot.db")
	if err := os.WriteFile(snapPath, snapshot, 0o600); err != nil {
		t.Fatal(err)
	}
	snapDB, err := sql.Open("sqlite", snapPath)
	if err != nil {
		t.Fatal(err)
	}
	defer snapDB.Close()
	var integrity string
	if err := snapDB.QueryRow(`PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatal(err)
	} else if integrity != "ok" {
		t.Fatalf("snapshot integrity check: %s", integrity)
	}
	var n int
	if err := snapDB.QueryRow(`SELECT COUNT(*) FROM kv`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if n != wantRows {
		t.Fatalf("snapshot has %d rows, want %d", n, wantRows)
	}
}

// rebuildFromL0 compacts the local L0 files from TXID 1 through maxTXID into a
// raw database image.
func rebuildFromL0(t *testing.T, db *DB, maxTXID ltx.TXID) []byte {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(db.LTXLevelDir(0), "*.ltx"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(paths)

	var rdrs []io.Reader
	for _, p := range paths {
		_, max, err := ltx.ParseFilename(filepath.Base(p))
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		if max > maxTXID {
			continue
		}
		f, err := os.Open(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		rdrs = append(rdrs, f)
	}
	if len(rdrs) == 0 {
		t.Fatalf("no L0 files at or below %s in %s", maxTXID, db.LTXLevelDir(0))
	}

	var compacted bytes.Buffer
	c, err := ltx.NewCompactor(&compacted, rdrs)
	if err != nil {
		t.Fatal(err)
	}
	c.HeaderFlags = ltx.HeaderFlagNoChecksum
	if err := c.Compact(context.Background()); err != nil {
		t.Fatalf("compact L0 chain: %v", err)
	}

	var image bytes.Buffer
	dec := ltx.NewDecoder(&compacted)
	if err := dec.DecodeDatabaseTo(&image); err != nil {
		t.Fatalf("decode compacted chain: %v", err)
	}
	if got := dec.Header().MaxTXID; got != maxTXID {
		t.Fatalf("L0 chain rebuilt to %s, want %s", got, maxTXID)
	}
	return image.Bytes()
}
