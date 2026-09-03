package litestream

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"testing"
)

// buildTestWAL returns a valid WAL of pageSize pages holding one transaction
// per element of txns. Each transaction writes the listed pages in order and
// its last frame carries the commit, with the database size set to the highest
// page number written so far. Checksums use the little-endian variant.
func buildTestWAL(t *testing.T, pageSize uint32, txns [][]uint32) []byte {
	t.Helper()

	const salt1, salt2 = 0xAABBCCDD, 0x11223344
	bo := binary.LittleEndian

	hdr := make([]byte, WALHeaderSize)
	binary.BigEndian.PutUint32(hdr[0:], 0x377f0682) // magic: little-endian checksums
	binary.BigEndian.PutUint32(hdr[4:], 3007000)    // format version
	binary.BigEndian.PutUint32(hdr[8:], pageSize)
	binary.BigEndian.PutUint32(hdr[12:], 0) // checkpoint sequence
	binary.BigEndian.PutUint32(hdr[16:], salt1)
	binary.BigEndian.PutUint32(hdr[20:], salt2)
	c0, c1 := WALChecksum(bo, 0, 0, hdr[:24])
	binary.BigEndian.PutUint32(hdr[24:], c0)
	binary.BigEndian.PutUint32(hdr[28:], c1)

	var buf bytes.Buffer
	buf.Write(hdr)

	var dbSize uint32
	for _, pgnos := range txns {
		for i, pgno := range pgnos {
			if pgno > dbSize {
				dbSize = pgno
			}
			var commit uint32
			if i == len(pgnos)-1 {
				commit = dbSize
			}

			fh := make([]byte, WALFrameHeaderSize)
			binary.BigEndian.PutUint32(fh[0:], pgno)
			binary.BigEndian.PutUint32(fh[4:], commit)
			binary.BigEndian.PutUint32(fh[8:], salt1)
			binary.BigEndian.PutUint32(fh[12:], salt2)
			data := bytes.Repeat([]byte{byte(pgno)}, int(pageSize))
			c0, c1 = WALChecksum(bo, c0, c1, fh[:8])
			c0, c1 = WALChecksum(bo, c0, c1, data)
			binary.BigEndian.PutUint32(fh[16:], c0)
			binary.BigEndian.PutUint32(fh[20:], c1)

			buf.Write(fh)
			buf.Write(data)
		}
	}
	return buf.Bytes()
}

// The WAL used below mirrors the shape reported in #1490: transaction A
// commits at frame 3, transaction B spans frames 4-6. With page size 4096 the
// end of frame 4 is offset 16512 and the end of frame 6 is 24752 — the exact
// "max offset 24752 > end offset 16512" pair from the report.
const pageMapTestPageSize = 4096

func pageMapTestWAL(t *testing.T) []byte {
	t.Helper()
	return buildTestWAL(t, pageMapTestPageSize, [][]uint32{{1, 2, 3}, {4, 5, 6}})
}

// endOfFrame returns the WAL offset just past the given 1-based frame.
func endOfFrame(frame int) int64 {
	return WALHeaderSize + int64(frame)*(WALFrameHeaderSize+pageMapTestPageSize)
}

func newPageMapTestReader(t *testing.T, wal []byte) *WALReader {
	t.Helper()
	r, err := NewWALReader(bytes.NewReader(wal), slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestWALReader_PageMapUntil(t *testing.T) {
	wal := pageMapTestWAL(t)

	assertPages := func(t *testing.T, m map[uint32]int64, want ...uint32) {
		t.Helper()
		if len(m) != len(want) {
			t.Fatalf("page map has %d pages, want %d: %v", len(m), len(want), m)
		}
		for _, pgno := range want {
			if _, ok := m[pgno]; !ok {
				t.Fatalf("page %d missing from page map: %v", pgno, m)
			}
		}
	}

	t.Run("BoundInsideTransaction", func(t *testing.T) {
		// The bound sits after frame 4, inside transaction B. B must be left
		// out entirely rather than read through to its commit at frame 6.
		m, maxOffset, commit, err := newPageMapTestReader(t, wal).pageMapUntil(context.Background(), endOfFrame(4))
		if err != nil {
			t.Fatal(err)
		}
		assertPages(t, m, 1, 2, 3)
		if got, want := maxOffset, endOfFrame(3); got != want {
			t.Fatalf("maxOffset=%d, want %d", got, want)
		}
		if got, want := commit, uint32(3); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		}
		if maxOffset > endOfFrame(4) {
			t.Fatalf("read exceeded bound: maxOffset=%d > %d", maxOffset, endOfFrame(4))
		}
	})

	t.Run("BoundAtCommit", func(t *testing.T) {
		m, maxOffset, commit, err := newPageMapTestReader(t, wal).pageMapUntil(context.Background(), endOfFrame(3))
		if err != nil {
			t.Fatal(err)
		}
		assertPages(t, m, 1, 2, 3)
		if got, want := maxOffset, endOfFrame(3); got != want {
			t.Fatalf("maxOffset=%d, want %d", got, want)
		}
		if got, want := commit, uint32(3); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		}
	})

	t.Run("BoundAtEnd", func(t *testing.T) {
		m, maxOffset, commit, err := newPageMapTestReader(t, wal).pageMapUntil(context.Background(), endOfFrame(6))
		if err != nil {
			t.Fatal(err)
		}
		assertPages(t, m, 1, 2, 3, 4, 5, 6)
		if got, want := maxOffset, endOfFrame(6); got != want {
			t.Fatalf("maxOffset=%d, want %d", got, want)
		}
		if got, want := commit, uint32(6); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		}
	})

	t.Run("BoundBeyondWAL", func(t *testing.T) {
		m, maxOffset, commit, err := newPageMapTestReader(t, wal).pageMapUntil(context.Background(), endOfFrame(10))
		if err != nil {
			t.Fatal(err)
		}
		assertPages(t, m, 1, 2, 3, 4, 5, 6)
		if got, want := maxOffset, endOfFrame(6); got != want {
			t.Fatalf("maxOffset=%d, want %d", got, want)
		}
		if got, want := commit, uint32(6); got != want {
			t.Fatalf("commit=%d, want %d", got, want)
		}
	})

	t.Run("BoundBeforeFirstCommit", func(t *testing.T) {
		m, maxOffset, commit, err := newPageMapTestReader(t, wal).pageMapUntil(context.Background(), endOfFrame(2))
		if err != nil {
			t.Fatal(err)
		}
		assertPages(t, m)
		if maxOffset != 0 || commit != 0 {
			t.Fatalf("maxOffset=%d commit=%d, want 0/0 for no complete transaction", maxOffset, commit)
		}
	})
}

// pageMap's byte limit is a batch size for the sync path and must always admit
// the transaction crossing it, or a transaction larger than the limit could
// never be synced. pageMapUntil deliberately does not share that property;
// this pins the sync semantics so a bound fix is not applied to the wrong
// function.
func TestWALReader_PageMap_AdmitsCrossingTransaction(t *testing.T) {
	wal := pageMapTestWAL(t)

	m, maxOffset, commit, limited, err := newPageMapTestReader(t, wal).pageMap(context.Background(), endOfFrame(4)-WALHeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	if !limited {
		t.Fatal("expected limited=true when the byte limit is reached")
	}
	if got, want := len(m), 6; got != want {
		t.Fatalf("page map has %d pages, want %d (crossing transaction must be admitted)", got, want)
	}
	if got, want := maxOffset, endOfFrame(6); got != want {
		t.Fatalf("maxOffset=%d, want %d", got, want)
	}
	if got, want := commit, uint32(6); got != want {
		t.Fatalf("commit=%d, want %d", got, want)
	}
}
