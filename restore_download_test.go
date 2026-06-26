package litestream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/superfly/ltx"
)

func TestDownloadRestoreFiles_SmallFilesUseParallelism(t *testing.T) {
	infos, data := newRestoreDownloadTestFiles(4, 4)
	delay := 40 * time.Millisecond

	serialClient := newRestoreDownloadTestClient(data, delay)
	serialStart := time.Now()
	serialReaders, err := downloadRestoreFiles(context.Background(), serialClient, infos, 1, 1024, t.TempDir(), restoreDownloadTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	serialElapsed := time.Since(serialStart)
	assertRestoreDownloadReaders(t, serialReaders, infos, data)

	parallelClient := newRestoreDownloadTestClient(data, delay)
	parallelStart := time.Now()
	parallelReaders, err := downloadRestoreFiles(context.Background(), parallelClient, infos, 4, 1024, t.TempDir(), restoreDownloadTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	parallelElapsed := time.Since(parallelStart)
	assertRestoreDownloadReaders(t, parallelReaders, infos, data)

	if serialClient.maxActiveOpen() != 1 {
		t.Fatalf("serial max active opens = %d, want 1", serialClient.maxActiveOpen())
	}
	if parallelClient.maxActiveOpen() < 2 {
		t.Fatalf("parallel max active opens = %d, want at least 2", parallelClient.maxActiveOpen())
	}
	if parallelElapsed*2 >= serialElapsed {
		t.Fatalf("parallel download took %s, serial took %s; want parallel at least 2x faster", parallelElapsed, serialElapsed)
	}
}

func TestDownloadRestoreFiles_LargeFileUsesParallelChunks(t *testing.T) {
	info := &ltx.FileInfo{Level: 0, MinTXID: 1, MaxTXID: 1, Size: 8}
	data := map[restoreDownloadTestKey][]byte{
		{level: info.Level, minTXID: info.MinTXID, maxTXID: info.MaxTXID}: []byte("abcdefgh"),
	}
	delay := 40 * time.Millisecond

	serialClient := newRestoreDownloadTestClient(data, delay)
	serialStart := time.Now()
	serialReaders, err := downloadRestoreFiles(context.Background(), serialClient, []*ltx.FileInfo{info}, 1, 2, t.TempDir(), restoreDownloadTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	serialElapsed := time.Since(serialStart)
	assertRestoreDownloadReaders(t, serialReaders, []*ltx.FileInfo{info}, data)

	parallelClient := newRestoreDownloadTestClient(data, delay)
	parallelStart := time.Now()
	parallelReaders, err := downloadRestoreFiles(context.Background(), parallelClient, []*ltx.FileInfo{info}, 4, 2, t.TempDir(), restoreDownloadTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	parallelElapsed := time.Since(parallelStart)
	assertRestoreDownloadReaders(t, parallelReaders, []*ltx.FileInfo{info}, data)

	if parallelClient.maxActiveOpen() < 2 {
		t.Fatalf("parallel max active opens = %d, want at least 2", parallelClient.maxActiveOpen())
	}
	if parallelElapsed*2 >= serialElapsed {
		t.Fatalf("parallel chunk download took %s, serial took %s; want parallel at least 2x faster", parallelElapsed, serialElapsed)
	}

	calls := parallelClient.openCalls()
	sort.Slice(calls, func(i, j int) bool { return calls[i].offset < calls[j].offset })
	if got, want := len(calls), 4; got != want {
		t.Fatalf("OpenLTXFile() calls = %d, want %d", got, want)
	}
	for i, call := range calls {
		if got, want := call.offset, int64(i*2); got != want {
			t.Fatalf("call[%d] offset = %d, want %d", i, got, want)
		}
		if got, want := call.size, int64(2); got != want {
			t.Fatalf("call[%d] size = %d, want %d", i, got, want)
		}
	}
}

func TestNewRestoreDownloadPlanStats(t *testing.T) {
	infos := []*ltx.FileInfo{
		{Level: SnapshotLevel, MinTXID: 1, MaxTXID: 10, Size: 10},
		{Level: 1, MinTXID: 11, MaxTXID: 20, Size: 17},
		{Level: 0, MinTXID: 21, MaxTXID: 21, Size: 5},
		{Level: 1, MinTXID: 22, MaxTXID: 30, Size: 9},
	}

	got := newRestoreDownloadPlanStats(infos, 8)
	want := restoreDownloadPlanStats{
		FileCount:      4,
		TotalSize:      41,
		MinSize:        5,
		MaxSize:        17,
		AvgSize:        10,
		DownloadChunks: 8,
		MinTXID:        1,
		MaxTXID:        30,
		LevelCounts:    "L0=1,L1=2,L9=1",
		LevelSizes:     "L0=5,L1=26,L9=10",
	}
	if got != want {
		t.Fatalf("stats=%#v, want %#v", got, want)
	}
}

func assertRestoreDownloadReaders(tb testing.TB, readers []io.Reader, infos []*ltx.FileInfo, data map[restoreDownloadTestKey][]byte) {
	tb.Helper()
	defer closeRestoreReaders(readers)

	for i, info := range infos {
		want := data[restoreDownloadTestKey{level: info.Level, minTXID: info.MinTXID, maxTXID: info.MaxTXID}]
		got, err := io.ReadAll(readers[i])
		if err != nil {
			tb.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			tb.Fatalf("reader[%d] = %q, want %q", i, got, want)
		}
	}
}

func newRestoreDownloadTestFiles(n, size int) ([]*ltx.FileInfo, map[restoreDownloadTestKey][]byte) {
	infos := make([]*ltx.FileInfo, n)
	data := make(map[restoreDownloadTestKey][]byte, n)
	for i := range infos {
		info := &ltx.FileInfo{
			Level:   0,
			MinTXID: ltx.TXID(i + 1),
			MaxTXID: ltx.TXID(i + 1),
		}
		b := bytes.Repeat([]byte{byte('a' + i)}, size)
		info.Size = int64(len(b))
		infos[i] = info
		data[restoreDownloadTestKey{level: info.Level, minTXID: info.MinTXID, maxTXID: info.MaxTXID}] = b
	}
	return infos, data
}

func restoreDownloadTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type restoreDownloadTestClient struct {
	data  map[restoreDownloadTestKey][]byte
	delay time.Duration

	mu        sync.Mutex
	active    int
	maxActive int
	calls     []restoreDownloadTestCall
}

func newRestoreDownloadTestClient(data map[restoreDownloadTestKey][]byte, delay time.Duration) *restoreDownloadTestClient {
	return &restoreDownloadTestClient{data: data, delay: delay}
}

func (c *restoreDownloadTestClient) Type() string { return "restore-download-test" }

func (c *restoreDownloadTestClient) Init(context.Context) error { return nil }

func (c *restoreDownloadTestClient) LTXFiles(context.Context, int, ltx.TXID, bool) (ltx.FileIterator, error) {
	return nil, errors.New("not implemented")
}

func (c *restoreDownloadTestClient) OpenLTXFile(ctx context.Context, level int, minTXID, maxTXID ltx.TXID, offset, size int64) (io.ReadCloser, error) {
	c.beginOpen(restoreDownloadTestCall{offset: offset, size: size})
	defer c.endOpen()

	if c.delay > 0 {
		timer := time.NewTimer(c.delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	b, ok := c.data[restoreDownloadTestKey{level: level, minTXID: minTXID, maxTXID: maxTXID}]
	if !ok {
		return nil, os.ErrNotExist
	}

	if offset > int64(len(b)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := int64(len(b))
	if size > 0 && offset+size < end {
		end = offset + size
	}
	return io.NopCloser(bytes.NewReader(b[offset:end])), nil
}

func (c *restoreDownloadTestClient) WriteLTXFile(context.Context, int, ltx.TXID, ltx.TXID, io.Reader) (*ltx.FileInfo, error) {
	return nil, errors.New("not implemented")
}

func (c *restoreDownloadTestClient) DeleteLTXFiles(context.Context, []*ltx.FileInfo) error {
	return errors.New("not implemented")
}

func (c *restoreDownloadTestClient) DeleteAll(context.Context) error {
	return errors.New("not implemented")
}

func (c *restoreDownloadTestClient) SetLogger(*slog.Logger) {}

func (c *restoreDownloadTestClient) beginOpen(call restoreDownloadTestCall) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.calls = append(c.calls, call)
}

func (c *restoreDownloadTestClient) endOpen() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
}

func (c *restoreDownloadTestClient) maxActiveOpen() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxActive
}

func (c *restoreDownloadTestClient) openCalls() []restoreDownloadTestCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]restoreDownloadTestCall(nil), c.calls...)
}

type restoreDownloadTestKey struct {
	level   int
	minTXID ltx.TXID
	maxTXID ltx.TXID
}

type restoreDownloadTestCall struct {
	offset int64
	size   int64
}
