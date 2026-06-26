package litestream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

const restoreDownloadChunkSize = 8 * 1024 * 1024

const restoreDownloadMaxRetries = 3

type restoreDownloadPlanStats struct {
	FileCount      int
	TotalSize      int64
	MinSize        int64
	MaxSize        int64
	AvgSize        int64
	DownloadChunks int64
	MinTXID        ltx.TXID
	MaxTXID        ltx.TXID
	LevelCounts    string
	LevelSizes     string
}

type restoreDownloadPlanLevelStats struct {
	count int
	size  int64
}

func newRestoreDownloadPlanStats(infos []*ltx.FileInfo, chunkSize int64) restoreDownloadPlanStats {
	if len(infos) == 0 {
		return restoreDownloadPlanStats{}
	}
	if chunkSize < 1 {
		chunkSize = restoreDownloadChunkSize
	}

	levelStats := make(map[int]restoreDownloadPlanLevelStats)
	stats := restoreDownloadPlanStats{
		FileCount: len(infos),
		MinSize:   infos[0].Size,
		MaxSize:   infos[0].Size,
		MinTXID:   infos[0].MinTXID,
		MaxTXID:   infos[0].MaxTXID,
	}
	for _, info := range infos {
		stats.TotalSize += info.Size
		stats.MinSize = min(stats.MinSize, info.Size)
		stats.MaxSize = max(stats.MaxSize, info.Size)
		stats.DownloadChunks += (info.Size + chunkSize - 1) / chunkSize
		if info.MinTXID < stats.MinTXID {
			stats.MinTXID = info.MinTXID
		}
		if info.MaxTXID > stats.MaxTXID {
			stats.MaxTXID = info.MaxTXID
		}

		level := levelStats[info.Level]
		level.count++
		level.size += info.Size
		levelStats[info.Level] = level
	}
	stats.AvgSize = stats.TotalSize / int64(stats.FileCount)
	stats.LevelCounts, stats.LevelSizes = formatRestoreDownloadPlanLevels(levelStats)
	return stats
}

func formatRestoreDownloadPlanLevels(levelStats map[int]restoreDownloadPlanLevelStats) (counts, sizes string) {
	levels := make([]int, 0, len(levelStats))
	for level := range levelStats {
		levels = append(levels, level)
	}
	sort.Ints(levels)

	var countBuilder, sizeBuilder strings.Builder
	for i, level := range levels {
		if i > 0 {
			countBuilder.WriteByte(',')
			sizeBuilder.WriteByte(',')
		}
		stats := levelStats[level]
		fmt.Fprintf(&countBuilder, "L%d=%d", level, stats.count)
		fmt.Fprintf(&sizeBuilder, "L%d=%d", level, stats.size)
	}
	return countBuilder.String(), sizeBuilder.String()
}

func downloadRestoreFiles(ctx context.Context, client ReplicaClient, infos []*ltx.FileInfo, parallelism int, chunkSize int64, dir string, logger *slog.Logger) (_ []io.Reader, err error) {
	if parallelism < 1 {
		parallelism = 1
	}
	if chunkSize < 1 {
		chunkSize = restoreDownloadChunkSize
	}
	if logger == nil {
		logger = slog.Default()
	}

	files := make([]*restoreDownloadFile, len(infos))
	defer func() {
		if err != nil {
			closeRestoreDownloadFiles(files)
		}
	}()

	for i, info := range infos {
		f, err := os.CreateTemp(dir, ".litestream-restore-*.ltx")
		if err != nil {
			return nil, fmt.Errorf("create temp ltx file: %w", err)
		}
		files[i] = &restoreDownloadFile{File: f, path: f.Name()}

		if err := f.Truncate(info.Size); err != nil {
			return nil, fmt.Errorf("truncate temp ltx file: %w", err)
		}
	}

	jobs := make(chan restoreDownloadJob)
	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < parallelism; i++ {
		g.Go(func() error {
			for job := range jobs {
				if err := downloadRestoreChunk(ctx, client, job, logger); err != nil {
					return err
				}
			}
			return nil
		})
	}

	g.Go(func() error {
		defer close(jobs)

		for i, info := range infos {
			for offset := int64(0); offset < info.Size; offset += chunkSize {
				size := min(chunkSize, info.Size-offset)
				select {
				case jobs <- restoreDownloadJob{info: info, file: files[i], offset: offset, size: size}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	readers := make([]io.Reader, len(files))
	for i, f := range files {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek temp ltx file: %w", err)
		}
		readers[i] = f
	}
	return readers, nil
}

type restoreDownloadJob struct {
	info   *ltx.FileInfo
	file   *restoreDownloadFile
	offset int64
	size   int64
}

type restoreDownloadFile struct {
	*os.File
	path string
}

func (f *restoreDownloadFile) Close() error {
	return errors.Join(f.File.Close(), os.Remove(f.path))
}

func closeRestoreReaders(readers []io.Reader) {
	for _, rd := range readers {
		if closer, ok := rd.(io.Closer); ok {
			_ = closer.Close()
		}
	}
}

func closeRestoreDownloadFiles(files []*restoreDownloadFile) {
	for _, f := range files {
		if f != nil {
			_ = f.Close()
		}
	}
}

func downloadRestoreChunk(ctx context.Context, client ReplicaClient, job restoreDownloadJob, logger *slog.Logger) error {
	var lastErr error
	for attempt := 0; attempt <= restoreDownloadMaxRetries; attempt++ {
		if err := downloadRestoreChunkOnce(ctx, client, job); err == nil {
			return nil
		} else if errors.Is(err, os.ErrNotExist) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return err
		} else {
			lastErr = err
			logger.Debug("download ltx chunk failed, retrying",
				"level", job.info.Level, "min", job.info.MinTXID, "max", job.info.MaxTXID,
				"offset", job.offset, "size", job.size, "error", err, "attempt", attempt+1)
		}
	}
	return fmt.Errorf("max retries exceeded downloading ltx file chunk (level=%d, min=%s, max=%s, offset=%d, size=%d): %w",
		job.info.Level, job.info.MinTXID, job.info.MaxTXID, job.offset, job.size, lastErr)
}

func downloadRestoreChunkOnce(ctx context.Context, client ReplicaClient, job restoreDownloadJob) error {
	rc, err := client.OpenLTXFile(ctx, job.info.Level, job.info.MinTXID, job.info.MaxTXID, job.offset, job.size)
	if err != nil {
		return fmt.Errorf("open ltx file chunk: %w", err)
	}

	n, copyErr := io.CopyN(io.NewOffsetWriter(job.file, job.offset), rc, job.size)
	closeErr := rc.Close()
	if copyErr != nil {
		return fmt.Errorf("read ltx file chunk: %w", copyErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close ltx file chunk: %w", closeErr)
	}
	if n != job.size {
		return fmt.Errorf("read ltx file chunk: %w", io.ErrUnexpectedEOF)
	}
	return nil
}
