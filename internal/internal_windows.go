//go:build windows
// +build windows

package internal

import (
	"os"
)

// Fileinfo returns syscall fields from a FileInfo object.
func Fileinfo(fi os.FileInfo) (uid, gid int) {
	return -1, -1
}

// fixRootDirectory is copied from the standard library for use with mkdirAll()
func fixRootDirectory(p string) string {
	if len(p) == len(`\\?\c:`) {
		if os.IsPathSeparator(p[0]) && os.IsPathSeparator(p[1]) && p[2] == '?' && os.IsPathSeparator(p[3]) && p[5] == ':' {
			return p + `\`
		}
	}
	return p
}

// FsyncDir is a no-op on Windows. Directory handles opened by os.Open() are
// read-only and FlushFileBuffers requires write access, so File.Sync() on a
// directory always fails with ERROR_ACCESS_DENIED. NTFS metadata updates such
// as renames are journaled, so there is no directory sync equivalent to call.
func FsyncDir(path string) error {
	return nil
}
