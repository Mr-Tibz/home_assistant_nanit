//go:build !unix

package session

import "os"

// keepOwner does nothing where files have no owner and group in the Unix
// sense.
func keepOwner(f *os.File, existing os.FileInfo) error {
	return nil
}

// allocatedSize returns the space that the file takes on the disk, in
// bytes, or 0 when that is not known.
func allocatedSize(info os.FileInfo) int64 {
	return 0
}
