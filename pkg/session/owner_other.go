//go:build !unix

package session

import "os"

// keepOwner does nothing where files have no owner and group in the Unix
// sense.
func keepOwner(f *os.File, existing os.FileInfo) error {
	return nil
}
