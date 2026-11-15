//go:build unix

package session

import (
	"os"
	"syscall"
)

// keepOwner gives f the owner and group of the existing file. A file that
// replaces another one would otherwise belong to this process, and the
// former owner could lose access to it.
//
// Changing the owner needs privileges. When the owner is already right,
// nothing is done, so an unprivileged process does not fail here.
func keepOwner(f *os.File, existing os.FileInfo) error {
	want, ok := existing.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}

	info, err := f.Stat()
	if err != nil {
		return err
	}

	if have, ok := info.Sys().(*syscall.Stat_t); ok && have.Uid == want.Uid && have.Gid == want.Gid {
		return nil
	}

	return f.Chown(int(want.Uid), int(want.Gid))
}
