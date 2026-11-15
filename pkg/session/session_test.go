package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *Store {
	store := NewSessionStore()
	store.Filename = filepath.Join(t.TempDir(), "session.json")
	return store
}

func readSession(t *testing.T, filename string) Session {
	data, err := os.ReadFile(filename)
	require.NoError(t, err)

	var stored Session
	require.NoError(t, json.Unmarshal(data, &stored), "the file must hold exactly one JSON value: %s", data)

	return stored
}

func fileNames(t *testing.T, dir string) []string {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}

	return names
}

func TestSaveAndLoad(t *testing.T) {
	store := newTestStore(t)
	store.Session.AuthToken = "access"
	store.Session.RefreshToken = "refresh"
	require.NoError(t, store.Save())

	loaded := InitSessionStore(store.Filename)

	assert.Equal(t, "access", loaded.Session.AuthToken)
	assert.Equal(t, "refresh", loaded.Session.RefreshToken)
}

// A shorter session must replace a longer one completely. The file used to
// be overwritten from its start without being cut, which left the end of
// the older content behind.
func TestSaveReplacesALongerFile(t *testing.T) {
	store := newTestStore(t)
	store.Session.AuthToken = "a-very-long-access-token-that-makes-the-file-longer"
	require.NoError(t, store.Save())

	store.Session.AuthToken = "short"
	require.NoError(t, store.Save())

	assert.Equal(t, "short", readSession(t, store.Filename).AuthToken)
}

func TestSaveCreatesAPrivateFileAndLeavesNoTemporaryFile(t *testing.T) {
	store := newTestStore(t)
	store.Session.RefreshToken = "refresh"
	require.NoError(t, store.Save())

	info, err := os.Stat(store.Filename)
	require.NoError(t, err)
	assert.Equal(t, newSessionFileMode, info.Mode().Perm(), "a new session file holds tokens and is private")

	assert.Equal(t, []string{"session.json"}, fileNames(t, filepath.Dir(store.Filename)))
}

// A file that exists keeps its mode. Its owner may rely on it.
func TestSaveKeepsTheModeOfAnExistingFile(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, os.WriteFile(store.Filename, []byte(`{"revision":3}`), 0640))
	require.NoError(t, os.Chmod(store.Filename, 0640))

	store.Session.RefreshToken = "refresh"
	require.NoError(t, store.Save())

	info, err := os.Stat(store.Filename)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0640), info.Mode().Perm())
	assert.Equal(t, "refresh", readSession(t, store.Filename).RefreshToken)
}

// A session file that is a symbolic link stays a link, and the file it
// points to gets the new content.
func TestSaveFollowsASymbolicLink(t *testing.T) {
	realDir := t.TempDir()
	realFile := filepath.Join(realDir, "real-session.json")
	require.NoError(t, os.WriteFile(realFile, []byte(`{"revision":3}`), 0600))

	store := newTestStore(t)
	require.NoError(t, os.Symlink(realFile, store.Filename))

	store.Session.RefreshToken = "refresh"
	require.NoError(t, store.Save())

	info, err := os.Lstat(store.Filename)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the session path is still a symbolic link")

	assert.Equal(t, "refresh", readSession(t, realFile).RefreshToken)
	assert.Equal(t, []string{"real-session.json"}, fileNames(t, realDir), "no temporary file is left next to the real file")
}

// A link to a file that does not exist yet: the save creates that file
// and leaves the link in place. A rename over the link would put the
// tokens in the directory of the link.
func TestSaveCreatesTheFileBehindADanglingLink(t *testing.T) {
	realDir := t.TempDir()
	realFile := filepath.Join(realDir, "real-session.json")

	store := newTestStore(t)
	require.NoError(t, os.Symlink(realFile, store.Filename))

	store.Session.RefreshToken = "refresh"
	require.NoError(t, store.Save())

	info, err := os.Lstat(store.Filename)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the session path is still a symbolic link")
	assert.Equal(t, "refresh", readSession(t, realFile).RefreshToken)
	assert.False(t, store.inPlace, "the next save can replace the file in one step")

	// Now that the file exists, it is replaced like any other
	store.Session.RefreshToken = "refresh-2"
	require.NoError(t, store.Save())
	assert.Equal(t, "refresh-2", readSession(t, realFile).RefreshToken)
	assert.Equal(t, []string{"real-session.json"}, fileNames(t, realDir))
}

// Temporary files of a run that was ended in the middle of a save hold
// tokens. The next start removes them and leaves other files alone, also
// files with a similar name that somebody made by hand.
func TestInitRemovesStaleTemporaryFiles(t *testing.T) {
	store := newTestStore(t)
	store.Session.RefreshToken = "refresh"
	require.NoError(t, store.Save())

	dir := filepath.Dir(store.Filename)

	// A real leftover: made the way a save makes its temporary file
	leftover, err := os.CreateTemp(dir, "session.json"+tempFileMark+"*")
	require.NoError(t, err)
	require.NoError(t, leftover.Close())

	keep := []string{"other.json", "session.json.tmp-backup", "session.json.tmp-", "session.json.bak", "my-session.json.tmp-123"}
	for _, name := range keep {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0600))
	}
	require.NoError(t, os.Mkdir(filepath.Join(dir, "session.json.tmp-456"), 0700))

	loaded := InitSessionStore(store.Filename)

	assert.Equal(t, "refresh", loaded.Session.RefreshToken)
	assert.ElementsMatch(t, append(keep, "session.json", "session.json.tmp-456"), fileNames(t, dir))
}

// Characters that have a meaning in a file name pattern are part of the
// name here, nothing more.
func TestInitRemovesStaleTemporaryFilesInADirectoryWithSpecialCharacters(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "[nanit] *?")
	require.NoError(t, os.Mkdir(dir, 0700))

	filename := filepath.Join(dir, "session.json")
	require.NoError(t, os.WriteFile(filename+tempFileMark+"123", []byte("{}"), 0600))

	InitSessionStore(filename)

	assert.Equal(t, []string{"session.json"}, fileNames(t, dir), "the leftover is gone, and the start has created the session file")
}

// The start finds out whether the session file can be written, with a
// save that leaves the content as it is.
func TestCheckWritableKeepsTheContent(t *testing.T) {
	store := newTestStore(t)
	content := []byte(`{"revision":1,"somethingOfAnotherVersion":true}`)
	require.NoError(t, os.WriteFile(store.Filename, content, 0600))

	require.NoError(t, store.checkWritable())

	data, err := os.ReadFile(store.Filename)
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestCheckWritableCreatesAMissingFile(t *testing.T) {
	store := newTestStore(t)

	require.NoError(t, store.checkWritable())

	assert.Equal(t, Revision, readSession(t, store.Filename).Revision)
}

// A place that cannot be written is reported, by the check at the start
// and by every save. The program used to end at the first save. Now only
// the check at the start ends it.
func TestAPlaceThatCannotBeWrittenIsReported(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	store := newTestStore(t)
	dir := filepath.Dir(store.Filename)
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { os.Chmod(dir, 0700) })

	assert.Error(t, store.checkWritable())

	store.Session.RefreshToken = "refresh"
	err := store.Save()
	assert.True(t, errors.Is(err, fs.ErrPermission), "got: %v", err)
}

// Only a cause that lies in the place of the file leads to the rewrite in
// place. A cause that can pass, or that says the disk is in trouble, must
// not: the rewrite in place would then be kept until the program ends,
// and on a full disk it could damage the file that is still good.
func TestWhichFailuresLeadToTheRewriteInPlace(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EPERM, syscall.EROFS, syscall.EBUSY, syscall.EXDEV, syscall.ENOTSUP} {
		err := refusedByPlace(&os.LinkError{Op: "rename", Old: "session.json.tmp-1", New: "session.json", Err: errno})

		var cannotReplace *cannotReplaceError
		assert.True(t, errors.As(err, &cannotReplace), "%v lies in the place of the file", errno)
	}

	for _, errno := range []syscall.Errno{syscall.ENOSPC, syscall.EMFILE, syscall.ENFILE, syscall.EIO, syscall.EDQUOT, syscall.ENOENT} {
		cause := &os.PathError{Op: "open", Path: "session.json.tmp-1", Err: errno}
		err := refusedByPlace(cause)

		var cannotReplace *cannotReplaceError
		assert.False(t, errors.As(err, &cannotReplace), "%v does not lie in the place of the file", errno)
		assert.Same(t, cause, err)
	}
}

// A failure that does not lie in the place of the file is reported, and
// the next save tries the temporary file again.
func TestAFailureThatCanPassDoesNotLeadToTheRewriteInPlace(t *testing.T) {
	store := newTestStore(t)
	store.Session.RefreshToken = "first"
	require.NoError(t, store.Save())

	// The directory of the session file is gone: no temporary file can
	// be created there (ENOENT)
	dir := filepath.Dir(store.Filename)
	moved := dir + "-moved"
	require.NoError(t, os.Rename(dir, moved))

	store.Session.RefreshToken = "second"
	assert.Error(t, store.Save())
	assert.False(t, store.inPlace)

	// It is back: the save works again, through a temporary file
	require.NoError(t, os.Rename(moved, dir))
	require.NoError(t, store.Save())

	assert.Equal(t, "second", readSession(t, store.Filename).RefreshToken)
	assert.False(t, store.inPlace)
}

func TestSyncUnsupported(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EINVAL, syscall.ENOTSUP, syscall.ENOSYS} {
		err := &os.PathError{Op: "sync", Path: "session.json", Err: errno}
		assert.True(t, syncUnsupported(err), "%v means that the file system cannot sync", errno)
	}

	for _, errno := range []syscall.Errno{syscall.EIO, syscall.ENOSPC} {
		err := &os.PathError{Op: "sync", Path: "session.json", Err: errno}
		assert.False(t, syncUnsupported(err), "%v is a failed write", errno)
	}
}

// Several goroutines change the session during the run. Every change must
// reach the file, and the file must be complete at every moment.
func TestChangesAtTheSameMoment(t *testing.T) {
	store := newTestStore(t)

	const writers = 6
	const rounds = 10

	var group sync.WaitGroup
	for writer := 0; writer < writers; writer++ {
		group.Add(1)
		go func(writer int) {
			defer group.Done()

			for round := 0; round < rounds; round++ {
				err := store.Update(func(s *Session) {
					s.AuthToken = fmt.Sprintf("access-%d-%d", writer, round)
					s.RefreshToken = fmt.Sprintf("refresh-%d-%d", writer, round)
				})
				assert.NoError(t, err)

				seen := store.Snapshot()
				assert.Equal(t, strings.TrimPrefix(seen.AuthToken, "access-"), strings.TrimPrefix(seen.RefreshToken, "refresh-"), "a snapshot holds the two tokens of one change")
			}
		}(writer)
	}

	group.Wait()

	stored := readSession(t, store.Filename)
	assert.Equal(t, store.Session.AuthToken, stored.AuthToken, "the file holds the last change")
	assert.Equal(t, store.Session.RefreshToken, stored.RefreshToken)
	assert.Equal(t, []string{"session.json"}, fileNames(t, filepath.Dir(store.Filename)))
}

// A file that cannot be replaced by a rename is rewritten in place and cut
// to the new length. The new content is written before the file is cut,
// so the file is never empty in between.
func TestWriteFileInPlace(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "session.json")
	require.NoError(t, os.WriteFile(filename, []byte("0123456789"), 0644))

	require.NoError(t, writeFileInPlace(filename, []byte("abc")))

	data, err := os.ReadFile(filename)
	require.NoError(t, err)
	assert.Equal(t, "abc", string(data))

	info, err := os.Stat(filename)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0644), info.Mode().Perm(), "an existing file keeps its mode")
}

// When no temporary file can be created next to the session file, the
// save still succeeds by rewriting the file in place.
func TestSaveFallsBackToInPlaceWhenTheDirectoryIsReadOnly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a read-only directory")
	}

	store := newTestStore(t)
	store.Session.RefreshToken = "first"
	require.NoError(t, store.Save())

	dir := filepath.Dir(store.Filename)
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { os.Chmod(dir, 0700) })

	store.Session.RefreshToken = "second"
	require.NoError(t, store.Save())

	assert.Equal(t, "second", readSession(t, store.Filename).RefreshToken)
	assert.True(t, store.inPlace, "the following saves do not try a temporary file again")

	store.Session.RefreshToken = "third"
	require.NoError(t, store.Save())

	assert.Equal(t, "third", readSession(t, store.Filename).RefreshToken)
}
