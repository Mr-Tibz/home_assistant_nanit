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
	"time"

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
	store.session.AuthToken = "access"
	store.session.RefreshToken = "refresh"
	require.NoError(t, store.save())

	loaded := InitSessionStore(store.Filename)

	assert.Equal(t, "access", loaded.session.AuthToken)
	assert.Equal(t, "refresh", loaded.session.RefreshToken)
}

// A shorter session must replace a longer one completely. The file used to
// be overwritten from its start without being cut, which left the end of
// the older content behind.
func TestSaveReplacesALongerFile(t *testing.T) {
	store := newTestStore(t)
	store.session.AuthToken = "a-very-long-access-token-that-makes-the-file-longer"
	require.NoError(t, store.save())

	store.session.AuthToken = "short"
	require.NoError(t, store.save())

	assert.Equal(t, "short", readSession(t, store.Filename).AuthToken)
}

func TestSaveCreatesAPrivateFileAndLeavesNoTemporaryFile(t *testing.T) {
	store := newTestStore(t)
	store.session.RefreshToken = "refresh"
	require.NoError(t, store.save())

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

	store.session.RefreshToken = "refresh"
	require.NoError(t, store.save())

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

	store.session.RefreshToken = "refresh"
	require.NoError(t, store.save())

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

	store.session.RefreshToken = "refresh"
	require.NoError(t, store.save())

	info, err := os.Lstat(store.Filename)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the session path is still a symbolic link")
	assert.Equal(t, "refresh", readSession(t, realFile).RefreshToken)
	assert.False(t, store.inPlace, "the next save can replace the file in one step")

	// Now that the file exists, it is replaced like any other
	store.session.RefreshToken = "refresh-2"
	require.NoError(t, store.save())
	assert.Equal(t, "refresh-2", readSession(t, realFile).RefreshToken)
	assert.Equal(t, []string{"real-session.json"}, fileNames(t, realDir))
}

// Temporary files of a run that was ended in the middle of a save hold
// tokens. The next start removes them and leaves other files alone, also
// files with a similar name that somebody made by hand.
func TestInitRemovesStaleTemporaryFiles(t *testing.T) {
	store := newTestStore(t)
	store.session.RefreshToken = "refresh"
	require.NoError(t, store.save())

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

	assert.Equal(t, "refresh", loaded.session.RefreshToken)
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

// leftover writes a temporary file as a save leaves it when the run ends
// between the write and the rename.
func leftover(t *testing.T, filename string, number string, content string) string {
	name := filename + tempFileMark + number
	require.NoError(t, os.WriteFile(name, []byte(content), 0600))
	return name
}

func sessionJSON(t *testing.T, change func(s *Session)) string {
	s := &Session{Revision: Revision}
	change(s)

	data, err := json.Marshal(s)
	require.NoError(t, err)

	return string(data)
}

// A run that was ended between the write of the temporary file and the
// rename leaves the newest session in the temporary file, possibly with a
// refresh token that the server has already put in the place of the old
// one. The next start completes the save.
func TestInitCompletesASaveThatWasCutShort(t *testing.T) {
	saved := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	store := newTestStore(t)
	store.session.RefreshToken = "refresh-1"
	store.session.AuthTime = saved
	require.NoError(t, store.save())

	leftover(t, store.Filename, "123", sessionJSON(t, func(s *Session) {
		s.AuthToken = "access-2"
		s.RefreshToken = "refresh-2"
		s.AuthTime = saved.Add(time.Hour)
	}))

	loaded := InitSessionStore(store.Filename)

	assert.Equal(t, "refresh-2", loaded.Snapshot().RefreshToken)
	assert.Equal(t, "refresh-2", readSession(t, store.Filename).RefreshToken)
	assert.Equal(t, []string{"session.json"}, fileNames(t, filepath.Dir(store.Filename)))
}

// Without a session file, a complete leftover is the session.
func TestInitUsesACompleteLeftoverWhenThereIsNoSessionFile(t *testing.T) {
	store := newTestStore(t)
	leftover(t, store.Filename, "123", sessionJSON(t, func(s *Session) { s.RefreshToken = "refresh-2" }))

	loaded := InitSessionStore(store.Filename)

	assert.Equal(t, "refresh-2", loaded.Snapshot().RefreshToken)
	assert.Equal(t, []string{"session.json"}, fileNames(t, filepath.Dir(store.Filename)))
}

// Of several leftovers, the one with the newest authorization counts.
func TestInitUsesTheNewestLeftover(t *testing.T) {
	saved := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	store := newTestStore(t)
	store.session.RefreshToken = "refresh-1"
	store.session.AuthTime = saved
	require.NoError(t, store.save())

	for number, hours := range map[string]int{"1": 2, "2": 3, "3": 1} {
		leftover(t, store.Filename, number, sessionJSON(t, func(s *Session) {
			s.RefreshToken = fmt.Sprintf("refresh-after-%d-hours", hours)
			s.AuthTime = saved.Add(time.Duration(hours) * time.Hour)
		}))
	}

	loaded := InitSessionStore(store.Filename)

	assert.Equal(t, "refresh-after-3-hours", loaded.Snapshot().RefreshToken)
	assert.Equal(t, []string{"session.json"}, fileNames(t, filepath.Dir(store.Filename)))
}

// A leftover that cannot be the newest session is removed, and the
// session file stays as it is.
func TestInitRemovesALeftoverThatIsNotTheNewestSession(t *testing.T) {
	saved := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	newer := sessionJSON(t, func(s *Session) {
		s.RefreshToken = "refresh-2"
		s.AuthTime = saved.Add(time.Hour)
	})

	for name, content := range map[string]string{
		"cut off": newer[:len(newer)/2],
		"empty":   "",
		"an older authorization": sessionJSON(t, func(s *Session) {
			s.RefreshToken = "refresh-0"
			s.AuthTime = saved.Add(-time.Hour)
		}),
		"another revision":      strings.Replace(newer, fmt.Sprintf(`"revision":%d`, Revision), `"revision":1`, 1),
		"more than one session": newer + newer,
	} {
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			store.session.RefreshToken = "refresh-1"
			store.session.AuthTime = saved
			require.NoError(t, store.save())

			leftover(t, store.Filename, "123", content)

			loaded := InitSessionStore(store.Filename)

			assert.Equal(t, "refresh-1", loaded.Snapshot().RefreshToken)
			assert.Equal(t, "refresh-1", readSession(t, store.Filename).RefreshToken)
			assert.Equal(t, []string{"session.json"}, fileNames(t, filepath.Dir(store.Filename)))
		})
	}
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

	store.session.RefreshToken = "refresh"
	err := store.save()
	assert.True(t, errors.Is(err, fs.ErrPermission), "got: %v", err)
}

// A cause that lies in the place of the file does not pass, so the
// rewrite in place is kept until the program ends. Every other cause can
// pass, and must not have that effect.
func TestWhichFailuresLieInThePlaceOfTheFile(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EACCES, syscall.EPERM, syscall.EROFS, syscall.EBUSY, syscall.EXDEV, syscall.ENAMETOOLONG, syscall.ENOTSUP} {
		err := &os.LinkError{Op: "rename", Old: "session.json.tmp-1", New: "session.json", Err: errno}
		assert.True(t, placeRefuses(err), "%v lies in the place of the file", errno)
	}

	for _, errno := range []syscall.Errno{syscall.ENOSPC, syscall.EMFILE, syscall.ENFILE, syscall.EIO, syscall.EDQUOT, syscall.ENOENT} {
		err := &os.PathError{Op: "open", Path: "session.json.tmp-1", Err: errno}
		assert.False(t, placeRefuses(err), "%v does not lie in the place of the file", errno)
	}
}

// failCreateTemp makes the creation of the temporary file fail with errno
// until the test ends.
func failCreateTemp(t *testing.T, errno syscall.Errno) {
	original := createTemp
	t.Cleanup(func() { createTemp = original })

	createTemp = func(dir string, pattern string) (*os.File, error) {
		return nil, &os.PathError{Op: "open", Path: filepath.Join(dir, pattern), Err: errno}
	}
}

// On a full disk no temporary file can be made. The file is then
// rewritten in place, if the new content fits in the space that the file
// already has: that needs no new space, and not saving would lose a
// refresh token that the server has replaced. The next save tries the
// temporary file again.
func TestAFullDiskLeadsToTheRewriteInPlaceForOneSave(t *testing.T) {
	store := newTestStore(t)
	store.session.RefreshToken = "first"
	require.NoError(t, store.save())

	original := createTemp
	failCreateTemp(t, syscall.ENOSPC)

	// As long as the first one: the content fits whatever the file
	// system reports about the space of the file
	store.session.RefreshToken = "other"
	require.NoError(t, store.save())

	assert.Equal(t, "other", readSession(t, store.Filename).RefreshToken)
	assert.False(t, store.inPlace, "a full disk can pass")

	// The check at the start passes on a full disk too
	require.NoError(t, store.checkWritable())

	createTemp = original

	store.session.RefreshToken = "third"
	require.NoError(t, store.save())

	assert.Equal(t, "third", readSession(t, store.Filename).RefreshToken)
	assert.False(t, store.inPlace)
}

// Content that needs more space than the file has is not written in
// place on a full disk: the write could stop half-way and damage the
// file. The file stays as it is, and the failure is reported.
func TestAFullDiskDoesNotRewriteInPlaceWhatDoesNotFit(t *testing.T) {
	store := newTestStore(t)
	store.session.RefreshToken = "first"
	require.NoError(t, store.save())

	failCreateTemp(t, syscall.ENOSPC)

	store.session.RefreshToken = strings.Repeat("x", 1<<20)
	err := store.save()

	assert.True(t, errors.Is(err, syscall.ENOSPC), "got: %v", err)
	assert.Equal(t, "first", readSession(t, store.Filename).RefreshToken, "the file is as it was")
	assert.False(t, store.inPlace)
}

// A new file cannot be made on a full disk.
func TestAFullDiskDoesNotCreateTheFileInPlace(t *testing.T) {
	store := newTestStore(t)
	failCreateTemp(t, syscall.ENOSPC)

	assert.Error(t, store.checkWritable())
	assert.Empty(t, fileNames(t, filepath.Dir(store.Filename)))
}

// When neither way works, the failure is reported, and the next save
// starts with the temporary file again.
func TestAFailureThatCanPassIsNotRemembered(t *testing.T) {
	store := newTestStore(t)
	store.session.RefreshToken = "first"
	require.NoError(t, store.save())

	// The directory of the session file is gone: no file can be created
	// or opened there (ENOENT)
	dir := filepath.Dir(store.Filename)
	moved := dir + "-moved"
	require.NoError(t, os.Rename(dir, moved))

	store.session.RefreshToken = "second"
	assert.Error(t, store.save())
	assert.False(t, store.inPlace)

	// It is back: the save works again, through a temporary file
	require.NoError(t, os.Rename(moved, dir))
	require.NoError(t, store.save())

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

	last := store.Snapshot()
	stored := readSession(t, store.Filename)
	assert.Equal(t, last.AuthToken, stored.AuthToken, "the file holds the last change")
	assert.Equal(t, last.RefreshToken, stored.RefreshToken)
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
	store.session.RefreshToken = "first"
	require.NoError(t, store.save())

	dir := filepath.Dir(store.Filename)
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { os.Chmod(dir, 0700) })

	store.session.RefreshToken = "second"
	require.NoError(t, store.save())

	assert.Equal(t, "second", readSession(t, store.Filename).RefreshToken)
	assert.True(t, store.inPlace, "the following saves do not try a temporary file again")

	store.session.RefreshToken = "third"
	require.NoError(t, store.save())

	assert.Equal(t, "third", readSession(t, store.Filename).RefreshToken)
}
