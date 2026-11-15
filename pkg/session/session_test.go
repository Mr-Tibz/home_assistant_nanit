package session

import (
	"encoding/json"
	"os"
	"path/filepath"
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
	store.Save()

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
	store.Save()

	store.Session.AuthToken = "short"
	store.Save()

	assert.Equal(t, "short", readSession(t, store.Filename).AuthToken)
}

func TestSaveCreatesAPrivateFileAndLeavesNoTemporaryFile(t *testing.T) {
	store := newTestStore(t)
	store.Session.RefreshToken = "refresh"
	store.Save()

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
	store.Save()

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
	store.Save()

	info, err := os.Lstat(store.Filename)
	require.NoError(t, err)
	assert.NotZero(t, info.Mode()&os.ModeSymlink, "the session path is still a symbolic link")

	assert.Equal(t, "refresh", readSession(t, realFile).RefreshToken)
	assert.Equal(t, []string{"real-session.json"}, fileNames(t, realDir), "no temporary file is left next to the real file")
}

// Temporary files of a run that was ended in the middle of a save hold
// tokens. The next start removes them and leaves other files alone.
func TestInitRemovesStaleTemporaryFiles(t *testing.T) {
	store := newTestStore(t)
	store.Session.RefreshToken = "refresh"
	store.Save()

	dir := filepath.Dir(store.Filename)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "session.json.tmp-123"), []byte("{}"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "other.json"), []byte("{}"), 0600))

	loaded := InitSessionStore(store.Filename)

	assert.Equal(t, "refresh", loaded.Session.RefreshToken)
	assert.ElementsMatch(t, []string{"session.json", "other.json"}, fileNames(t, dir))
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
	store.Save()

	dir := filepath.Dir(store.Filename)
	require.NoError(t, os.Chmod(dir, 0500))
	t.Cleanup(func() { os.Chmod(dir, 0700) })

	store.Session.RefreshToken = "second"
	store.Save()

	assert.Equal(t, "second", readSession(t, store.Filename).RefreshToken)
}
