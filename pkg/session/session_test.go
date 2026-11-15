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

func TestSaveAndLoad(t *testing.T) {
	store := newTestStore(t)
	store.Session.AuthToken = "access"
	store.Session.RefreshToken = "refresh"
	store.Save()

	loaded := NewSessionStore()
	loaded.Filename = store.Filename
	loaded.Load()

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

	data, err := os.ReadFile(store.Filename)
	require.NoError(t, err)

	var stored Session
	require.NoError(t, json.Unmarshal(data, &stored), "the file must hold exactly one JSON value: %s", data)
	assert.Equal(t, "short", stored.AuthToken)
}

func TestSaveKeepsTheFilePrivateAndLeavesNoTemporaryFile(t *testing.T) {
	store := newTestStore(t)
	require.NoError(t, os.WriteFile(store.Filename, []byte(`{"revision":3}`), 0644))

	store.Session.RefreshToken = "refresh"
	store.Save()

	info, err := os.Stat(store.Filename)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm(), "the session file holds tokens")

	entries, err := os.ReadDir(filepath.Dir(store.Filename))
	require.NoError(t, err)
	require.Len(t, entries, 1, "only the session file may be left")
	assert.Equal(t, "session.json", entries[0].Name())
}

// A file that cannot be replaced by a rename is rewritten in place and cut
// to the new length.
func TestWriteFileInPlaceCutsTheFile(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "session.json")
	require.NoError(t, os.WriteFile(filename, []byte("0123456789"), 0644))

	require.NoError(t, writeFileInPlace(filename, []byte("abc"), sessionFileMode))

	data, err := os.ReadFile(filename)
	require.NoError(t, err)
	assert.Equal(t, "abc", string(data))
}
