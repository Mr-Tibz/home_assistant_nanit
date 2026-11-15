package session

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/indiefan/home_assistant_nanit/pkg/baby"
)

// Revision - marks the version of the structure of a session file. Only files with equal revision will be loaded
// Note: you should increment this whenever you change the Session structure
const Revision = 3

// Session - application session data container
type Session struct {
	Revision            int         `json:"revision"`
	AuthToken           string      `json:"authToken"`
	AuthTime            time.Time   `json:"authTime"`
	Babies              []baby.Baby `json:"babies"`
	RefreshToken        string      `json:"refreshToken"`
	LastSeenMessageTime time.Time   `json:"lastSeenMessageTime"`
}

// Store - application session store context
type Store struct {
	Filename string
	Session  *Session
}

// NewSessionStore - constructor
func NewSessionStore() *Store {
	return &Store{
		Session: &Session{Revision: Revision},
	}
}

// Load - loads previous state from a file
func (store *Store) Load() {
	if _, err := os.Stat(store.Filename); os.IsNotExist(err) {
		log.Info().Str("filename", store.Filename).Msg("No app session file found")
		return
	}

	f, err := os.Open(store.Filename)
	if err != nil {
		log.Fatal().Str("filename", store.Filename).Err(err).Msg("Unable to open app session file")
	}

	defer f.Close()

	session := &Session{}
	jsonErr := json.NewDecoder(f).Decode(session)
	if jsonErr != nil {
		log.Fatal().Str("filename", store.Filename).Err(jsonErr).Msg("Unable to decode app session file")
	}

	if session.Revision == Revision {
		store.Session = session
		log.Info().Str("filename", store.Filename).Msg("Loaded app session from the file")
	} else {
		log.Warn().Str("filename", store.Filename).Msg("App session file contains older revision of the state, ignoring")
	}

}

// Save - stores current data in a file
//
// The data goes to a temporary file that then replaces the session file
// in one step. The session file is therefore always complete: a crash or
// a kill in the middle of a save cannot destroy the refresh token, which
// would force a new login.
func (store *Store) Save() {
	if store.Filename == "" {
		return
	}

	log.Trace().Str("filename", store.Filename).Msg("Storing app session to the file")

	data, jsonErr := json.Marshal(store.Session)
	if jsonErr != nil {
		log.Fatal().Str("filename", store.Filename).Err(jsonErr).Msg("Unable to marshal contents of app session file")
	}

	if err := writeFileAtomically(store.Filename, data, sessionFileMode); err != nil {
		// The session is still valid in memory. Ending the program here
		// would only lose the tokens we have just received.
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to store app session file. The session continues in memory, but the next start may need a new login")
	}
}

// sessionFileMode - the session file holds tokens that give full access
// to the account, so only its owner may read it.
const sessionFileMode os.FileMode = 0600

// writeFileAtomically writes data to a temporary file next to filename
// and renames it over filename.
//
// A file that is mounted on its own into a container cannot be replaced
// by a rename. In that case the file is rewritten in place.
func writeFileAtomically(filename string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(filename)

	tmp, err := os.CreateTemp(dir, filepath.Base(filename)+".tmp-*")
	if err != nil {
		return writeFileInPlace(filename, data, mode)
	}

	tmpName := tmp.Name()
	discard := func(cause error) error {
		tmp.Close()
		os.Remove(tmpName)
		return cause
	}

	if err := tmp.Chmod(mode); err != nil {
		return discard(err)
	}
	if _, err := tmp.Write(data); err != nil {
		return discard(err)
	}
	if err := tmp.Sync(); err != nil {
		return discard(err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}

	if err := os.Rename(tmpName, filename); err != nil {
		os.Remove(tmpName)
		return writeFileInPlace(filename, data, mode)
	}

	// Make the rename itself durable. Not every file system supports
	// syncing a directory, and the data is already in place, so a failure
	// here is not reported.
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}

	return nil
}

// writeFileInPlace replaces the content of filename without a temporary
// file. The file is cut to the new length, so no bytes of a longer, older
// content stay behind.
func writeFileInPlace(filename string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	return f.Close()
}

// InitSessionStore - Initializes new application session store
func InitSessionStore(sessionFile string) *Store {
	sessionStore := NewSessionStore()

	// Load previous state of the application from session file
	if sessionFile != "" {

		absFileName, filePathErr := filepath.Abs(sessionFile)
		if filePathErr != nil {
			log.Fatal().Str("path", sessionFile).Err(filePathErr).Msg("Unable to retrieve absolute file path")
		}

		sessionStore.Filename = absFileName
		sessionStore.Load()
	}

	return sessionStore
}
