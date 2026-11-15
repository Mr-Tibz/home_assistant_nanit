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

	if err := writeFileAtomically(store.Filename, data); err != nil {
		// The session is still valid in memory. Ending the program here
		// would only lose the tokens we have just received.
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to store app session file. The session continues in memory, but the next start may need a new login")
	}
}

// newSessionFileMode - mode of a session file that this program creates.
// The file holds tokens that give full access to the account, so only its
// owner may read it. A file that already exists keeps its mode and owner.
const newSessionFileMode os.FileMode = 0600

// tempFilePattern - name pattern of the temporary files of filename
func tempFilePattern(filename string) string {
	return filepath.Base(filename) + ".tmp-*"
}

// writeFileAtomically replaces the content of filename in one step: it
// writes data to a temporary file in the same directory and renames that
// file over filename.
//
// If filename is a symbolic link, the file it points to is replaced and
// the link stays. A file that exists keeps its mode and its owner.
//
// The rename is not possible everywhere, for example when the file itself
// is mounted into a container. Whenever the temporary file cannot be
// created, prepared or renamed, the file is rewritten in place.
func writeFileAtomically(filename string, data []byte) error {
	target := filename
	if resolved, err := filepath.EvalSymlinks(filename); err == nil {
		target = resolved
	}

	existing, statErr := os.Stat(target)
	exists := statErr == nil

	dir := filepath.Dir(target)

	// os.CreateTemp creates the file with mode 0600
	tmp, err := os.CreateTemp(dir, tempFilePattern(target))
	if err != nil {
		return writeFileInPlace(target, data)
	}

	tmpName := tmp.Name()

	prepare := func() error {
		if _, err := tmp.Write(data); err != nil {
			return err
		}

		if exists {
			if err := tmp.Chmod(existing.Mode().Perm()); err != nil {
				return err
			}
			if err := keepOwner(tmp, existing); err != nil {
				return err
			}
		}

		return tmp.Sync()
	}

	prepareErr := prepare()
	closeErr := tmp.Close()

	if prepareErr != nil || closeErr != nil {
		os.Remove(tmpName)
		return writeFileInPlace(target, data)
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return writeFileInPlace(target, data)
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
// file. It writes the new content first and cuts the file to the new
// length afterwards, so the file is never empty in between.
func writeFileInPlace(filename string, data []byte) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, newSessionFileMode)
	if err != nil {
		return err
	}

	if _, err := f.WriteAt(data, 0); err != nil {
		f.Close()
		return err
	}
	if err := f.Truncate(int64(len(data))); err != nil {
		f.Close()
		return err
	}

	// Some file systems cannot sync. The content is written either way.
	f.Sync()

	return f.Close()
}

// removeStaleTempFiles deletes temporary files that an earlier run left
// behind when it was ended in the middle of a save. They hold tokens.
func removeStaleTempFiles(filename string) {
	target := filename
	if resolved, err := filepath.EvalSymlinks(filename); err == nil {
		target = resolved
	}

	stale, err := filepath.Glob(filepath.Join(filepath.Dir(target), tempFilePattern(target)))
	if err != nil {
		return
	}

	for _, name := range stale {
		if err := os.Remove(name); err == nil {
			log.Debug().Str("filename", name).Msg("Removed a temporary session file of an earlier run")
		}
	}
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
		removeStaleTempFiles(absFileName)
		sessionStore.Load()
	}

	return sessionStore
}
