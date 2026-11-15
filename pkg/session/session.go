package session

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/indiefan/home_assistant_nanit/pkg/baby"
	"github.com/rs/zerolog/log"
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
//
// Several goroutines use the session during the run: every camera renews
// the token, and the message poll records what it has seen. Read the
// session with Snapshot and change it with Update. Using the fields of
// Session directly is safe only before those goroutines start.
type Store struct {
	Filename string
	Session  *Session

	// mu guards the fields of Session and the session file
	mu sync.Mutex

	// inPlace - the session file cannot be replaced by a rename here, so
	// every save rewrites it in place
	inPlace bool
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

// Snapshot returns a copy of the session as it is now.
func (store *Store) Snapshot() Session {
	store.mu.Lock()
	defer store.mu.Unlock()

	return *store.Session
}

// Update changes the session with change and stores the result in the
// file. It returns the error of Save.
func (store *Store) Update(change func(session *Session)) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	change(store.Session)

	return store.save()
}

// Save - stores current data in a file
//
// The data goes to a temporary file that then replaces the session file
// in one step. The session file is therefore always complete: a crash or
// a kill in the middle of a save cannot destroy the refresh token, which
// would force a new login.
//
// A failed save is logged here and returned. It does not end the program:
// the session is still valid in memory, and ending the program would only
// lose the tokens that have just been received.
func (store *Store) Save() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	return store.save()
}

// save does the work of Save. The caller holds store.mu.
func (store *Store) save() error {
	if store.Filename == "" {
		return nil
	}

	log.Trace().Str("filename", store.Filename).Msg("Storing app session to the file")

	data, jsonErr := json.Marshal(store.Session)
	if jsonErr != nil {
		log.Fatal().Str("filename", store.Filename).Err(jsonErr).Msg("Unable to marshal contents of app session file")
	}

	if err := store.write(data); err != nil {
		log.Error().Str("filename", store.Filename).Err(err).Msg("Unable to store app session file. The session continues in memory, but the next start may need a new login")
		return err
	}

	return nil
}

// write puts data in the session file. The caller holds store.mu.
func (store *Store) write(data []byte) error {
	if !store.inPlace {
		err := replaceFile(store.Filename, data)

		var cannotReplace *cannotReplaceError
		if !errors.As(err, &cannotReplace) {
			return err
		}

		// The cause lies in the place of the file and will not pass. Do
		// not go through a temporary file again at every save: it would
		// hold the tokens each time and help nothing.
		store.inPlace = true
		log.Warn().Str("filename", store.Filename).Err(cannotReplace.cause).Msg("The app session file cannot be replaced in one step in this place. It is rewritten in place until the program ends, and a crash in the middle of a save can damage it")
	}

	return writeFileInPlace(store.Filename, data)
}

// checkWritable does one save that leaves the content of the session file
// as it is, to find out whether saving works at all.
func (store *Store) checkWritable() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	data, err := os.ReadFile(store.Filename)
	if errors.Is(err, os.ErrNotExist) {
		// No file yet: create it, with the session as it is now
		data, err = json.Marshal(store.Session)
	}
	if err != nil {
		return err
	}

	return store.write(data)
}

// newSessionFileMode - mode of a session file that this program creates.
// The file holds tokens that give full access to the account, so only its
// owner may read it. A file that already exists keeps its mode and owner.
const newSessionFileMode os.FileMode = 0600

// tempFileMark - what follows the name of the session file in the name of
// its temporary files. A number follows the mark.
const tempFileMark = ".tmp-"

// cannotReplaceError - the session file cannot be replaced by a rename in
// this place. The data is not at fault, so the file can be rewritten in
// place.
type cannotReplaceError struct {
	cause error
}

func (e *cannotReplaceError) Error() string { return e.cause.Error() }
func (e *cannotReplaceError) Unwrap() error { return e.cause }

// placeRefuses reports whether err says that the place of the session
// file does not allow what replaceFile does there. These causes do not
// pass by themselves, and they say nothing against the file itself.
func placeRefuses(err error) bool {
	return errors.Is(err, fs.ErrPermission) || // the directory, or the mode or owner of the file, is not ours to change
		errors.Is(err, syscall.EROFS) || // the directory is on a read-only file system
		errors.Is(err, syscall.EBUSY) || // the file itself is mounted, for example into a container
		errors.Is(err, syscall.EXDEV) || // the file is on another file system than its directory
		errors.Is(err, errors.ErrUnsupported)
}

// refusedByPlace marks err as a cannotReplaceError when the place of the
// file is the cause. Every other failure, such as a full disk, too many
// open files or a read error, can pass or says that the disk is in
// trouble. It is returned as it is: the next save tries again, and the
// file that is still good is not touched.
func refusedByPlace(err error) error {
	if placeRefuses(err) {
		return &cannotReplaceError{err}
	}

	return err
}

// replaceFile replaces the content of filename in one step: it writes
// data to a temporary file in the same directory and renames that file
// over filename.
//
// If filename is a symbolic link, the file it points to is replaced and
// the link stays. A file that exists keeps its mode and its owner.
//
// The rename is not possible everywhere, for example when the file itself
// is mounted into a container. When the place does not allow the
// temporary file to be created, given the mode and owner, or renamed
// (see placeRefuses), the result is a cannotReplaceError. Every other
// failure is returned as it is, and the file that is still good is not
// touched.
func replaceFile(filename string, data []byte) error {
	target, err := filepath.EvalSymlinks(filename)
	if err != nil {
		if isSymlink(filename) {
			// A link to a file that does not exist yet. Opening the link
			// for writing creates that file. A rename would put a
			// regular file in the place of the link.
			return writeFileInPlace(filename, data)
		}

		target = filename
	}

	existing, statErr := os.Stat(target)
	exists := statErr == nil

	dir := filepath.Dir(target)

	// os.CreateTemp creates the file with mode 0600
	tmp, err := os.CreateTemp(dir, filepath.Base(target)+tempFileMark+"*")
	if err != nil {
		return refusedByPlace(err)
	}

	tmpName := tmp.Name()
	discard := func() {
		tmp.Close()
		os.Remove(tmpName)
	}

	if exists {
		if err := tmp.Chmod(existing.Mode().Perm()); err != nil {
			discard()
			return refusedByPlace(err)
		}
		if err := keepOwner(tmp, existing); err != nil {
			discard()
			return refusedByPlace(err)
		}
	}

	if _, err := tmp.Write(data); err != nil {
		discard()
		return err
	}
	if err := tmp.Sync(); err != nil && !syncUnsupported(err) {
		discard()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return refusedByPlace(err)
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
	if err := f.Sync(); err != nil && !syncUnsupported(err) {
		f.Close()
		return err
	}

	return f.Close()
}

// syncUnsupported reports whether err says that the file system cannot
// flush a file to the disk at all. Some network and virtual file systems
// cannot. That is not a failed write.
func syncUnsupported(err error) bool {
	return errors.Is(err, errors.ErrUnsupported) || errors.Is(err, syscall.EINVAL)
}

func isSymlink(filename string) bool {
	info, err := os.Lstat(filename)
	return err == nil && info.Mode()&os.ModeSymlink != 0
}

// isTempFileName reports whether name is the name of a temporary file of
// the session file sessionName: that name, the mark, and a number.
func isTempFileName(name string, sessionName string) bool {
	number, found := strings.CutPrefix(name, sessionName+tempFileMark)
	if !found || number == "" {
		return false
	}

	for _, char := range number {
		if char < '0' || char > '9' {
			return false
		}
	}

	return true
}

// removeStaleTempFiles deletes temporary files that an earlier run left
// behind when it was ended in the middle of a save. They hold tokens.
func removeStaleTempFiles(filename string) {
	target := filename
	if resolved, err := filepath.EvalSymlinks(filename); err == nil {
		target = resolved
	}

	dir := filepath.Dir(target)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if !entry.Type().IsRegular() || !isTempFileName(entry.Name(), filepath.Base(target)) {
			continue
		}

		name := filepath.Join(dir, entry.Name())
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

		// The server replaces the refresh token during the run, and the
		// new one must reach the file. If it cannot, say so now. Later
		// would be at the next start, when the token in the file is no
		// longer valid and a new login by hand is needed.
		if err := sessionStore.checkWritable(); err != nil {
			log.Fatal().Str("filename", absFileName).Err(err).Msg("The app session file cannot be written")
		}
	}

	return sessionStore
}
