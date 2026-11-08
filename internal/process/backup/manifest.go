package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/yakugate/yakugate/internal/platform/version"
)

// ManifestName is the integrity file of one backup run. It is written LAST,
// by temp file + rename, so a run directory without it is an incomplete run:
// the role was stopped mid-dump, or the disk filled. [SCOPE P8 S8-05]
const ManifestName = "MANIFEST.json"

// manifestFormat is bumped when the layout of a run changes in a way an older
// restore tool could not read.
const manifestFormat = 1

// ManifestFile is one file of the run. Path is relative to the run directory
// and always written with forward slashes.
type ManifestFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Rows   *int64 `json:"rows,omitempty"` // data files only (<table>.jsonl.gz)
}

// Manifest describes one complete backup run.
type Manifest struct {
	Format          int            `json:"format"`
	Run             string         `json:"run"`
	GatewayVersion  string         `json:"gateway_version"`
	GatewayCommit   string         `json:"gateway_commit"`
	StartedAt       time.Time      `json:"started_at"`
	FinishedAt      time.Time      `json:"finished_at"`
	DatabasesFailed []string       `json:"databases_failed,omitempty"` // a whole store that could not be dumped
	ExcludedTables  []string       `json:"excluded_tables,omitempty"`  // "<db>.<table>" left out (dump or seal failed)
	Files           []ManifestFile `json:"files"`
}

// Whole says the run holds every table of every database it was asked to
// back up (2026-10-18, go-live P8 B1/E3). A run that left a table or a whole
// database out still gets its manifest — its files are good and it may be the
// best copy there is — but it is not a whole backup: it does not count as
// "the last complete backup", never counts towards YAKU_BACKUP_KEEP, never
// pushes a whole run out, and backup-verify does not pass it. Read from the
// two lists, so a run written before this day is judged the same way.
func (m *Manifest) Whole() bool {
	return m != nil && len(m.DatabasesFailed) == 0 && len(m.ExcludedTables) == 0
}

// File returns the entry for a relative path, or nil.
func (m *Manifest) File(rel string) *ManifestFile {
	for i := range m.Files {
		if m.Files[i].Path == rel {
			return &m.Files[i]
		}
	}
	return nil
}

// WriteManifest hashes every file under runDir and writes MANIFEST.json last,
// atomically. rows carries the row count of each data file by relative path.
func WriteManifest(runDir string, m Manifest, rows map[string]int64) (*Manifest, error) {
	paths, err := runFiles(runDir)
	if err != nil {
		return nil, err
	}
	m.Format = manifestFormat
	m.GatewayVersion, m.GatewayCommit = version.Version, version.Commit
	m.Files = make([]ManifestFile, 0, len(paths))
	for _, rel := range paths {
		sum, size, err := hashFile(filepath.Join(runDir, filepath.FromSlash(rel)))
		if err != nil {
			return nil, err
		}
		mf := ManifestFile{Path: rel, SHA256: sum, Bytes: size}
		if n, ok := rows[rel]; ok {
			mf.Rows = &n
		}
		m.Files = append(m.Files, mf)
	}
	m.FinishedAt = time.Now().UTC()
	body, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	tmp := filepath.Join(runDir, ManifestName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, fileMode)
	if err != nil {
		return nil, err
	}
	if _, err = f.Write(append(body, '\n')); err == nil {
		err = f.Sync() // on disk before the rename makes the run "complete"
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, filepath.Join(runDir, ManifestName))
	}
	if err != nil {
		os.Remove(tmp)
		return nil, err
	}
	return &m, nil
}

// ReadManifest loads the manifest of a run directory.
func ReadManifest(runDir string) (*Manifest, error) {
	body, err := os.ReadFile(filepath.Join(runDir, ManifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%s has no %s: the run is incomplete, or is older than the manifest", runDir, ManifestName)
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, fmt.Errorf("%s: %w", ManifestName, err)
	}
	if m.Format > manifestFormat {
		return nil, fmt.Errorf("%s is format %d; this gateway reads up to %d", ManifestName, m.Format, manifestFormat)
	}
	return &m, nil
}

// Complete reports whether a run directory carries its manifest: the run
// FINISHED. It may still have left tables out (Whole).
func Complete(runDir string) bool {
	st, err := os.Stat(filepath.Join(runDir, ManifestName))
	return err == nil && st.Mode().IsRegular()
}

// runState is what a run directory is: not finished, finished with something
// left out, or a whole backup.
type runState int

const (
	runIncomplete runState = iota // no manifest: cut short, or still being written
	runPartial                    // a manifest, but a table or a database was left out
	runWhole                      // a manifest, and nothing was left out
)

// stateOf reads a run directory's state. A manifest that cannot be read is a
// finished run that cannot be trusted: partial.
func stateOf(runDir string) runState {
	if !Complete(runDir) {
		return runIncomplete
	}
	if m, err := ReadManifest(runDir); err == nil && m.Whole() {
		return runWhole
	}
	return runPartial
}

// The modes of what a run writes (2026-10-18, go-live P8 A3). The config
// dump holds password hashes, API tokens and vendor logins in plain text, so
// a run is the gateway user's alone: folders 0700, files 0600. They were 0755
// and 0644, readable by anyone on the host who could reach the root.
const (
	dirMode  os.FileMode = 0o700
	fileMode os.FileMode = 0o600
)

// Mismatch is one file whose content no longer matches the manifest.
type Mismatch struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// VerifyReport is the outcome of checking a run against its manifest.
type VerifyReport struct {
	Files    int        `json:"files"` // files the manifest lists
	Bytes    int64      `json:"bytes"` // bytes re-hashed
	Missing  []string   `json:"missing,omitempty"`
	Extra    []string   `json:"extra,omitempty"`
	Mismatch []Mismatch `json:"mismatch,omitempty"`
}

// OK is true when the directory is exactly what the manifest says.
func (r VerifyReport) OK() bool { return len(r.Missing)+len(r.Extra)+len(r.Mismatch) == 0 }

// Problems is how many files are wrong.
func (r VerifyReport) Problems() int { return len(r.Missing) + len(r.Extra) + len(r.Mismatch) }

// String is the one-line summary the role logs.
func (r VerifyReport) String() string {
	if r.OK() {
		return fmt.Sprintf("ok: %d files, %d bytes match the manifest", r.Files, r.Bytes)
	}
	return fmt.Sprintf("FAILED: %d missing, %d extra, %d changed (of %d files)",
		len(r.Missing), len(r.Extra), len(r.Mismatch), r.Files)
}

// Verify re-hashes a run directory against its manifest. The error is for a
// run that cannot be checked at all (no manifest, unreadable directory); files
// that are missing, extra or changed are in the report.
func Verify(runDir string) (*Manifest, VerifyReport, error) {
	var rep VerifyReport
	m, err := ReadManifest(runDir)
	if err != nil {
		return nil, rep, err
	}
	rep.Files = len(m.Files)
	listed := make(map[string]bool, len(m.Files))
	for _, mf := range m.Files {
		listed[mf.Path] = true
		if !fs.ValidPath(mf.Path) { // no "..", no absolute path: stay inside the run
			rep.Mismatch = append(rep.Mismatch, Mismatch{mf.Path, "path leaves the run directory"})
			continue
		}
		sum, size, err := hashFile(filepath.Join(runDir, filepath.FromSlash(mf.Path)))
		switch {
		case os.IsNotExist(err):
			rep.Missing = append(rep.Missing, mf.Path)
		case err != nil:
			rep.Mismatch = append(rep.Mismatch, Mismatch{mf.Path, "unreadable: " + err.Error()})
		case size != mf.Bytes:
			rep.Mismatch = append(rep.Mismatch, Mismatch{mf.Path, fmt.Sprintf("size %d, manifest says %d", size, mf.Bytes)})
		case sum != mf.SHA256:
			rep.Mismatch = append(rep.Mismatch, Mismatch{mf.Path, "SHA-256 differs"})
		default:
			rep.Bytes += size
		}
	}
	onDisk, err := runFiles(runDir)
	if err != nil {
		return m, rep, err
	}
	for _, rel := range onDisk {
		if !listed[rel] {
			rep.Extra = append(rep.Extra, rel)
		}
	}
	return m, rep, nil
}

// runFiles lists every regular file under runDir except the manifest itself,
// as sorted slash-separated relative paths.
func runFiles(runDir string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(runDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(runDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == ManifestName {
			return nil
		}
		out = append(out, rel)
		return nil
	})
	sort.Strings(out)
	return out, err
}

func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
