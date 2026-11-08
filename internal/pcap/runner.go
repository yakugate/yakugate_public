package pcap

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Purpose names the gateway data-key family the files are sealed under.
const Purpose = "pcap"

// Windows are the only capture lengths allowed, in minutes (PO 2026-09-17).
var Windows = []int{1, 5, 15}

// ValidWindow reports whether minutes is one of Windows.
func ValidWindow(minutes int) bool {
	for _, w := range Windows {
		if w == minutes {
			return true
		}
	}
	return false
}

// Errors a caller can tell apart. Each message is plain enough to show an
// operator as it stands.
var (
	ErrNoKey     = errors.New("packet capture is refused: the gateway has no key of its own (YAKU_CRYPTO_KEK_REF), so the file could not be stored encrypted")
	ErrNoTcpdump = errors.New("packet capture is not available: tcpdump is not installed on the gateway host")
	ErrBusy      = errors.New("a packet capture is already running on this port")
	ErrRunning   = errors.New("the capture is still running; download it when the window has closed")
	ErrNotFound  = errors.New("the capture file is not on this gateway (it was deleted, or never written)")
)

// Sealer is the part of crypto.Service the runner needs.
type Sealer interface {
	HasDefaultKey() bool
	SealStream(ctx context.Context, purpose string, dst io.Writer, src io.Reader) (int64, error)
	OpenStream(ctx context.Context, dst io.Writer, src io.Reader) error
}

// Result is how one capture ended.
type Result struct {
	ID         int64
	PlainBytes int64 // pcap bytes captured
	FileBytes  int64 // sealed bytes on disk
	Truncated  bool  // stopped early at the size limit
	Err        error // non-nil: the capture failed and its file was removed
}

// Runner starts captures, seals them as they are produced, and deletes them
// when they are old. One Runner per process; its methods are safe to call
// from several goroutines.
type Runner struct {
	Dir        string        // where sealed files live
	Tcpdump    string        // command name or path; "" = "tcpdump"
	Iface      string        // capture interface; "" = "any"
	MaxBytes   int64         // stop a capture at this many pcap bytes; 0 = no limit
	Keep       time.Duration // delete a file this long after it was finished; 0 = 24h
	StartGrace time.Duration // how long Start waits to see tcpdump fail; 0 = 1s
	Sealer     Sealer
	OnDone     func(Result) // called once per capture that Start accepted

	// Command builds the capture command. nil = the real tcpdump. Tests put a
	// fake here. The command must be built with exec.CommandContext(ctx, …),
	// write pcap to stdout and stop on SIGINT.
	Command func(ctx context.Context, bin, iface string, port int) *exec.Cmd

	now func() time.Time

	mu      sync.Mutex
	running map[int64]*job
}

type job struct {
	port   int
	cancel context.CancelFunc
}

func (r *Runner) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *Runner) keep() time.Duration {
	if r.Keep > 0 {
		return r.Keep
	}
	return 24 * time.Hour
}

func (r *Runner) path(id int64) string {
	return filepath.Join(r.Dir, "pcap_"+strconv.FormatInt(id, 10)+".yks")
}

// Check reports why a capture could not be started right now, or nil. It is
// what Start checks first, exposed so the portal can say so before asking.
func (r *Runner) Check() error {
	if r.Sealer == nil || !r.Sealer.HasDefaultKey() {
		return ErrNoKey
	}
	if _, err := r.lookPath(); err != nil {
		return ErrNoTcpdump
	}
	return nil
}

func (r *Runner) lookPath() (string, error) {
	bin := r.Tcpdump
	if bin == "" {
		bin = "tcpdump"
	}
	return exec.LookPath(bin)
}

func defaultCommand(ctx context.Context, bin, iface string, port int) *exec.Cmd {
	// -n no name lookups, -U write each packet at once, -s 0 whole packets,
	// -w - pcap to stdout: the plain capture goes down a pipe, never to disk.
	return exec.CommandContext(ctx, bin, "-i", iface, "-n", "-U", "-s", "0", "-w", "-",
		"tcp", "port", strconv.Itoa(port))
}

// Start begins capturing port for window. It returns once tcpdump is seen to
// be running (or has failed to). The capture then ends by itself; OnDone
// reports how.
func (r *Runner) Start(id int64, port int, window time.Duration) error {
	if port <= 0 || port > 65535 || window <= 0 {
		return fmt.Errorf("packet capture needs a port and a window")
	}
	if err := r.Check(); err != nil {
		return err
	}
	bin, _ := r.lookPath()

	r.mu.Lock()
	if r.running == nil {
		r.running = map[int64]*job{}
	}
	for _, j := range r.running {
		if j.port == port {
			r.mu.Unlock()
			return ErrBusy
		}
	}
	if _, dup := r.running[id]; dup {
		r.mu.Unlock()
		return ErrBusy
	}
	ctx, cancel := context.WithTimeout(context.Background(), window)
	r.running[id] = &job{port: port, cancel: cancel}
	r.mu.Unlock()

	fail := func(err error) error {
		cancel()
		r.forget(id)
		os.Remove(r.path(id))
		return err
	}

	if err := os.MkdirAll(r.Dir, 0o700); err != nil {
		return fail(fmt.Errorf("packet capture folder cannot be used: %v", err))
	}
	file, err := os.OpenFile(r.path(id), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fail(fmt.Errorf("packet capture file cannot be created: %v", err))
	}

	build := r.Command
	if build == nil {
		build = defaultCommand
	}
	iface := r.Iface
	if iface == "" {
		iface = "any"
	}
	cmd := build(ctx, bin, iface, port)
	// End the window the way an operator would: Ctrl-C, so tcpdump flushes
	// what it holds. Killed only if it ignores that.
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = 5 * time.Second
	var stderr tailBuffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		file.Close()
		return fail(err)
	}
	if err := cmd.Start(); err != nil {
		file.Close()
		return fail(fmt.Errorf("tcpdump could not be started: %v", err))
	}

	done := make(chan Result, 1)
	go func() {
		src := &cappedReader{r: stdout, left: r.MaxBytes, unlimited: r.MaxBytes <= 0, onCap: cancel}
		plain, sealErr := r.Sealer.SealStream(context.Background(), Purpose, file, src)
		io.Copy(io.Discard, stdout) // after the size limit: let tcpdump finish
		waitErr := cmd.Wait()
		closeErr := file.Close()
		stoppedByUs := ctx.Err() != nil
		cancel()
		r.forget(id)

		res := Result{ID: id, PlainBytes: plain, Truncated: src.capped}
		switch {
		case waitErr != nil && !stoppedByUs:
			res.Err = fmt.Errorf("tcpdump stopped: %s", stderr.lastLine(waitErr))
		case closeErr != nil:
			res.Err = fmt.Errorf("the capture file could not be written: %v", closeErr)
		case sealErr != nil && !r.readable(id):
			// A read error after the window (the pipe closed under a slow
			// tcpdump) still leaves a properly closed file; a write error
			// (disk full) does not. Reading it back tells the two apart.
			res.Err = fmt.Errorf("the capture could not be stored encrypted: %v", sealErr)
		}
		if res.Err != nil {
			os.Remove(r.path(id))
		} else if st, err := os.Stat(r.path(id)); err == nil {
			res.FileBytes = st.Size()
		}
		done <- res
	}()

	grace := r.StartGrace
	if grace <= 0 {
		grace = time.Second
	}
	select {
	case res := <-done:
		if res.Err != nil {
			return res.Err // failed at once: most often "no permission"
		}
		r.report(res)
	case <-time.After(grace):
		go func() { r.report(<-done) }()
	}
	return nil
}

func (r *Runner) report(res Result) {
	if r.OnDone != nil {
		r.OnDone(res)
	}
}

func (r *Runner) forget(id int64) {
	r.mu.Lock()
	delete(r.running, id)
	r.mu.Unlock()
}

// Running reports whether this capture is being taken right now.
func (r *Runner) Running(id int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.running[id]
	return ok
}

// Stop ends a running capture now. What was captured so far is kept.
func (r *Runner) Stop(id int64) {
	r.mu.Lock()
	j := r.running[id]
	r.mu.Unlock()
	if j != nil {
		j.cancel()
	}
}

// Open writes the capture, decrypted, to w. Nothing plain is written to disk.
func (r *Runner) Open(ctx context.Context, id int64, w io.Writer) error {
	if r.Running(id) {
		return ErrRunning
	}
	f, err := os.Open(r.path(id))
	if err != nil {
		return ErrNotFound
	}
	defer f.Close()
	if r.Sealer == nil {
		return ErrNoKey
	}
	return r.Sealer.OpenStream(ctx, w, f)
}

// readable reports whether the sealed file opens from start to end.
func (r *Runner) readable(id int64) bool {
	f, err := os.Open(r.path(id))
	if err != nil {
		return false
	}
	defer f.Close()
	return r.Sealer.OpenStream(context.Background(), io.Discard, f) == nil
}

// Size is the sealed file's size on disk, or false when it is not here.
func (r *Runner) Size(id int64) (int64, bool) {
	st, err := os.Stat(r.path(id))
	if err != nil {
		return 0, false
	}
	return st.Size(), true
}

// Delete stops the capture if it is running and removes its file. It reports
// whether there was a file to remove.
func (r *Runner) Delete(id int64) bool {
	r.Stop(id)
	for i := 0; i < 70 && r.Running(id); i++ { // tcpdump gets WaitDelay to go
		time.Sleep(100 * time.Millisecond)
	}
	return os.Remove(r.path(id)) == nil
}

// Sweep deletes every finished capture file older than Keep and returns the
// ids it removed. The age is the FILE's own (it was last written when the
// window closed), so deletion needs no database and cannot be held up by one.
func (r *Runner) Sweep() []int64 {
	entries, err := os.ReadDir(r.Dir)
	if err != nil {
		return nil
	}
	cutoff := r.clock().Add(-r.keep())
	var gone []int64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasPrefix(name, "pcap_") || !strings.HasSuffix(name, ".yks") {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(name, "pcap_"), ".yks"), 10, 64)
		if err != nil || r.Running(id) {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		if os.Remove(filepath.Join(r.Dir, name)) == nil {
			gone = append(gone, id)
		}
	}
	return gone
}

// cappedReader passes bytes through until the size limit, then reports the
// end of the stream and asks for the capture to stop.
type cappedReader struct {
	r         io.Reader
	left      int64
	unlimited bool
	capped    bool
	onCap     func()
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if c.unlimited {
		return c.r.Read(p)
	}
	if c.left <= 0 {
		if !c.capped {
			c.capped = true
			c.onCap()
		}
		return 0, io.EOF
	}
	if int64(len(p)) > c.left {
		p = p[:c.left]
	}
	n, err := c.r.Read(p)
	c.left -= int64(n)
	return n, err
}

// tailBuffer keeps the last few KB of tcpdump's stderr, for the error message.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if len(t.buf) > 4096 {
		t.buf = t.buf[len(t.buf)-4096:]
	}
	return len(p), nil
}

// lastLine is the last thing tcpdump said (its last two lines: the refusal
// and its hint come as a pair), or the exit error when it said nothing.
// tcpdump's own words ("You don't have permission to perform this capture on
// that device") are the plainest message there is.
func (t *tailBuffer) lastLine(fallback error) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var lines []string
	for _, l := range strings.Split(string(t.buf), "\n") {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) == 0 {
		return fallback.Error()
	}
	if len(lines) > 2 {
		lines = lines[len(lines)-2:]
	}
	return strings.Join(lines, " ")
}
