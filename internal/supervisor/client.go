package supervisor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Client talks to a running supervisor's control port (the console's Gateway
// menu).
type Client struct {
	Addr string // host:port
	Key  string // YAKU_ADMIN_KEY, "" = none
	HTTP *http.Client
	// ConfigFile is the settings file the gateway reported at the last
	// Status ("" = environment variables only, or not known yet).
	ConfigFile string
}

// NewClient builds a client for addr ("" = the default address).
func NewClient(addr, key string) *Client {
	if addr == "" {
		addr = DefaultControlAddr
	}
	// Trimmed as the gateway trims it (guard), and as HTTP trims the header.
	return &Client{Addr: addr, Key: strings.TrimSpace(key), HTTP: &http.Client{Timeout: 60 * time.Second}}
}

// Status lists every role. An error means no gateway answered.
func (c *Client) Status(ctx context.Context) ([]RoleStatus, error) {
	var out struct {
		Roles      []RoleStatus `json:"roles"`
		ConfigFile string       `json:"config_file"`
	}
	if err := c.call(ctx, http.MethodGet, "/roles", &out); err != nil {
		return nil, err
	}
	c.ConfigFile = out.ConfigFile
	return out.Roles, nil
}

// Do runs start, stop or restart on one role or on "all".
func (c *Client) Do(ctx context.Context, role, action string) ([]RoleStatus, error) {
	var out struct {
		Roles []RoleStatus `json:"roles"`
	}
	if err := c.call(ctx, http.MethodPost, "/roles/"+role+"/"+action, &out); err != nil {
		return nil, err
	}
	return out.Roles, nil
}

// Shutdown stops every role and then the supervisor.
func (c *Client) Shutdown(ctx context.Context) error {
	return c.call(ctx, http.MethodPost, "/shutdown", nil)
}

// Running reports whether a supervisor answers on the control port.
func (c *Client) Running(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, err := c.Status(ctx)
	return err == nil
}

func (c *Client) call(ctx context.Context, method, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://"+c.Addr+path, bytes.NewReader(nil))
	if err != nil {
		return err
	}
	if c.Key != "" {
		req.Header.Set(KeyHeader, c.Key)
	}
	res, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("no gateway answers on %s: %w", c.Addr, err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if res.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &e)
		if e.Error == "" {
			e.Error = res.Status
		}
		return fmt.Errorf("%s", e.Error)
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("the gateway's answer could not be read: %w", err)
		}
	}
	return nil
}

// IdleFlag starts the gateway with every role stopped (`yakugate --idle`).
const IdleFlag = "--idle"

// LogFile is where a gateway started in the background writes (the console's
// Gateway menu and `yakugate --background`): the log folder's gateway.log, or
// ./logs/gateway.log when the log folder is not writable (a desk install
// without /var/log/yakugate). dir "" = /var/log/yakugate.
func LogFile(dir string) string {
	if dir == "" {
		dir = "/var/log/yakugate"
	}
	// A configured folder that does not exist yet is made, as the roles make
	// theirs — not swapped for ./logs while the roles write in the folder.
	_ = os.MkdirAll(dir, 0o755)
	if f, err := os.CreateTemp(dir, ".probe"); err == nil {
		name := f.Name()
		f.Close()
		os.Remove(name)
		return filepath.Join(dir, "gateway.log")
	}
	return filepath.Join("logs", "gateway.log")
}

// StartDetached starts the whole gateway (`yakugate [--config FILE]`) in the
// background, in its own session so it keeps running when the console that
// started it closes. Its output goes to logFile. It returns once the process
// has started; the caller waits for the control port with Running.
func StartDetached(exe, cfgPath, logFile string, env []string, idle bool) (int, error) {
	if err := os.MkdirAll(filepath.Dir(logFile), 0o755); err != nil {
		return 0, fmt.Errorf("log folder for %s: %w", logFile, err)
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, fmt.Errorf("gateway log %s: %w", logFile, err)
	}
	defer f.Close()
	var args []string
	if cfgPath != "" {
		args = append(args, "--config", cfgPath)
	}
	if idle {
		args = append(args, IdleFlag)
	}
	cmd := exec.Command(exe, args...)
	cmd.Stdout, cmd.Stderr, cmd.Stdin = f, f, nil
	cmd.Env = env
	detach(cmd)
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start the gateway: %w", err)
	}
	pid := cmd.Process.Pid
	_ = cmd.Process.Release() // not our child to wait for: it outlives the console
	return pid, nil
}
