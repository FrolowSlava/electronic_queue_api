package testutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Process представляет настоящий дочерний процесс cmd/api, а не httptest.Server.
type Process struct {
	URL             string
	ApplicationName string
	cmd             *exec.Cmd
	output          *processOutput
	done            chan struct{}
	waitErr         error
	stopOnce        sync.Once
	signalErr       error
}

var processSequence atomic.Uint64

func projectRoot() (string, error) {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("не удалось определить корень проекта")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", "..")), nil
}

// BuildAPI собирает бинарный файл во временном каталоге. cleanup вызывается
// только после остановки всех использующих этот файл процессов.
func BuildAPI(ctx context.Context, race bool) (binary string, cleanup func(), err error) {
	root, err := projectRoot()
	if err != nil {
		return "", nil, err
	}
	dir, err := os.MkdirTemp("", "electronic-queue-api-binary-")
	if err != nil {
		return "", nil, fmt.Errorf("создать каталог сборки API: %w", err)
	}
	cleanup = func() { _ = os.RemoveAll(dir) }
	binary = filepath.Join(dir, "queue-api")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	args := []string{"build"}
	if race {
		args = append(args, "-race")
	}
	args = append(args, "-o", binary, "./cmd/api")
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = root
	output, err := cmd.CombinedOutput()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("собрать API: %w\n%s", err, output)
	}
	return binary, cleanup, nil
}

// StartAPI использует ctx только для ожидания готовности. Успешно запущенный
// процесс продолжает работать после отмены этого контекста до Stop или Kill.
func StartAPI(ctx context.Context, binary, databaseURL string, extraEnv ...string) (*Process, error) {
	root, err := projectRoot()
	if err != nil {
		return nil, err
	}
	connectionURL, err := url.Parse(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("некорректный URL тестовой БД")
	}
	applicationName := fmt.Sprintf("eqapi_%d_%d", os.Getpid(), processSequence.Add(1))
	query := connectionURL.Query()
	query.Set("application_name", applicationName)
	connectionURL.RawQuery = query.Encode()
	settings := []string{
		"HTTP_ADDR=127.0.0.1:0", "DATABASE_URL=" + connectionURL.String(),
		"DOCS_DIR=" + root, "DB_MAX_CONNS=10",
		"REQUEST_TIMEOUT=20s", "SHUTDOWN_TIMEOUT=15s",
		"DB_STATEMENT_TIMEOUT=15s", "DB_LOCK_TIMEOUT=10s",
		// Race runtime обычно добавляет 1s при выходе. Эта тестовая задержка
		// не должна пересекаться с коротким общим deadline shutdown.
		"GORACE=" + os.Getenv("GORACE") + " atexit_sleep_ms=0",
	}
	settings = append(settings, extraEnv...)
	env, err := processEnvironment(settings)
	if err != nil {
		return nil, err
	}
	output := &processOutput{address: make(chan string, 1)}
	cmd := exec.Command(binary)
	cmd.Dir = root
	cmd.Env = env
	cmd.Stdout = output
	cmd.Stderr = output
	p := &Process{cmd: cmd, output: output, done: make(chan struct{}), ApplicationName: applicationName}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("запустить API: %w", err)
	}
	go func() {
		p.waitErr = cmd.Wait()
		// close служит широковещательным событием: несколько Stop могут ждать
		// один процесс. waitErr читается только после получения этого события.
		close(p.done)
	}()
	select {
	case address := <-output.address:
		p.URL = "http://" + address
	case <-p.done:
		return nil, fmt.Errorf("API завершился до запуска HTTP: %v\n%s", p.waitErr, p.Logs())
	case <-ctx.Done():
		p.Kill()
		return nil, fmt.Errorf("ожидание запуска API: %w\n%s", ctx.Err(), p.Logs())
	}
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"/healthz", nil)
		if err == nil {
			var response *http.Response
			response, err = client.Do(request)
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					return p, nil
				}
			}
		}
		select {
		case <-p.done:
			return nil, fmt.Errorf("API завершился до готовности: %v\n%s", p.waitErr, p.Logs())
		case <-ctx.Done():
			p.Kill()
			return nil, fmt.Errorf("ожидание готовности API: %w\n%s", ctx.Err(), p.Logs())
		case <-ticker.C:
		}
	}
}

// Stop посылает SIGTERM на Unix и ждёт полного завершения cmd.Wait.
// На Windows Signal(SIGTERM) не поддерживается, поэтому очистка использует Kill.
func (p *Process) Stop(ctx context.Context) error {
	select {
	case <-p.done:
		return p.exitError()
	default:
	}
	p.stopOnce.Do(func() {
		if runtime.GOOS == "windows" {
			p.signalErr = p.cmd.Process.Kill()
		} else {
			p.signalErr = p.cmd.Process.Signal(syscall.SIGTERM)
		}
	})
	if p.signalErr != nil && !errors.Is(p.signalErr, os.ErrProcessDone) {
		return fmt.Errorf("остановить API: %w", p.signalErr)
	}
	select {
	case <-p.done:
		return p.exitError()
	case <-ctx.Done():
		return fmt.Errorf("ожидание остановки API: %w", ctx.Err())
	}
}

func (p *Process) exitError() error {
	if p.waitErr == nil || runtime.GOOS == "windows" {
		return nil
	}
	return fmt.Errorf("API завершился с ошибкой: %w\n%s", p.waitErr, p.Logs())
}

// Kill используется как последняя страховка в cleanup после неуспешного Stop.
func (p *Process) Kill() {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	<-p.done
}

func (p *Process) Logs() string { return p.output.String() }

func processEnvironment(settings []string) ([]string, error) {
	values := make(map[string]string)
	for _, entry := range settings {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("настройка процесса должна иметь формат KEY=value")
		}
		values[strings.ToUpper(key)] = entry
	}
	result := make([]string, 0, len(os.Environ())+len(values))
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, overridden := values[strings.ToUpper(key)]; !overridden {
			result = append(result, entry)
		}
	}
	for _, entry := range values {
		result = append(result, entry)
	}
	return result, nil
}

type processOutput struct {
	mu      sync.Mutex
	all     bytes.Buffer
	pending []byte
	address chan string
	found   bool
}

func (output *processOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	_, _ = output.all.Write(data)
	if output.found {
		return len(data), nil
	}
	output.pending = append(output.pending, data...)
	for {
		newline := bytes.IndexByte(output.pending, '\n')
		if newline < 0 {
			break
		}
		var entry struct {
			Address string `json:"address"`
		}
		if json.Unmarshal(output.pending[:newline], &entry) == nil && entry.Address != "" {
			output.address <- entry.Address
			output.found = true
			output.pending = nil
			break
		}
		output.pending = output.pending[newline+1:]
	}
	return len(data), nil
}

func (output *processOutput) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.all.String()
}
