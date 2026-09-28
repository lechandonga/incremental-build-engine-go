package buildengine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testLogger 把任务状态转换与缓存判定打印到单测日志。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf(format, args...)
}

// writeArtifact 在暂存目录写入声明产物（自动创建嵌套父目录）。
func writeArtifact(rt *Runtime, name, content string) error {
	p := filepath.Join(rt.StagingDir(), filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	return os.WriteFile(p, []byte(content), 0o644)
}

func readArtifact(t *testing.T, res *BuildResult, task, artifact string) string {
	t.Helper()
	tr := res.Tasks[task]
	p := filepath.Join(tr.OutputDir, filepath.FromSlash(artifact))
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read artifact %s/%s: %v", task, artifact, err)
	}
	return string(b)
}

func threeTaskSpecs(runs *int32) []TaskSpec {
	return []TaskSpec{
		{Name: "compile", Produces: []ArtifactSpec{{Name: "compile.o", Kind: "bin"}},
			Inputs: map[string]string{"src": "v1"},
			Run: func(ctx context.Context, rt *Runtime) error {
				atomic.AddInt32(runs, 1)
				return writeArtifact(rt, "compile.o", "OBJ-v1")
			}},
		{Name: "link", DependsOn: []string{"compile"},
			Produces: []ArtifactSpec{{Name: "app", Kind: "bin"}},
			Consumes: []ArtifactRef{{Name: "compile.o", WantKind: "bin"}},
			Run: func(ctx context.Context, rt *Runtime) error {
				atomic.AddInt32(runs, 1)
				p, _ := rt.ArtifactPath("compile.o")
				b, err := os.ReadFile(p)
				if err != nil {
					return err
				}
				return writeArtifact(rt, "app", "APP-with-"+string(b))
			}},
	}
}

func TestBasicExecutionAndArtifacts(t *testing.T) {
	var runs int32
	e := NewEngine(WithLogger(testLogger{t}))
	res, err := e.Build(context.Background(), threeTaskSpecs(&runs))
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if runs != 2 {
		t.Fatalf("expected 2 task executions, got %d", runs)
	}
	if res.Status("compile") != StatusSuccess || res.Status("link") != StatusSuccess {
		t.Fatalf("unexpected statuses: compile=%s link=%s", res.Status("compile"), res.Status("link"))
	}
	if got := readArtifact(t, res, "link", "app"); got != "APP-with-OBJ-v1" {
		t.Fatalf("unexpected artifact content: %q", got)
	}
}

func TestMaxConcurrency(t *testing.T) {
	var inFlight, maxSeen int32
	var mu sync.Mutex
	mk := func(name string) TaskSpec {
		return TaskSpec{
			Name: name,
			Run: func(ctx context.Context, rt *Runtime) error {
				cur := atomic.AddInt32(&inFlight, 1)
				mu.Lock()
				if cur > maxSeen {
					maxSeen = cur
				}
				mu.Unlock()
				time.Sleep(30 * time.Millisecond)
				atomic.AddInt32(&inFlight, -1)
				return nil
			},
		}
	}
	specs := []TaskSpec{mk("t1"), mk("t2"), mk("t3"), mk("t4")}
	e := NewEngine(WithMaxConcurrency(2), WithLogger(testLogger{t}))
	res, err := e.Build(context.Background(), specs)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	for _, name := range []string{"t1", "t2", "t3", "t4"} {
		if res.Status(name) != StatusSuccess {
			t.Fatalf("task %s status=%s", name, res.Status(name))
		}
	}
	if maxSeen > 2 {
		t.Fatalf("concurrency limit violated: max in-flight=%d", maxSeen)
	}
	if maxSeen < 2 {
		t.Fatalf("expected tasks to actually run in parallel, max=%d", maxSeen)
	}
}

func TestFailureCascadesCancel(t *testing.T) {
	var downstreamRan int32
	specs := []TaskSpec{
		{Name: "boom", Run: func(ctx context.Context, rt *Runtime) error {
			return fmt.Errorf("kaboom")
		}},
		{Name: "downstream", DependsOn: []string{"boom"}, Run: func(ctx context.Context, rt *Runtime) error {
			atomic.AddInt32(&downstreamRan, 1)
			return nil
		}},
	}
	e := NewEngine(WithLogger(testLogger{t}))
	res, err := e.Build(context.Background(), specs)
	if err == nil {
		t.Fatal("expected build error")
	}
	if res.Status("boom") != StatusFailed {
		t.Fatalf("boom=%s", res.Status("boom"))
	}
	if res.Status("downstream") != StatusCanceled {
		t.Fatalf("downstream should be canceled, got %s", res.Status("downstream"))
	}
	if downstreamRan != 0 {
		t.Fatal("downstream must never run after dependency failure")
	}
	if d := res.Tasks["downstream"]; d.OutputDir != "" {
		t.Fatal("canceled task must not leave an artifact entry")
	}
}

func TestExternalCancelStopsWaitingTasks(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	specs := []TaskSpec{
		{Name: "slow", Run: func(ctx context.Context, rt *Runtime) error {
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}},
		{Name: "waiter", DependsOn: []string{"slow"}, Run: noopRun},
	}
	e := NewEngine(WithLogger(testLogger{t}))
	ctx, cancel := context.WithCancel(context.Background())
	var res *BuildResult
	var err error
	done := make(chan struct{})
	go func() {
		res, err = e.Build(ctx, specs)
		close(done)
	}()
	<-started
	cancel()
	close(release)
	<-done
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if st := res.Status("waiter"); st != StatusCanceled {
		t.Fatalf("waiter=%s", st)
	}
	if st := res.Status("slow"); st != StatusCanceled {
		t.Fatalf("slow=%s", st)
	}
}
