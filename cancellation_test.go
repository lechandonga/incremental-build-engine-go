package buildengine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// blockingRunner 注册一个先写出产物、再阻塞直到 ctx 取消的执行器，
// 用于验证运行中取消会中断执行并清理半成品产物。
func blockingRunner(started chan<- string) Runner {
	return RunnerFunc(func(ctx context.Context, rc RunContext) error {
		// 先写出声明产物（模拟执行到一半）。
		for _, out := range rc.Task.Outputs {
			target, err := safeJoin(rc.RootDir, out)
			if err != nil {
				return err
			}
			if err := atomicWriteFile(target, []byte("partial")); err != nil {
				return err
			}
		}
		started <- rc.Task.Name
		<-ctx.Done()
		return ctx.Err()
	})
}

func TestCancelRunningTaskCleansArtifactsAndCancelsDownstream(t *testing.T) {
	root, cache := newWorkspace(t)
	started := make(chan string, 4)
	e := NewEngine()
	e.Register("blocker", blockingRunner(started))
	evs, _ := hookEngine(t, e)

	a := makeValidTask("a")
	a.Tool = "blocker"
	b := makeValidTask("b", "a")
	c := makeValidTask("c", "b")
	d := makeValidTask("d") // 无关任务，应正常成功

	cfg := Config{RootDir: root, CacheDir: cache, MaxConcurrency: 4, Tasks: []TaskSpec{a, b, c, d}}
	h, err := e.Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := <-started; got != "a" {
		t.Fatalf("expected a to start, got %s", got)
	}
	if err := h.CancelTask("a"); err != nil {
		t.Fatal(err)
	}
	rep := h.Wait()

	if rep.Succeeded() {
		t.Fatal("build should not succeed after cancel")
	}
	for _, name := range []string{"a", "b", "c"} {
		if rep.Results[name].State != StateCanceled {
			t.Fatalf("%s should be Canceled, got %s", name, rep.Results[name].State)
		}
	}
	if rep.Results["d"].State != StateSucceeded {
		t.Fatalf("unrelated d should succeed, got %s", rep.Results["d"].State)
	}
	// b、c 从未运行。
	for _, ev := range *evs {
		if (ev.Task == "b" || ev.Task == "c") && ev.To == StateRunning {
			t.Fatalf("%s must never run after a was canceled", ev.Task)
		}
	}
	// a 的半成品产物必须被清理，不留下不确定产物。
	if _, err := os.Stat(filepath.Join(root, "out", "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("partial artifact of a must be removed, stat err=%v", err)
	}
}

func TestCancelWaitingTaskDoesNotAffectItsDependencies(t *testing.T) {
	root, cache := newWorkspace(t)
	started := make(chan string, 4)
	release := make(chan struct{})
	var once sync.Once

	e := NewEngine()
	e.Register("gate", RunnerFunc(func(ctx context.Context, rc RunContext) error {
		started <- rc.Task.Name
		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}
		// 用内置 file 执行器逻辑产出文件。
		return newFileRunner().Run(ctx, rc)
	}))
	evs, _ := hookEngine(t, e)

	a := makeValidTask("a")
	a.Tool = "gate"
	b := makeValidTask("b", "a") // 等待 a
	c := makeValidTask("c", "b") // 等待 b
	cfg := Config{RootDir: root, CacheDir: cache, MaxConcurrency: 4, Tasks: []TaskSpec{a, b, c}}

	h, err := e.Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := <-started; got != "a" {
		t.Fatalf("expected a, got %s", got)
	}
	// 在 a 仍在运行时取消等待中的 b，取消应传播到 c 但不影响 a。
	if err := h.CancelTask("b"); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	rep := h.Wait()

	if rep.Results["a"].State != StateSucceeded {
		t.Fatalf("a should still succeed, got %s", rep.Results["a"].State)
	}
	if rep.Results["b"].State != StateCanceled || rep.Results["c"].State != StateCanceled {
		t.Fatalf("b and c should be Canceled: b=%s c=%s", rep.Results["b"].State, rep.Results["c"].State)
	}
	// a 的产物完好。
	if _, err := os.Stat(filepath.Join(root, "out", "a.txt")); err != nil {
		t.Fatalf("a artifact missing: %v", err)
	}
	for _, ev := range *evs {
		if (ev.Task == "b" || ev.Task == "c") && ev.To == StateRunning {
			t.Fatalf("%s must never run", ev.Task)
		}
	}
}

func TestRootContextCancellation(t *testing.T) {
	root, _ := newWorkspace(t)
	started := make(chan string, 4)
	e := NewEngine()
	e.Register("blocker", blockingRunner(started))
	hookEngine(t, e)

	var tasks []TaskSpec
	for i := 0; i < 3; i++ {
		t := makeValidTask(taskName(i))
		t.Tool = "blocker"
		tasks = append(tasks, t)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h, err := e.Start(ctx, Config{RootDir: root, MaxConcurrency: 3, Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		<-started
	}
	cancel()
	rep := h.Wait()

	if rep.Err != context.Canceled {
		t.Fatalf("report error should be context.Canceled, got %v", rep.Err)
	}
	for i := 0; i < 3; i++ {
		name := taskName(i)
		if rep.Results[name].State != StateCanceled {
			t.Fatalf("%s should be Canceled, got %s", name, rep.Results[name].State)
		}
	}
}

func TestCancelDuringRetryBackoff(t *testing.T) {
	root, _ := newWorkspace(t)
	e := NewEngine()

	task := makeValidTask("flaky")
	task.MaxAttempts = 5
	task.Params = map[string]string{"fail_times": "5"}

	retrying := make(chan struct{}, 1)
	e.OnEvent(func(ev Event) {
		t.Logf("[state] %-10s %s -> %s (attempt=%d) %s", ev.Task, ev.From, ev.To, ev.Attempt, ev.Reason)
		if ev.Task == "flaky" && ev.To == StateRetrying {
			select {
			case retrying <- struct{}{}:
			default:
			}
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h, err := e.Start(ctx, Config{RootDir: root, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-retrying:
	case <-time.After(2 * time.Second):
		t.Fatal("task never entered retrying state")
	}
	if err := h.CancelTask("flaky"); err != nil {
		t.Fatal(err)
	}
	rep := h.Wait()
	if rep.Results["flaky"].State != StateCanceled {
		t.Fatalf("expected Canceled, got %s", rep.Results["flaky"].State)
	}
	if rep.Results["flaky"].Attempts != 1 {
		t.Fatalf("cancel during first backoff must prevent further attempts, got %d", rep.Results["flaky"].Attempts)
	}
}

func TestCancelFinishedTaskIsNoop(t *testing.T) {
	root, _ := newWorkspace(t)
	e := NewEngine()
	hookEngine(t, e)
	cfg := Config{RootDir: root, MaxConcurrency: 1, Tasks: []TaskSpec{makeValidTask("a")}}
	rep := e.Build(context.Background(), cfg)
	if !rep.Succeeded() {
		t.Fatal(rep.Err)
	}
	// 再启动一次构建并在结束后对句柄取消，必须是无副作用的 no-op。
	h, err := e.Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep2 := h.Wait()
	if !rep2.Succeeded() {
		t.Fatal(rep2.Err)
	}
	if err := h.CancelTask("a"); err != nil {
		t.Fatalf("canceling a finished task must be a no-op, got %v", err)
	}
}
