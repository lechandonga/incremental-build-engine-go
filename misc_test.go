package buildengine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEmptyConfigSucceeds(t *testing.T) {
	rep := NewEngine().Build(context.Background(), Config{})
	if !rep.Succeeded() {
		t.Fatalf("empty config should succeed, got %v", rep.Err)
	}
	if len(rep.Results) != 0 {
		t.Fatalf("empty config should have no results")
	}
}

func TestCacheDisabled(t *testing.T) {
	root, _ := newWorkspace(t)
	e := NewEngine()
	_, decs := hookEngine(t, e)
	task := makeValidTask("a")
	cfg := Config{RootDir: root, MaxConcurrency: 1, Tasks: []TaskSpec{task}} // 无 CacheDir
	r1 := e.Build(context.Background(), cfg)
	r2 := e.Build(context.Background(), cfg)
	if !r1.Succeeded() || !r2.Succeeded() {
		t.Fatalf("both builds should succeed: %v / %v", r1.Err, r2.Err)
	}
	if r1.Results["a"].Cached || r2.Results["a"].Cached {
		t.Fatalf("caching must be disabled")
	}
	for _, d := range *decs {
		t.Fatalf("no cache decisions expected when disabled, got %+v", d)
	}
}

// liarRunner 声称成功却不产出声明的产物。
func TestRunnerSuccessButArtifactMissingFails(t *testing.T) {
	root, cache := newWorkspace(t)
	e := NewEngine()
	hookEngine(t, e)
	e.Register("liar", RunnerFunc(func(ctx context.Context, rc RunContext) error {
		return nil // 什么都不写
	}))
	task := makeValidTask("a")
	task.Tool = "liar"
	rep := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, Tasks: []TaskSpec{task}})
	if rep.Succeeded() {
		t.Fatal("build must fail when declared artifacts are absent")
	}
	if rep.Results["a"].State != StateFailed {
		t.Fatalf("expected Failed, got %s", rep.Results["a"].State)
	}
	// 失败不得写缓存。
	if entries, _ := os.ReadDir(filepath.Join(cache, "v1")); len(entries) != 0 {
		t.Fatalf("no cache entry should exist after failure")
	}
}

func TestStateMachineTransitions(t *testing.T) {
	var got []Event
	m := newStateMachine("x", func(ev Event) { got = append(got, ev) })
	if m.current() != StatePending {
		t.Fatal("initial state must be Pending")
	}
	if !m.transition(StatePending, StateReady, "r") {
		t.Fatal("Pending -> Ready should be allowed")
	}
	if m.transition(StatePending, StateRunning, "r") {
		t.Fatal("transition requires exact current state")
	}
	if !m.transition(StateReady, StateRunning, "r") {
		t.Fatal("Ready -> Running should be allowed")
	}
	if !m.transition(StateRunning, StateRetrying, "r") {
		t.Fatal("Running -> Retrying should be allowed")
	}
	if !m.transition(StateRetrying, StateRunning, "r") {
		t.Fatal("Retrying -> Running should be allowed")
	}
	if !m.transition(StateRunning, StateSucceeded, "r") {
		t.Fatal("Running -> Succeeded should be allowed")
	}
	// 终态后任何迁移都失败。
	if m.transition(StateSucceeded, StateRunning, "r") {
		t.Fatal("terminal state cannot transition")
	}
	if changed, _ := m.cancel("c"); changed {
		t.Fatal("cancel cannot move a terminal state")
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 events, got %d", len(got))
	}
}

func TestStateMachineCancelRunningSettledByWorkerFlag(t *testing.T) {
	m := newStateMachine("x", nil)
	m.transition(StatePending, StateReady, "")
	m.transition(StateReady, StateRunning, "")
	changed, workerAlive := m.cancel("cancel")
	if !changed || !workerAlive {
		t.Fatalf("canceling a running task: changed=%v workerAlive=%v", changed, workerAlive)
	}
	if m.current() != StateCanceled {
		t.Fatalf("state=%s", m.current())
	}
	// 再次取消无效果。
	changed2, _ := m.cancel("again")
	if changed2 {
		t.Fatal("second cancel must be a no-op")
	}
}

func TestDuplicateDependencyEdgesDedup(t *testing.T) {
	root, _ := newWorkspace(t)
	a := makeValidTask("a")
	b := makeValidTask("b", "a", "a") // 重复声明同一依赖
	rep := NewEngine().Build(context.Background(), Config{RootDir: root, Tasks: []TaskSpec{a, b}})
	if !rep.Succeeded() {
		t.Fatalf("duplicate dep edges should be tolerated: %v", rep.Err)
	}
}

func TestUnusedSettleIsIdempotent(t *testing.T) {
	// 直接验证 settleTerminal 幂等性，通过一次取消传播 + 竞态取消调用模拟：
	root, _ := newWorkspace(t)
	started := make(chan string, 1)
	e := NewEngine()
	e.Register("b", blockingRunner(started))
	a := makeValidTask("a")
	a.Tool = "b"
	h, err := e.Start(context.Background(), Config{RootDir: root, MaxConcurrency: 1, Tasks: []TaskSpec{a}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// 重复取消同一个运行中任务不得 panic、不得重复结算。
	if err := h.CancelTask("a"); err != nil {
		t.Fatal(err)
	}
	if err := h.CancelTask("a"); err != nil {
		t.Fatal(err)
	}
	rep := h.Wait()
	if rep.Results["a"].State != StateCanceled {
		t.Fatalf("expected Canceled, got %s", rep.Results["a"].State)
	}
}

func TestMissingInputFileRunsWithMissingMarker(t *testing.T) {
	root, cache := newWorkspace(t)
	e := NewEngine()
	hookEngine(t, e)
	task := makeValidTask("a")
	task.Inputs = []string{"does-not-exist.txt"}
	rep := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, Tasks: []TaskSpec{task}})
	if !rep.Succeeded() {
		t.Fatalf("missing input should be part of digest (MISSING marker), not fatal: %v", rep.Err)
	}
	if _, err := os.Stat(filepath.Join(root, "out", "a.txt")); err != nil {
		t.Fatalf("artifact should still be produced: %v", err)
	}
}

func TestValidationErrorSentinelTypes(t *testing.T) {
	// 确保所有错误类型彼此不通过 errors.As 误匹配（扁平类型，无包装链）。
	var dup *DuplicateTaskError
	if errors.As(&MissingDependencyError{Task: "a", Dependency: "b"}, &dup) {
		t.Fatal("MissingDependency must not match DuplicateTask")
	}
	var cyc *CyclicDependencyError
	if errors.As(&IncompatibleArtifactError{}, &cyc) {
		t.Fatal("IncompatibleArtifact must not match CyclicDependency")
	}
}
