package buildengine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newWorkspace 创建临时根目录与缓存目录。
func newWorkspace(t *testing.T) (root, cache string) {
	t.Helper()
	root = t.TempDir()
	cache = filepath.Join(t.TempDir(), "cache")
	return root, cache
}

// hookEngine 安装在测试日志中打印状态转换与缓存判定的钩子，并线程安全地记录事件。
func hookEngine(t *testing.T, e *Engine) (*[]Event, *[]CacheDecision) {
	t.Helper()
	var mu sync.Mutex
	var events []Event
	var decisions []CacheDecision
	e.OnEvent(func(ev Event) {
		t.Logf("[state] %-10s %s -> %s (attempt=%d) %s", ev.Task, ev.From, ev.To, ev.Attempt, ev.Reason)
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}).OnCacheDecision(func(d CacheDecision) {
		t.Logf("[cache] %-10s stage=%s hit=%-5v attempt=%d %s", d.Task, d.Stage, d.Hit, d.Attempt, d.Reason)
		mu.Lock()
		decisions = append(decisions, d)
		mu.Unlock()
	})
	return &events, &decisions
}

func TestBasicDAGExecution(t *testing.T) {
	root, cache := newWorkspace(t)
	e := NewEngine()
	evs, _ := hookEngine(t, e)

	cfg := Config{
		RootDir:        root,
		CacheDir:       cache,
		MaxConcurrency: 2,
		Tasks: []TaskSpec{
			makeValidTask("compile", "fetch"),
			makeValidTask("fetch"),
			makeValidTask("link", "compile"),
		},
	}
	rep := e.Build(context.Background(), cfg)
	if !rep.Succeeded() {
		t.Fatalf("build should succeed: %+v", rep.Err)
	}
	for _, name := range []string{"fetch", "compile", "link"} {
		res := rep.Results[name]
		if res.State != StateSucceeded {
			t.Fatalf("task %s state=%s", name, res.State)
		}
		if _, err := os.Stat(filepath.Join(root, "out", name+".txt")); err != nil {
			t.Fatalf("artifact of %s missing: %v", name, err)
		}
	}

	// 依赖顺序：fetch 先于 compile 进入 Running，compile 先于 link。
	firstRunning := map[string]int{}
	for i, ev := range *evs {
		if ev.To == StateRunning {
			if _, ok := firstRunning[ev.Task]; !ok {
				firstRunning[ev.Task] = i
			}
		}
	}
	if !(firstRunning["fetch"] < firstRunning["compile"] && firstRunning["compile"] < firstRunning["link"]) {
		t.Fatalf("dependency order violated: %v", firstRunning)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	root, _ := newWorkspace(t)
	var running int32
	var maxObserved int32

	e := NewEngine()
	e.Register("probe", RunnerFunc(func(ctx context.Context, rc RunContext) error {
		cur := atomic.AddInt32(&running, 1)
		for {
			old := atomic.LoadInt32(&maxObserved)
			if cur <= old || atomic.CompareAndSwapInt32(&maxObserved, old, cur) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		atomic.AddInt32(&running, -1)
		return nil
	}))

	tasks := []TaskSpec{}
	for i := 0; i < 6; i++ {
		tasks = append(tasks, TaskSpec{
			Name:    taskName(i),
			Tool:    "probe",
			Outputs: []string{}, // probe 不产产物
		})
	}
	cfg := Config{RootDir: root, MaxConcurrency: 2, Tasks: tasks}
	rep := e.Build(context.Background(), cfg)
	if !rep.Succeeded() {
		t.Fatalf("build failed: %v", rep.Err)
	}
	if got := atomic.LoadInt32(&maxObserved); got > 2 {
		t.Fatalf("max concurrency exceeded: observed %d > 2", got)
	}
	if got := atomic.LoadInt32(&maxObserved); got < 1 {
		t.Fatalf("probe runner never observed concurrency")
	}
}

func taskName(i int) string {
	return "t" + string(rune('a'+i))
}
