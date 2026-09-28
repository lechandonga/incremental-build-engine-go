package buildengine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// TestCombinedCancelRetryCache 组合场景：
//   - stableA：稳定任务（读取输入文件），首次执行后写缓存；
//   - flakyB：依赖 stableA，前两次失败、第三次成功（重试后成功）；
//   - flakyC：独立任务，在运行中被取消，其下游 cancelChain 一并取消；
//   - 取消与 flakyB 的重试并发发生，互不干扰；
//   - 相同输入重建：stableA 与 flakyB 全部命中缓存；
//   - 修改 stableA 的输入后重建：stableA 因输入变化重跑，
//     flakyB 因传递依赖键变化重跑，执行次数与缓存判定均可解释。
func TestCombinedCancelRetryCache(t *testing.T) {
	root, cache := newWorkspace(t)
	inputPath := filepath.Join(root, "input.txt")
	if err := os.WriteFile(inputPath, []byte("version-1"), 0o644); err != nil {
		t.Fatal(err)
	}

	var flakyBRuns int32
	var stableARuns int32
	startedC := make(chan string, 1)

	countingRunner := RunnerFunc(func(ctx context.Context, rc RunContext) error {
		switch rc.Task.Name {
		case "stableA":
			atomic.AddInt32(&stableARuns, 1)
		case "flakyB":
			n := atomic.AddInt32(&flakyBRuns, 1)
			// 仅在第一次构建的前两次尝试失败；后续重跑（第 3 次起）稳定成功。
			if n <= 2 {
				return fmt.Errorf("flakyB simulated failure #%d", n)
			}
		case "flakyC":
			startedC <- rc.Task.Name
			<-ctx.Done()
			return ctx.Err()
		}
		return newFileRunner().Run(ctx, rc)
	})

	e := NewEngine()
	e.OnEvent(func(ev Event) {
		t.Logf("[state] %-12s %s -> %s (attempt=%d) %s", ev.Task, ev.From, ev.To, ev.Attempt, ev.Reason)
	}).OnCacheDecision(func(d CacheDecision) {
		t.Logf("[cache] %-12s stage=%s hit=%-5v attempt=%d %s", d.Task, d.Stage, d.Hit, d.Attempt, d.Reason)
	})
	e.Register("counting-file", countingRunner)

	mk := func(name string, deps ...string) TaskSpec {
		return makeValidTask(name, deps...)
	}
	stableA := mk("stableA")
	stableA.Inputs = []string{"input.txt"}
	stableA.Tool = "counting-file"

	flakyB := mk("flakyB", "stableA")
	flakyB.MaxAttempts = 3
	flakyB.Tool = "counting-file"

	flakyC := mk("flakyC")
	flakyC.Tool = "counting-file"

	cancelChain := mk("cancelChain", "flakyC") // 内置 file 执行器

	cfg := Config{
		RootDir:        root,
		CacheDir:       cache,
		MaxConcurrency: 4,
		Tasks:          []TaskSpec{stableA, flakyB, flakyC, cancelChain},
	}
	cachedTasks := []TaskSpec{stableA, flakyB}

	h, err := e.Start(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := <-startedC; got != "flakyC" {
		t.Fatalf("expected flakyC started, got %s", got)
	}
	// 在 flakyB 第一次尝试失败、进入退避/重试窗口期间并发取消 flakyC。
	time.Sleep(3 * time.Millisecond)
	if err := h.CancelTask("flakyC"); err != nil {
		t.Fatal(err)
	}
	rep1 := h.Wait()

	// --- 首次构建最终状态 ---
	if rep1.Err != nil {
		t.Fatalf("build-level error: %v", rep1.Err)
	}
	if rep1.Results["stableA"].State != StateSucceeded {
		t.Fatalf("stableA: %s", rep1.Results["stableA"].State)
	}
	if rep1.Results["flakyB"].State != StateSucceeded || rep1.Results["flakyB"].Attempts != 3 {
		t.Fatalf("flakyB should succeed after 3 attempts: %+v", rep1.Results["flakyB"])
	}
	if rep1.Results["flakyC"].State != StateCanceled || rep1.Results["cancelChain"].State != StateCanceled {
		t.Fatalf("flakyC and cancelChain must be Canceled: %+v %+v",
			rep1.Results["flakyC"], rep1.Results["cancelChain"])
	}
	if got := atomic.LoadInt32(&flakyBRuns); got != 3 {
		t.Fatalf("flakyB should execute exactly 3 times, got %d", got)
	}
	if _, err := os.Stat(filepath.Join(root, "out", "flakyC.txt")); !os.IsNotExist(err) {
		t.Fatalf("flakyC artifact must not remain after cancel")
	}
	if _, err := os.Stat(filepath.Join(root, "out", "cancelChain.txt")); !os.IsNotExist(err) {
		t.Fatalf("cancelChain must never produce artifact")
	}

	// --- 第二次构建：输入不变，全部命中缓存 ---
	e2 := NewEngine()
	e2.Register("counting-file", countingRunner)
	e2.OnCacheDecision(func(d CacheDecision) {
		t.Logf("[cache2] %-12s stage=%s hit=%-5v %s", d.Task, d.Stage, d.Hit, d.Reason)
	})
	rep2 := e2.Build(context.Background(), Config{
		RootDir: root, CacheDir: cache, MaxConcurrency: 2, Tasks: cachedTasks,
	})
	if !rep2.Succeeded() {
		t.Fatalf("second build should succeed: %v", rep2.Err)
	}
	if !rep2.Results["stableA"].Cached || !rep2.Results["flakyB"].Cached {
		t.Fatalf("both should be cached: stableA=%+v flakyB=%+v",
			rep2.Results["stableA"], rep2.Results["flakyB"])
	}

	// --- 第三次构建：stableA 输入变化 -> 两个任务都重跑 ---
	if err := os.WriteFile(inputPath, []byte("version-2-changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	e3 := NewEngine()
	e3.Register("counting-file", countingRunner)
	var reasons []string
	e3.OnCacheDecision(func(d CacheDecision) {
		t.Logf("[cache3] %-12s stage=%s hit=%-5v %s", d.Task, d.Stage, d.Hit, d.Reason)
		if d.Stage == CacheLookup {
			reasons = append(reasons, d.Task+":"+d.Reason)
		}
	})
	rep3 := e3.Build(context.Background(), Config{
		RootDir: root, CacheDir: cache, MaxConcurrency: 2, Tasks: cachedTasks,
	})
	if !rep3.Succeeded() {
		t.Fatalf("third build should succeed: %v", rep3.Err)
	}
	if rep3.Results["stableA"].Cached || rep3.Results["flakyB"].Cached {
		t.Fatalf("both must re-execute after upstream change: %+v %+v",
			rep3.Results["stableA"], rep3.Results["flakyB"])
	}
	if got := atomic.LoadInt32(&stableARuns); got != 2 {
		t.Fatalf("stableA total executions should be 2, got %d", got)
	}
	if got := atomic.LoadInt32(&flakyBRuns); got != 4 {
		t.Fatalf("flakyB total executions should be 4 (3 first build + 1 rebuild), got %d", got)
	}
	if rep3.Results["flakyB"].Attempts != 1 {
		t.Fatalf("flakyB rebuild should succeed on first attempt, got %d", rep3.Results["flakyB"].Attempts)
	}
	wantReasons := map[string]bool{
		"stableA:input digest changed":  false,
		"flakyB:dependency key changed": false,
	}
	for _, r := range reasons {
		if _, ok := wantReasons[r]; ok {
			wantReasons[r] = true
		}
	}
	for r, found := range wantReasons {
		if !found {
			t.Fatalf("missing expected cache invalidation reason: %q (got %v)", r, reasons)
		}
	}

	// 产物一致性：最终工作区产物内容与输入版本自洽。
	data, err := os.ReadFile(filepath.Join(root, "out", "stableA.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("stableA final artifact empty")
	}
}
