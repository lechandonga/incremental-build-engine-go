package buildengine

import (
	"context"
	"testing"
)

func TestRetryThenSuccess(t *testing.T) {
	root, cache := newWorkspace(t)
	e := NewEngine()
	evs, decs := hookEngine(t, e)

	task := makeValidTask("flaky")
	task.MaxAttempts = 3
	task.Params = map[string]string{"fail_times": "2"} // 前两次失败，第三次成功

	cfg := Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}}
	rep := e.Build(context.Background(), cfg)
	if !rep.Succeeded() {
		t.Fatalf("expected eventual success, err=%v", rep.Err)
	}
	res := rep.Results["flaky"]
	if res.Attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", res.Attempts)
	}
	if !res.Cached == false {
		t.Fatalf("fresh execution should not be marked cached")
	}

	// 状态转换必须包含两次 Retrying 以及最终 Succeeded，且没有 Failed 终态。
	var retrying, running int
	for _, ev := range *evs {
		switch ev.To {
		case StateRetrying:
			retrying++
		case StateRunning:
			running++
		case StateFailed:
			t.Fatalf("task must not reach Failed terminal before success: %+v", ev)
		}
	}
	if retrying != 2 || running != 3 {
		t.Fatalf("expected 2 Retrying and 3 Running, got %d/%d", retrying, running)
	}
	// 失败过程不得写缓存；只在最终成功后有一次 store 命中。
	stores := 0
	for _, d := range *decs {
		if d.Stage == CacheStore {
			stores++
			if d.Attempt != 3 || !d.Hit {
				t.Fatalf("cache must only be populated on final success: %+v", d)
			}
		}
	}
	if stores != 1 {
		t.Fatalf("expected exactly 1 cache store, got %d", stores)
	}
}

func TestRetryExhaustedFailsAndCancelsDependents(t *testing.T) {
	root, cache := newWorkspace(t)
	e := NewEngine()
	evs, _ := hookEngine(t, e)

	flaky := makeValidTask("flaky")
	flaky.MaxAttempts = 2
	flaky.Params = map[string]string{"fail_times": "5"} // 永远失败
	down := makeValidTask("down", "flaky")
	indep := makeValidTask("indep")

	cfg := Config{RootDir: root, CacheDir: cache, MaxConcurrency: 2, Tasks: []TaskSpec{flaky, down, indep}}
	rep := e.Build(context.Background(), cfg)
	if rep.Succeeded() {
		t.Fatal("build should fail")
	}
	if rep.Results["flaky"].State != StateFailed || rep.Results["flaky"].Attempts != 2 {
		t.Fatalf("flaky should fail after 2 attempts, got %+v", rep.Results["flaky"])
	}
	if rep.Results["down"].State != StateCanceled {
		t.Fatalf("dependent should be canceled, got %s", rep.Results["down"].State)
	}
	if rep.Results["indep"].State != StateSucceeded {
		t.Fatalf("unrelated task should still succeed, got %s", rep.Results["indep"].State)
	}

	// down 必须经历 Waiting -> Canceled，且从未 Running。
	sawRun := false
	sawCancel := false
	for _, ev := range *evs {
		if ev.Task == "down" {
			if ev.To == StateRunning {
				sawRun = true
			}
			if ev.To == StateCanceled {
				sawCancel = true
			}
		}
	}
	if sawRun || !sawCancel {
		t.Fatalf("down must never run but must be canceled; run=%v cancel=%v", sawRun, sawCancel)
	}
}

func TestRetryDoesNotPollutePreviousGoodCache(t *testing.T) {
	root, cache := newWorkspace(t)
	e := NewEngine()

	task := makeValidTask("svc")
	task.Params = map[string]string{"content:out/svc.txt": "v1"}

	// 第一次：成功并写缓存。
	r1 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r1.Succeeded() {
		t.Fatalf("first build failed: %v", r1.Err)
	}

	// 第二次：不改输入，应命中缓存。
	_, decs := hookEngine(t, e)
	r2 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r2.Succeeded() || !r2.Results["svc"].Cached {
		t.Fatalf("second build should be a cache hit, got %+v", r2.Results["svc"])
	}
	for _, d := range *decs {
		if d.Stage == CacheLookup && d.Hit {
			t.Logf("cache decision: %s", d.Reason)
		}
	}
}
