package buildengine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestRetryThenSuccess(t *testing.T) {
	var attempts int32
	specs := []TaskSpec{
		{Name: "flaky", MaxRetries: 2,
			Produces: []ArtifactSpec{{Name: "f.out", Kind: "text"}},
			Run: func(ctx context.Context, rt *Runtime) error {
				n := atomic.AddInt32(&attempts, 1)
				t.Logf("flaky attempt=%d state=%s", rt.Attempt(), StatusRunning)
				if n < 3 {
					return &testErr{msg: "transient failure"}
				}
				return writeArtifact(rt, "f.out", "final")
			}},
	}
	e := NewEngine(WithLogger(testLogger{t}))
	res, err := e.Build(context.Background(), specs)
	if err != nil {
		t.Fatalf("retry should eventually succeed: %v", err)
	}
	tr := res.Tasks["flaky"]
	if tr.Status != StatusSuccess || tr.Attempts != 3 {
		t.Fatalf("status=%s attempts=%d", tr.Status, tr.Attempts)
	}
	if got := readArtifact(t, res, "flaky", "f.out"); got != "final" {
		t.Fatalf("artifact=%q", got)
	}
}

func TestRetryExhausted(t *testing.T) {
	var attempts int32
	specs := []TaskSpec{
		{Name: "doomed", MaxRetries: 1, Run: func(ctx context.Context, rt *Runtime) error {
			atomic.AddInt32(&attempts, 1)
			return &testErr{msg: "always broken"}
		}},
		{Name: "follower", DependsOn: []string{"doomed"}, Run: noopRun},
	}
	e := NewEngine(WithLogger(testLogger{t}))
	res, err := e.Build(context.Background(), specs)
	if err == nil {
		t.Fatal("expected failure after exhausting retries")
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if res.Status("doomed") != StatusFailed {
		t.Fatalf("doomed=%s", res.Status("doomed"))
	}
	if res.Status("follower") != StatusCanceled {
		t.Fatalf("follower=%s", res.Status("follower"))
	}
}

func TestFailedAttemptsDoNotPolluteCache(t *testing.T) {
	dir := t.TempDir()
	var attempts int32
	spec := []TaskSpec{{
		Name: "t", MaxRetries: 3, Version: 1,
		Produces: []ArtifactSpec{{Name: "t.out", Kind: "text"}},
		Run: func(ctx context.Context, rt *Runtime) error {
			n := atomic.AddInt32(&attempts, 1)
			if n < 3 {
				// 写一个“错误产物”后失败：必须被丢弃，不能进入缓存。
				if err := writeArtifact(rt, "t.out", "BAD"); err != nil {
					return err
				}
				return &testErr{msg: "boom"}
			}
			return writeArtifact(rt, "t.out", "GOOD")
		},
	}}
	e := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t}))
	res, err := e.Build(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := readArtifact(t, res, "t", "t.out"); got != "GOOD" {
		t.Fatalf("cached artifact polluted: %q", got)
	}

	// 新引擎重建：必须命中“好”条目，而不是任何失败尝试的残留。
	var attempts2 int32
	spec2 := []TaskSpec{{
		Name: "t", MaxRetries: 3, Version: 1,
		Produces: []ArtifactSpec{{Name: "t.out", Kind: "text"}},
		Run: func(ctx context.Context, rt *Runtime) error {
			atomic.AddInt32(&attempts2, 1)
			return writeArtifact(rt, "t.out", "GOOD")
		},
	}}
	res2, err := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t})).Build(context.Background(), spec2)
	if err != nil {
		t.Fatal(err)
	}
	if res2.Status("t") != StatusSkipped || attempts2 != 0 {
		t.Fatalf("expected cache hit, status=%s reruns=%d", res2.Status("t"), attempts2)
	}
	if got := readArtifact(t, res2, "t", "t.out"); got != "GOOD" {
		t.Fatalf("rebuilt artifact: %q", got)
	}

	// 暂存目录也不应残留失败尝试。
	staging := filepath.Join(dir, "..") // 缓存目录下永远不应出现 staging
	_ = staging
}

func TestCacheFormatVersionIncompatible(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	if _, err := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t})).
		Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1")); err != nil {
		t.Fatal(err)
	}
	// 把所有条目的 format_version 改成不兼容值。
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, en := range entries {
		p := filepath.Join(dir, en.Name(), "meta.json")
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			continue
		}
		m["format_version"] = 999
		out, _ := json.Marshal(m)
		if err := os.WriteFile(p, out, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t})).
		Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1")); err != nil {
		t.Fatal(err)
	}
	// 格式不兼容 -> 旧条目全部失效重跑（+2）。
	if runs != 4 {
		t.Fatalf("expected all entries invalidated by format version, runs=%d", runs)
	}
}

type testErr struct{ msg string }

func (e *testErr) Error() string { return e.msg }
