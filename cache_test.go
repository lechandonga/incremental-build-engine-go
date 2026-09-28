package buildengine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runTwice 执行同一配置两次，返回两次报告；用于验证第二次的缓存行为。
func runTwice(t *testing.T, root, cache string, tasks []TaskSpec) (r1, r2 *Report, d2 *[]CacheDecision) {
	t.Helper()
	e1 := NewEngine()
	hookEngine(t, e1)
	r1 = e1.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 2, Tasks: tasks})
	e2 := NewEngine()
	_, decs := hookEngine(t, e2)
	r2 = e2.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 2, Tasks: tasks})
	return r1, r2, decs
}

func TestCacheHitWhenNothingChanged(t *testing.T) {
	root, cache := newWorkspace(t)
	task := makeValidTask("a")
	r1, r2, _ := runTwice(t, root, cache, []TaskSpec{task})
	if !r1.Succeeded() || !r2.Succeeded() {
		t.Fatalf("both builds should succeed: %v / %v", r1.Err, r2.Err)
	}
	if r1.Results["a"].Cached || !r2.Results["a"].Cached {
		t.Fatalf("first run executes, second run must hit cache")
	}
}

func TestCacheInvalidatedByInputChange(t *testing.T) {
	root, cache := newWorkspace(t)
	inputPath := filepath.Join(root, "in.txt")
	if err := os.WriteFile(inputPath, []byte("v1"), 0o644); err != nil {
		t.Fatal(err)
	}
	task := makeValidTask("a")
	task.Inputs = []string{"in.txt"}

	e := NewEngine()
	hookEngine(t, e)
	r1 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r1.Succeeded() || r1.Results["a"].Cached {
		t.Fatalf("first build must execute: %+v", r1.Results["a"])
	}

	if err := os.WriteFile(inputPath, []byte("v2-changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	e2 := NewEngine()
	_, decs := hookEngine(t, e2)
	r2 := e2.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r2.Succeeded() || r2.Results["a"].Cached {
		t.Fatalf("changed input must force re-execution: %+v", r2.Results["a"])
	}
	if r2.Results["a"].Attempts != 1 {
		t.Fatalf("expected one fresh attempt, got %d", r2.Results["a"].Attempts)
	}
	found := false
	for _, d := range *decs {
		if d.Reason == "input digest changed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("cache decision must explain input digest change: %+v", *decs)
	}
}

func TestCacheInvalidatedByDependencyChange(t *testing.T) {
	root, cache := newWorkspace(t)
	a := makeValidTask("a")
	a.Params = map[string]string{"content:out/a.txt": "a-v1"}
	b := makeValidTask("b", "a")

	e := NewEngine()
	hookEngine(t, e)
	r1 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{a, b}})
	if !r1.Succeeded() {
		t.Fatalf("first build failed: %v", r1.Err)
	}

	// 修改 A 的产物内容参数，但 B 自身规格不变 -> B 的依赖键变化，必须重跑。
	a2 := a
	a2.Params = map[string]string{"content:out/a.txt": "a-v2-changed"}
	e2 := NewEngine()
	_, decs := hookEngine(t, e2)
	r2 := e2.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{a2, b}})
	if !r2.Succeeded() {
		t.Fatalf("second build failed: %v", r2.Err)
	}
	if r2.Results["a"].Cached || r2.Results["b"].Cached {
		t.Fatalf("both a and b must re-execute; a=%+v b=%+v", r2.Results["a"], r2.Results["b"])
	}
	found := false
	for _, d := range *decs {
		if d.Task == "b" && d.Reason == "dependency key changed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("b must be invalidated by dependency key change: %+v", *decs)
	}
}

// tamperEntry 直接改写缓存中某任务的 entry.json。
func tamperEntry(t *testing.T, cache, task string, mutate func(*cacheEntry)) {
	t.Helper()
	c := NewCache(cache)
	p := filepath.Join(c.taskDir(task), "entry.json")
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var entry cacheEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		t.Fatal(err)
	}
	mutate(&entry)
	out, err := json.Marshal(&entry)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCacheInvalidatedByCorruptMetadata(t *testing.T) {
	root, cache := newWorkspace(t)
	task := makeValidTask("a")
	r1, _, _ := runTwice(t, root, cache, []TaskSpec{task})
	if !r1.Succeeded() {
		t.Fatal(r1.Err)
	}
	// 到此处第二次构建已命中；现在破坏元数据，再跑第三次。
	tamperEntry(t, cache, "a", func(e *cacheEntry) { e.InputDigest = "tampered" })

	e3 := NewEngine()
	_, d3 := hookEngine(t, e3)
	r3 := e3.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r3.Succeeded() || r3.Results["a"].Cached {
		t.Fatalf("corrupt metadata must invalidate cache: %+v", r3.Results["a"])
	}
	found := false
	for _, d := range *d3 {
		if d.Reason == "input digest changed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected digest mismatch reason, got %+v", *d3)
	}
}

func TestCacheInvalidatedByMissingSnapshot(t *testing.T) {
	root, cache := newWorkspace(t)
	task := makeValidTask("a")
	r1, _, _ := runTwice(t, root, cache, []TaskSpec{task})
	if !r1.Succeeded() {
		t.Fatal(r1.Err)
	}
	c := NewCache(cache)
	if err := os.RemoveAll(filepath.Join(c.taskDir("a"), "outputs")); err != nil {
		t.Fatal(err)
	}

	e := NewEngine()
	_, decs := hookEngine(t, e)
	r2 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r2.Succeeded() || r2.Results["a"].Cached {
		t.Fatalf("missing snapshot must invalidate cache: %+v", r2.Results["a"])
	}
	found := false
	for _, d := range *decs {
		if strings.HasPrefix(d.Reason, "cached artifact missing: ") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected missing artifact reason, got %+v", *decs)
	}
}

func TestCacheInvalidatedByTamperedSnapshot(t *testing.T) {
	root, cache := newWorkspace(t)
	task := makeValidTask("a")
	r1, _, _ := runTwice(t, root, cache, []TaskSpec{task})
	if !r1.Succeeded() {
		t.Fatal(r1.Err)
	}
	c := NewCache(cache)
	snap := filepath.Join(c.taskDir("a"), "outputs", "out", "a.txt")
	if err := os.WriteFile(snap, []byte("tampered-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	e := NewEngine()
	_, decs := hookEngine(t, e)
	r2 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r2.Succeeded() || r2.Results["a"].Cached {
		t.Fatalf("tampered snapshot must invalidate: %+v", r2.Results["a"])
	}
	found := false
	for _, d := range *decs {
		if strings.HasPrefix(d.Reason, "cached artifact corrupt:") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected corrupt artifact reason, got %+v", *decs)
	}
}

func TestCacheInvalidatedByVersionMismatch(t *testing.T) {
	root, cache := newWorkspace(t)
	task := makeValidTask("a")
	r1, _, _ := runTwice(t, root, cache, []TaskSpec{task})
	if !r1.Succeeded() {
		t.Fatal(r1.Err)
	}
	tamperEntry(t, cache, "a", func(e *cacheEntry) { e.Version = CacheVersion + 999 })

	e := NewEngine()
	_, decs := hookEngine(t, e)
	r2 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r2.Succeeded() || r2.Results["a"].Cached {
		t.Fatalf("incompatible cache version must invalidate: %+v", r2.Results["a"])
	}
	found := false
	for _, d := range *decs {
		if strings.HasPrefix(d.Reason, "cache version mismatch") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected version mismatch reason, got %+v", *decs)
	}
}

func TestCacheRestoresArtifactsToWorkspace(t *testing.T) {
	root, cache := newWorkspace(t)
	task := makeValidTask("a")
	r1, _, _ := runTwice(t, root, cache, []TaskSpec{task})
	if !r1.Succeeded() {
		t.Fatal(r1.Err)
	}
	// 删除工作区产物后再命中缓存，产物必须被恢复。
	if err := os.Remove(filepath.Join(root, "out", "a.txt")); err != nil {
		t.Fatal(err)
	}
	e := NewEngine()
	hookEngine(t, e)
	r2 := e.Build(context.Background(), Config{RootDir: root, CacheDir: cache, MaxConcurrency: 1, Tasks: []TaskSpec{task}})
	if !r2.Succeeded() || !r2.Results["a"].Cached {
		t.Fatalf("expected cache hit, got %+v", r2.Results["a"])
	}
	data, err := os.ReadFile(filepath.Join(root, "out", "a.txt"))
	if err != nil {
		t.Fatalf("artifact should be restored from cache: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("restored artifact is empty")
	}
}
