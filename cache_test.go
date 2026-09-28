package buildengine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func cacheSpecs(runCount *int32, compileSrc, objContent string) []TaskSpec {
	return []TaskSpec{
		{Name: "compile", Version: 1,
			Inputs:   map[string]string{"src": compileSrc},
			Produces: []ArtifactSpec{{Name: "compile.o", Kind: "bin"}},
			Run: func(ctx context.Context, rt *Runtime) error {
				atomic.AddInt32(runCount, 1)
				return writeArtifact(rt, "compile.o", objContent)
			}},
		{Name: "link", Version: 1, DependsOn: []string{"compile"},
			Produces: []ArtifactSpec{{Name: "app", Kind: "bin"}},
			Consumes: []ArtifactRef{{Name: "compile.o", WantKind: "bin"}},
			Run: func(ctx context.Context, rt *Runtime) error {
				atomic.AddInt32(runCount, 1)
				p, _ := rt.ArtifactPath("compile.o")
				b, _ := os.ReadFile(p)
				return writeArtifact(rt, "app", "app<-"+string(b))
			}},
	}
}

func TestCacheHitSkipsExecution(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	e1 := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t}))
	res1, err := e1.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1"))
	if err != nil {
		t.Fatal(err)
	}
	if got := readArtifact(t, res1, "link", "app"); got != "app<-OBJ1" {
		t.Fatalf("first build artifact: %q", got)
	}

	// 第二次构建：输入与依赖完全相同，应全部命中缓存跳过。
	e2 := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t}))
	res2, err := e2.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"compile", "link"} {
		tr := res2.Tasks[name]
		if tr.Status != StatusSkipped || !tr.Cached || tr.Attempts != 0 {
			t.Fatalf("task %s expected cached skip, got status=%s attempts=%d", name, tr.Status, tr.Attempts)
		}
	}
	if runs != 2 {
		t.Fatalf("expected still 2 executions after cache rebuild, got %d", runs)
	}
	if got := readArtifact(t, res2, "link", "app"); got != "app<-OBJ1" {
		t.Fatalf("cached artifact content: %q", got)
	}
}

func TestCacheInvalidatesOnInputChange(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	e := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t}))
	if _, err := e.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1")); err != nil {
		t.Fatal(err)
	}
	// 输入 src 改变：compile 必须重跑；其产物内容也改变，link 必须级联重跑。
	if _, err := e.Build(context.Background(), cacheSpecs(&runs, "v2", "OBJ2")); err != nil {
		t.Fatal(err)
	}
	if runs != 4 {
		t.Fatalf("expected all tasks rerun after input change, total runs=%d", runs)
	}
}

func TestCacheDependencyChangeCascades(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	e := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t}))
	if _, err := e.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1")); err != nil {
		t.Fatal(err)
	}
	// 输入键值摘要相同，但 compile 任务版本升级且产物内容变化：
	// compile 重跑，link 因 depDigest 变化也必须重跑，即便 link 自身输入没变。
	specs := cacheSpecs(&runs, "v1", "OBJ3")
	specs[0].Version = 2
	if _, err := e.Build(context.Background(), specs); err != nil {
		t.Fatal(err)
	}
	compileRuns := int32(0)
	_ = compileRuns
	if runs != 4 {
		t.Fatalf("expected both tasks rerun after dependency artifact change, runs=%d", runs)
	}
}

func TestCacheCorruptionAutoInvalidates(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	e := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t}))
	if _, err := e.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1")); err != nil {
		t.Fatal(err)
	}
	// 破坏 compile 条目的 meta.json。
	if err := corruptMeta(dir, "compile"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1")); err != nil {
		t.Fatal(err)
	}
	// compile 重跑产生新条目；link 仍可命中（depDigest 相同），故总执行 +1。
	if runs != 3 {
		t.Fatalf("expected only corrupted compile to rerun, runs=%d", runs)
	}
}

func TestCacheMissingArtifactAutoInvalidates(t *testing.T) {
	dir := t.TempDir()
	var runs int32
	e := NewEngine(WithCacheDir(dir), WithLogger(testLogger{t}))
	res, err := e.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1"))
	if err != nil {
		t.Fatal(err)
	}
	// 从 compile 的缓存条目中删除产物文件。
	compileDir := filepath.Dir(res.Tasks["compile"].OutputDir)
	if err := os.Remove(filepath.Join(compileDir, "art", "compile.o")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Build(context.Background(), cacheSpecs(&runs, "v1", "OBJ1")); err != nil {
		t.Fatal(err)
	}
	if runs != 3 {
		t.Fatalf("expected compile rerun after artifact removal, runs=%d", runs)
	}
}

// corruptMeta 找到任务名匹配的缓存条目并破坏其 meta.json。
func corruptMeta(dir, taskName string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, en := range entries {
		metaPath := filepath.Join(dir, en.Name(), "meta.json")
		b, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		if bytes.Contains(b, []byte(`"task_name": "`+taskName+`"`)) {
			return os.WriteFile(metaPath, []byte("{not-json"), 0o644)
		}
	}
	return nil
}
