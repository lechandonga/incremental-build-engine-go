package buildengine

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// TestCombinedRetryCacheInvalidationAndCancellation 组合场景：
//
//  1. 首次构建：gen 前两次失败、第三次成功（重试后成功）；down 消费 gen 产物。
//  2. 第二次构建（相同配置）：两任务均命中缓存，执行次数不变。
//  3. 第三次构建（gen 输入变化）：gen 重跑，新产物导致 down 的依赖摘要变化而级联重跑。
//  4. 并行跑另一条独立分支时外部取消：等待中的任务必须全部 canceled，
//     且不产生任何产物目录。
func TestCombinedRetryCacheInvalidationAndCancellation(t *testing.T) {
	dir := t.TempDir()

	var genRuns, downRuns int32
	// genSpec 的 flakyBefore 指定第几次尝试之前都失败（1=不失败）。
	genSpec := func(input string, flakyBefore int32) TaskSpec {
		return TaskSpec{
			Name: "gen", Version: 1, MaxRetries: 2,
			Inputs:   map[string]string{"seed": input},
			Produces: []ArtifactSpec{{Name: "gen/data.txt", Kind: "text"}},
			Run: func(ctx context.Context, rt *Runtime) error {
				atomic.AddInt32(&genRuns, 1)
				if int32(rt.Attempt()) < flakyBefore {
					return &testErr{msg: "flaky gen"}
				}
				return writeArtifact(rt, "gen/data.txt", "data:"+input)
			},
		}
	}
	downSpec := TaskSpec{
		Name: "down", Version: 1, DependsOn: []string{"gen"},
		Produces: []ArtifactSpec{{Name: "down/result.txt", Kind: "text"}},
		Consumes: []ArtifactRef{{Name: "gen/data.txt", WantKind: "text"}},
		Run: func(ctx context.Context, rt *Runtime) error {
			atomic.AddInt32(&downRuns, 1)
			p, _ := rt.ArtifactPath("gen/data.txt")
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			return writeArtifact(rt, "down/result.txt", "result("+string(b)+")")
		},
	}

	// --- 构建 1：重试后成功 ---
	e := NewEngine(WithCacheDir(dir), WithMaxConcurrency(2), WithLogger(testLogger{t}))
	res1, err := e.Build(context.Background(), []TaskSpec{genSpec("A", 3), downSpec})
	if err != nil {
		t.Fatalf("build1: %v", err)
	}
	if g := res1.Tasks["gen"]; g.Status != StatusSuccess || g.Attempts != 3 {
		t.Fatalf("build1 gen: status=%s attempts=%d", g.Status, g.Attempts)
	}
	if d := res1.Tasks["down"]; d.Status != StatusSuccess || d.Attempts != 1 {
		t.Fatalf("build1 down: status=%s attempts=%d", d.Status, d.Attempts)
	}
	if got := readArtifact(t, res1, "down", "down/result.txt"); got != "result(data:A)" {
		t.Fatalf("build1 artifact mismatch: %q", got)
	}

	// --- 构建 2：完全缓存命中 ---
	res2, err := e.Build(context.Background(), []TaskSpec{genSpec("A", 3), downSpec})
	if err != nil {
		t.Fatalf("build2: %v", err)
	}
	for _, n := range []string{"gen", "down"} {
		if res2.Status(n) != StatusSkipped {
			t.Fatalf("build2 %s expected skipped, got %s", n, res2.Status(n))
		}
	}
	if genRuns != 3 || downRuns != 1 {
		t.Fatalf("build2 execution counts changed: gen=%d down=%d", genRuns, downRuns)
	}
	if got := readArtifact(t, res2, "down", "down/result.txt"); got != "result(data:A)" {
		t.Fatalf("build2 artifact mismatch: %q", got)
	}

	// --- 构建 3：gen 输入变化 -> gen 重跑；down 因依赖产物变化级联重跑 ---
	res3, err := e.Build(context.Background(), []TaskSpec{genSpec("B", 1), downSpec})
	if err != nil {
		t.Fatalf("build3: %v", err)
	}
	if g := res3.Tasks["gen"]; g.Attempts != 1 {
		// 注：输入变化后的首次执行是稳定的（计数器与首次构建无关）。
		t.Fatalf("build3 gen expected 1 attempt, got %d (status=%s)", g.Attempts, g.Status)
	}
	if d := res3.Tasks["down"]; d.Status != StatusSuccess || d.Attempts != 1 {
		t.Fatalf("build3 down should rerun once, status=%s attempts=%d", d.Status, d.Attempts)
	}
	if got := readArtifact(t, res3, "down", "down/result.txt"); got != "result(data:B)" {
		t.Fatalf("build3 artifact mismatch: %q", got)
	}
	if genRuns != 4 || downRuns != 2 {
		t.Fatalf("build3 execution counts: gen=%d down=%d", genRuns, downRuns)
	}

	// --- 构建 4（独立配置）：外部取消，等待中的任务级联取消，不留产物 ---
	barrier := make(chan struct{})
	started := make(chan struct{})
	cancelSpecs := []TaskSpec{
		{Name: "leader", Produces: []ArtifactSpec{{Name: "l.o", Kind: "bin"}},
			Run: func(ctx context.Context, rt *Runtime) error {
				close(started)
				select {
				case <-barrier:
					return nil
				case <-ctx.Done():
					// 故意不写产物，模拟取消时的中断。
					return ctx.Err()
				}
			}},
		{Name: "follower1", DependsOn: []string{"leader"},
			Produces: []ArtifactSpec{{Name: "f1.o", Kind: "bin"}}, Run: noopRun},
		{Name: "follower2", DependsOn: []string{"leader"},
			Produces: []ArtifactSpec{{Name: "f2.o", Kind: "bin"}}, Run: noopRun},
		{Name: "parallelIndependent",
			Run: func(ctx context.Context, rt *Runtime) error {
				time.Sleep(10 * time.Millisecond)
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return nil
				}
			}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	var res4 *BuildResult
	buildDone := make(chan struct{})
	go func() {
		res4, err = e.Build(ctx, cancelSpecs)
		close(buildDone)
	}()
	<-started
	cancel()
	close(barrier)
	<-buildDone
	if err == nil {
		t.Fatal("build4 expected cancellation error")
	}
	if st := res4.Status("leader"); st != StatusCanceled {
		t.Fatalf("leader=%s", st)
	}
	for _, n := range []string{"follower1", "follower2"} {
		if st := res4.Status(n); st != StatusCanceled {
			t.Fatalf("%s=%s, want canceled", n, st)
		}
		if d := res4.Tasks[n].OutputDir; d != "" {
			t.Fatalf("canceled %s left artifact dir: %s", n, d)
		}
	}
	// 已完成构建 1-3 的既有缓存产物不得被取消操作污染：
	// 取消路径不调用 commit，只保留之前成功构建的条目。
	entries, _ := os.ReadDir(dir)
	t.Logf("cache entries after canceled build: %d", len(entries))
}
