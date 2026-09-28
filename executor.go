package buildengine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// executor 承载一次 Build 的全部运行态：每个任务的状态、产物落地目录、
// 完成信号，以及用于级联取消的内部 cancel。
type executor struct {
	eng  *Engine
	g    *Graph
	root context.Context

	mu      sync.Mutex
	status  map[string]TaskStatus
	results map[string]*TaskResult
	outDir  map[string]string // 终态任务的产物条目目录（缓存条目或临时条目）
	done    map[string]chan struct{}

	sem chan struct{} // 最大并发信号量；nil 表示不限并发

	// failCancel 在任一任务终态失败/被取消时调用，用于中止仍在排队或运行的任务。
	failCancel context.CancelFunc
	// cause 记录首个触发级联取消的原因。
	causeMu    sync.Mutex
	cause      error
	scratchDir string
}

func newExecutor(eng *Engine, g *Graph, root context.Context) *executor {
	ex := &executor{
		eng:     eng,
		g:       g,
		root:    root,
		status:  make(map[string]TaskStatus),
		results: make(map[string]*TaskResult),
		outDir:  make(map[string]string),
		done:    make(map[string]chan struct{}),
	}
	if eng.maxConcurrency > 0 {
		ex.sem = make(chan struct{}, eng.maxConcurrency)
	}
	return ex
}

// Build 校验并执行全部任务，详见 Engine.Build。
func (e *Engine) Build(ctx context.Context, specs []TaskSpec) (*BuildResult, error) {
	g, verr := BuildGraph(specs)
	if verr != nil {
		// 校验失败：不执行任何任务，原样返回可区分的聚合错误。
		return nil, verr
	}

	ex := newExecutor(e, g, ctx)
	scratch, err := os.MkdirTemp("", "incremental-build-")
	if err != nil {
		return nil, fmt.Errorf("create scratch dir: %w", err)
	}
	ex.scratchDir = scratch
	// 禁用缓存时使用当次构建私有的临时缓存：产物仍能供下游使用，但不跨构建复用。
	cache := e.cache
	if cache == nil {
		cache = NewCacheStore(filepath.Join(scratch, "volatile-cache"))
	}

	sub, cancel := context.WithCancel(ctx)
	defer cancel()
	ex.failCancel = cancel

	// 先完成全部运行态初始化，再统一启动 goroutine，避免与 map 写入竞争。
	specsByOrder := make([]*TaskSpec, 0, len(g.order))
	ex.mu.Lock()
	for _, name := range g.order {
		specsByOrder = append(specsByOrder, g.tasks[name])
		ex.status[name] = StatusPending
		ex.results[name] = &TaskResult{Name: name, Status: StatusPending}
		ex.done[name] = make(chan struct{})
	}
	ex.mu.Unlock()
	for _, spec := range specsByOrder {
		go ex.runTask(sub, cache, spec)
	}

	// 等待所有任务 goroutine 到达终态（级联取消保证它们都会退出）。
	for _, name := range g.order {
		<-ex.done[name]
	}

	res := &BuildResult{Tasks: ex.results}
	// 注：即使禁用跨构建缓存，本次构建的产物目录也保留到 BuildResult 中，
	// 供下游/调用方检查；临时目录由系统的 tmp 清理策略回收。
	ex.causeMu.Lock()
	cause := ex.cause
	ex.causeMu.Unlock()
	if cause != nil {
		return res, cause
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	return res, nil
}

func (ex *executor) runTask(ctx context.Context, cache *CacheStore, spec *TaskSpec) {
	name := spec.Name
	defer close(ex.done[name])

	// pending -> waiting：开始等待直接依赖完成。
	ex.transition(name, StatusPending, StatusWaiting, "")

	// 等待依赖（同时响应外部/级联取消）。
	for _, d := range ex.g.deps[name] {
		select {
		case <-ex.done[d]:
		case <-ctx.Done():
			ex.finishCanceled(name, ctx.Err())
			return
		}
	}

	// 依赖未全部成功则不执行，并把取消沿依赖边向下传播。
	for _, d := range ex.g.deps[name] {
		st := ex.getStatus(d)
		if st != StatusSuccess && st != StatusSkipped {
			ex.finishCanceled(name, fmt.Errorf("dependency %q did not succeed (status=%s)", d, st))
			return
		}
	}

	if ctx.Err() != nil {
		ex.finishCanceled(name, ctx.Err())
		return
	}

	// 计算输入摘要与直接依赖产物摘要。
	inputDigest := InputDigest(spec)
	depFiles := map[string]map[string]string{}
	ex.mu.Lock()
	for _, d := range ex.g.deps[name] {
		dir := ex.outDir[d]
		m := map[string]string{}
		for _, a := range ex.g.tasks[d].Produces {
			m[a.Name] = filepath.Join(dir, "art", filepath.FromSlash(a.Name))
		}
		depFiles[d] = m
	}
	ex.mu.Unlock()

	depDigest, err := DepDigestOfFiles(depFiles)
	if err != nil {
		// 依赖产物文件缺失属于不可恢复的缓存/产物损坏：判失败并级联取消。
		ex.eng.logger.Printf("task=%s cache decision=MISS reason=%q", name, err.Error())
		ex.finishFailed(name, 1, err)
		return
	}

	// 增量缓存判定。
	if entryDir, hit := cache.lookup(ctx, spec, inputDigest, depDigest); hit {
		ex.eng.logger.Printf(
			"task=%s cache decision=HIT key=%s input=%s deps=%s version=%d",
			name, shortKey(entryDir), inputDigest[:12], depDigest[:12], spec.Version)
		ex.markSkipped(name, entryDir)
		return
	}
	ex.eng.logger.Printf(
		"task=%s cache decision=MISS key-inputs=%s key-deps=%s version=%d",
		name, inputDigest[:12], depDigest[:12], spec.Version)

	// 执行 + 重试循环。每次尝试使用全新暂存目录，失败尝试一律丢弃，
	// 只有成功并通过原子提交后产物才对下游可见——重试不会污染任何旧结果。
	var lastErr error
	attempts := 0
	for {
		if ctx.Err() != nil {
			ex.finishCanceled(name, ctx.Err())
			return
		}
		attempts++
		from := StatusWaiting
		if attempts > 1 {
			from = StatusRetrying
		}
		ex.transition(name, from, StatusRunning,
			fmt.Sprintf("attempt=%d", attempts))

		staging := filepath.Join(ex.scratchDir, "staging", name, fmt.Sprintf("attempt-%d", attempts))
		if err := os.MkdirAll(staging, 0o755); err != nil {
			lastErr = err
		} else {
			rt := ex.newRuntime(spec, attempts, staging)
			acquired := ex.acquire(ctx)
			if !acquired {
				_ = os.RemoveAll(staging)
				ex.finishCanceled(name, ctx.Err())
				return
			}
			runErr := spec.Run(ctx, rt)
			ex.release()
			if ctx.Err() != nil {
				// 即使任务体忽略 ctx 而“成功”，取消后也绝不提交产物。
				_ = os.RemoveAll(staging)
				ex.finishCanceled(name, ctx.Err())
				return
			}
			if runErr != nil {
				lastErr = runErr
			} else {
				if cerr := cache.commit(ctx, spec, inputDigest, depDigest, staging); cerr != nil {
					lastErr = cerr
				} else {
					key := cacheKey(spec.Name, inputDigest, depDigest)
					entryDir := filepath.Join(cache.dir, key)
					_ = os.RemoveAll(staging)
					ex.eng.logger.Printf("task=%s cache decision=COMMIT attempt=%d entry=%s",
						name, attempts, shortKey(entryDir))
					ex.markSuccess(name, attempts, false, entryDir, nil)
					return
				}
			}
			_ = os.RemoveAll(staging)
		}

		if attempts <= spec.MaxRetries {
			ex.transition(name, StatusRunning, StatusRetrying,
				fmt.Sprintf("attempt=%d failed: %v; retry scheduled", attempts, lastErr))
			continue
		}
		ex.finishFailed(name, attempts, &ExecutionError{
			Task: name, Attempt: attempts, Err: lastErr,
		})
		return
	}
}

func (ex *executor) newRuntime(spec *TaskSpec, attempt int, staging string) *Runtime {
	paths := map[string]string{}
	for _, ref := range spec.Consumes {
		if prod, ok := ex.g.ProducerOf(ref.Name); ok {
			ex.mu.Lock()
			dir := ex.outDir[prod]
			ex.mu.Unlock()
			if dir != "" {
				paths[ref.Name] = filepath.Join(dir, "art", filepath.FromSlash(ref.Name))
			}
		}
	}
	return &Runtime{attempt: attempt, stagingDir: staging, artifactPath: paths}
}

// acquire 获取并发槽；返回 false 表示在获取期间被取消（未持槽，无需释放）。
func (ex *executor) acquire(ctx context.Context) bool {
	if ex.sem == nil {
		return true
	}
	select {
	case ex.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (ex *executor) release() {
	if ex.sem == nil {
		return
	}
	select {
	case <-ex.sem:
	default:
	}
}

func (ex *executor) getStatus(name string) TaskStatus {
	ex.mu.Lock()
	defer ex.mu.Unlock()
	return ex.status[name]
}

func (ex *executor) transition(name string, from, to TaskStatus, note string) {
	ex.mu.Lock()
	ex.status[name] = to
	if tr := ex.results[name]; tr != nil {
		tr.Status = to
		if to == StatusRunning && tr.Started.IsZero() {
			tr.Started = ex.eng.now()
		}
	}
	ex.mu.Unlock()
	if note != "" {
		ex.eng.logger.Printf("task=%s transition: %s -> %s (%s)", name, from, to, note)
	} else {
		ex.eng.logger.Printf("task=%s transition: %s -> %s", name, from, to)
	}
}

func (ex *executor) finishTerminal(name string, st TaskStatus, attempts int, cached bool, dir string, err error) {
	ex.mu.Lock()
	ex.status[name] = st
	tr := ex.results[name]
	tr.Status = st
	tr.Attempts = attempts
	tr.Cached = cached
	tr.Ended = ex.eng.now()
	tr.Err = err
	if dir != "" {
		tr.OutputDir = filepath.Join(dir, "art")
		ex.outDir[name] = dir
	}
	ex.mu.Unlock()
	ex.eng.logger.Printf("task=%s terminal status=%s attempts=%d cached=%t err=%v",
		name, st, attempts, cached, err)
}

func (ex *executor) markSuccess(name string, attempts int, cached bool, entryDir string, err error) {
	ex.finishTerminal(name, StatusSuccess, attempts, cached, entryDir, err)
}

func (ex *executor) markSkipped(name, entryDir string) {
	// 等待态直接进入 skipped（缓存命中语义等同成功），不执行任务体。
	ex.finishTerminal(name, StatusSkipped, 0, true, entryDir, nil)
}

func (ex *executor) finishFailed(name string, attempts int, err error) {
	ex.causeMu.Lock()
	if ex.cause == nil {
		ex.cause = err
	}
	ex.causeMu.Unlock()
	ex.finishTerminal(name, StatusFailed, attempts, false, "", err)
	ex.failCancel() // 级联取消：阻止依赖它的任务继续执行
}

func (ex *executor) finishCanceled(name string, cause error) {
	ex.mu.Lock()
	attempts := ex.results[name].Attempts
	ex.mu.Unlock()
	ex.eng.logger.Printf("task=%s transition: -> canceled (cause=%v)", name, cause)
	ex.finishTerminal(name, StatusCanceled, attempts, false, "", &CanceledError{Task: name})
	// 被级联取消的任务本身不作为首因，但也要继续传播取消信号。
	ex.failCancel()
}

func shortKey(dir string) string {
	base := filepath.Base(dir)
	if len(base) > 12 {
		return base[:12]
	}
	return base
}
