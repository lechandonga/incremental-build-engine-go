package buildengine

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

// executor.go 按依赖顺序并发调度任务，负责并发上限、取消传播、
// 增量缓存判定与失败重试。

// CacheStage 标识一次缓存判定发生的阶段。
type CacheStage string

const (
	CacheLookup CacheStage = "lookup"
	CacheStore  CacheStage = "store"
)

// CacheDecision 记录一次缓存判定及其可解释依据，供日志与测试使用。
type CacheDecision struct {
	Task    string
	Stage   CacheStage
	Hit     bool
	Reason  string
	Attempt int
}

// CacheHook 在每次缓存判定（查询/写入）后被调用。
type CacheHook func(CacheDecision)

// Engine 是可复用的增量构建执行组件。
type Engine struct {
	reg       *registry
	eventHook EventHook
	cacheHook CacheHook
}

// NewEngine 创建一个带内置 "file" 执行器的引擎。
func NewEngine() *Engine {
	reg := newRegistry()
	reg.register(FileRunnerName, newFileRunner())
	return &Engine{reg: reg}
}

// Register 注册（或覆盖）一个具名执行器，返回引擎自身以便链式调用。
func (e *Engine) Register(name string, runner Runner) *Engine {
	e.reg.register(name, runner)
	return e
}

// OnEvent 注册任务状态转换事件钩子。
func (e *Engine) OnEvent(hook EventHook) *Engine {
	e.eventHook = hook
	return e
}

// OnCacheDecision 注册缓存判定钩子。
func (e *Engine) OnCacheDecision(hook CacheHook) *Engine {
	e.cacheHook = hook
	return e
}

// Build 校验配置并执行整个构建。校验失败时不会执行任何任务，
// 返回的 Report.Err 为可通过 errors.As 区分的校验错误。
func (e *Engine) Build(ctx context.Context, cfg Config) *Report {
	h, err := e.Start(ctx, cfg)
	if err != nil {
		return &Report{Results: map[string]*TaskResult{}, Err: err}
	}
	return h.Wait()
}

// Handle 是一次进行中构建的句柄，支持等待结果或取消单个任务。
type Handle struct {
	x    *execution
	done chan struct{}
	rep  *Report
}

// Wait 阻塞直到构建结束并返回汇总报告。
func (h *Handle) Wait() *Report {
	<-h.done
	return h.rep
}

// CancelTask 请求取消单个任务：正在运行则中断，等待/就绪则直接取消，
// 并沿依赖边向所有传递下游传播取消；与该任务无依赖关系的任务继续执行。
// 对已结束任务调用返回 nil 且不产生任何效果。
func (h *Handle) CancelTask(name string) error {
	return h.x.cancelTaskByName(name, "canceled by caller")
}

// Start 校验配置并在后台开始执行，返回可等待/可取消的句柄。
func (e *Engine) Start(ctx context.Context, cfg Config) (*Handle, error) {
	g, err := buildGraph(cfg, e.reg)
	if err != nil {
		return nil, err
	}
	maxConc := cfg.MaxConcurrency
	if maxConc <= 0 {
		maxConc = runtime.NumCPU()
	}
	if maxConc < 1 {
		maxConc = 1
	}
	if n := len(g.nodes); n > 0 && maxConc > n {
		maxConc = n
	}

	x := &execution{
		cfg:        cfg,
		parentCtx:  ctx,
		g:          g,
		cache:      NewCache(cfg.CacheDir),
		reg:        e.reg,
		eventHook:  e.eventHook,
		cacheHook:  e.cacheHook,
		machines:   make(map[string]*stateMachine, len(g.nodes)),
		results:    make(map[string]*TaskResult, len(g.nodes)),
		pending:    make(map[string]int, len(g.nodes)),
		keys:       make(map[string]string, len(g.nodes)),
		taskCtx:    make(map[string]context.Context, len(g.nodes)),
		taskCancel: make(map[string]context.CancelFunc, len(g.nodes)),
		settled:    make(map[string]bool, len(g.nodes)),
		ready:      make(chan *taskNode, len(g.nodes)),
		maxConc:    maxConc,
		finished:   make(chan struct{}),
	}
	rootCtx, rootCancel := context.WithCancel(ctx)
	x.rootCtx = rootCtx
	x.rootCancel = rootCancel
	x.defaultAttempts = cfg.DefaultMaxAttempts
	if x.defaultAttempts <= 0 {
		x.defaultAttempts = 1
	}

	h := &Handle{x: x, done: make(chan struct{})}
	x.handle = h
	go x.run()
	return h, nil
}

// execution 承载一次 Build 的运行时状态。
type execution struct {
	cfg       Config
	g         *graph
	cache     *Cache
	reg       *registry
	eventHook EventHook
	cacheHook CacheHook

	parentCtx  context.Context
	rootCtx    context.Context
	rootCancel context.CancelFunc

	maxConc         int
	defaultAttempts int

	wg       sync.WaitGroup // 每个任务抵达终态时 Done 一次
	ready    chan *taskNode
	finished chan struct{}
	handle   *Handle

	mu       sync.Mutex
	machines map[string]*stateMachine
	results  map[string]*TaskResult
	pending  map[string]int // 剩余未成功完成的依赖数
	keys     map[string]string
	settled  map[string]bool

	taskCtx    map[string]context.Context
	taskCancel map[string]context.CancelFunc
}

// retryBaseDelay 是重试退避的基础延迟（指数增长），保持很小以加快本地测试。
const retryBaseDelay = 2 * time.Millisecond

func (x *execution) emitCache(d CacheDecision) {
	if x.cacheHook != nil {
		x.cacheHook(d)
	}
}

// run 驱动整个调度生命周期。
func (x *execution) run() {
	defer close(x.finished)
	defer x.rootCancel()

	names := append([]string(nil), x.g.order...)
	for _, name := range names {
		node := x.g.nodes[name]
		x.machines[name] = newStateMachine(name, x.eventHook)
		x.pending[name] = len(node.deps)
		cctx, cancel := context.WithCancel(x.rootCtx)
		x.taskCtx[name] = cctx
		x.taskCancel[name] = cancel
	}

	x.wg.Add(len(names))

	// worker 池：数量即最大并发度。
	var workerWG sync.WaitGroup
	workerWG.Add(x.maxConc)
	for i := 0; i < x.maxConc; i++ {
		go func() {
			defer workerWG.Done()
			for node := range x.ready {
				x.runTask(node)
			}
		}()
	}
	go func() {
		x.wg.Wait()
		close(x.ready)
	}()

	// 根上下文取消：取消全部非终态任务（finished 后 rootCancel 会触发，需排除）。
	go func() {
		select {
		case <-x.finished:
			return
		case <-x.rootCtx.Done():
			select {
			case <-x.finished:
				return
			default:
			}
			x.cancelAll("build context canceled")
		}
	}()

	// 初始就绪集合，并发出首批状态转换事件。
	for _, name := range names {
		node := x.g.nodes[name]
		m := x.machines[name]
		if len(node.deps) == 0 {
			m.transition(StatePending, StateReady, "no dependencies")
			x.ready <- node
		} else {
			m.transition(StatePending, StateWaiting, "waiting for dependencies")
		}
	}

	workerWG.Wait()

	x.mu.Lock()
	rep := &Report{Results: make(map[string]*TaskResult, len(x.results))}
	for k, v := range x.results {
		cp := *v
		rep.Results[k] = &cp
	}
	x.mu.Unlock()
	if x.parentCtx.Err() != nil {
		rep.Err = x.parentCtx.Err()
	}
	x.handle.rep = rep
	close(x.handle.done)
}

// runTask 执行单个任务（每个任务至多有一个 goroutine 进入）。
func (x *execution) runTask(node *taskNode) {
	spec := node.spec
	m := x.machines[spec.Name]

	// 可能在被 worker 取走前已被取消传播命中，此时终态由取消方结算。
	if m.current() != StateReady {
		return
	}

	maxAttempts := spec.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = x.defaultAttempts
	}

	inputDigest, err := digestInputs(x.cfg.RootDir, spec.Inputs)
	if err != nil {
		x.failBeforeRun(node, "pre-execution failure: "+err.Error(), err)
		return
	}
	paramsHash := hashParams(spec.Params)
	depKeys := x.dependencyKeys(node)

	// 增量缓存判定。
	if x.cache.Enabled() {
		res := x.cache.lookup(spec.Name, inputDigest, paramsHash, depKeys)
		x.emitCache(CacheDecision{Task: spec.Name, Stage: CacheLookup, Hit: res.Hit, Reason: res.Reason})
		if res.Hit {
			ok, reason := x.cache.restore(x.taskCtx[spec.Name], spec.Name, x.cfg.RootDir, spec.Outputs)
			if ok && x.taskCtx[spec.Name].Err() == nil && m.current() == StateReady {
				m.transition(StateReady, StateSucceeded, "cache hit: "+res.Reason)
				x.recordSuccess(node, inputDigest, paramsHash, depKeys, true, "cache hit: "+reason)
				x.afterTerminal(node)
				return
			}
			// 恢复途中被取消：清理可能已经恢复出的部分产物并按取消结算。
			if x.taskCtx[spec.Name].Err() != nil {
				m.cancel("task context canceled")
				x.removeArtifacts(node)
				x.settleTerminal(node, &TaskResult{Name: spec.Name, State: StateCanceled, Attempts: 0, Err: context.Canceled})
				x.afterTerminal(node)
				return
			}
			// 恢复失败（条目已在内部失效）：退化为真实执行并记录依据。
			x.emitCache(CacheDecision{Task: spec.Name, Stage: CacheLookup, Hit: false,
				Reason: "cache restore failed, re-executing: " + reason})
		}
	}

	for {
		attempt := m.beginAttempt()
		var moved bool
		if attempt == 1 {
			moved = m.transition(StateReady, StateRunning, fmt.Sprintf("starting attempt %d/%d", attempt, maxAttempts))
		} else {
			moved = m.transition(StateRetrying, StateRunning, fmt.Sprintf("starting attempt %d/%d", attempt, maxAttempts))
		}
		if !moved {
			// 转换期间被取消；终态已由取消方结算。
			return
		}

		runner, _ := x.reg.get(spec.Tool)
		runErr := runner.Run(x.taskCtx[spec.Name], RunContext{Task: spec, RootDir: x.cfg.RootDir, Attempt: attempt})

		// 取消优先：任务上下文（根取消或本任务取消传播）被取消时，
		// 无论 runner 返回的是 error 还是恰好完成，结果都归为 Canceled。
		// 这里主动迁移一次，可覆盖“runner 已返回但取消遍历尚未命中本任务”的竞态。
		if x.taskCtx[spec.Name].Err() != nil {
			m.cancel("task context canceled")
		}
		if m.current() == StateCanceled {
			x.removeArtifacts(node)
			x.settleTerminal(node, &TaskResult{Name: spec.Name, State: StateCanceled, Attempts: attempt, Err: context.Canceled})
			x.afterTerminal(node)
			return
		}

		if runErr == nil {
			if miss := missingArtifacts(x.cfg.RootDir, spec.Outputs); miss != "" {
				runErr = fmt.Errorf("runner reported success but artifact %q is missing", miss)
			}
		}

		if runErr == nil {
			m.transition(StateRunning, StateSucceeded, fmt.Sprintf("attempt %d succeeded", attempt))
			cacheReason := "cache disabled"
			if x.cache.Enabled() {
				storeErr := x.cache.store(x.taskCtx[spec.Name], spec.Name, x.cfg.RootDir, inputDigest, paramsHash, depKeys, spec.Outputs)
				if storeErr != nil {
					cacheReason = "cache store failed (execution still succeeded): " + storeErr.Error()
					x.emitCache(CacheDecision{Task: spec.Name, Stage: CacheStore, Hit: false, Reason: cacheReason, Attempt: attempt})
				} else {
					cacheReason = "cache populated"
					x.emitCache(CacheDecision{Task: spec.Name, Stage: CacheStore, Hit: true, Reason: cacheReason, Attempt: attempt})
				}
			}
			x.recordSuccess(node, inputDigest, paramsHash, depKeys, false, cacheReason)
			x.afterTerminal(node)
			return
		}

		// 本次尝试失败：半成品产物不允许残留。
		x.removeArtifacts(node)
		if attempt < maxAttempts {
			m.transition(StateRunning, StateRetrying,
				fmt.Sprintf("attempt %d/%d failed: %v; will retry", attempt, maxAttempts, runErr))
			if !x.backoff(spec.Name, attempt) {
				// 退避期间被取消（取消传播或根取消）。
				if m.current() == StateCanceled {
					x.settleTerminal(node, &TaskResult{Name: spec.Name, State: StateCanceled, Attempts: attempt, Err: context.Canceled})
					x.afterTerminal(node)
				}
				return
			}
			continue
		}

		m.transition(StateRunning, StateFailed,
			fmt.Sprintf("all %d attempt(s) exhausted; last error: %v", attempt, runErr))
		x.settleTerminal(node, &TaskResult{Name: spec.Name, State: StateFailed, Attempts: attempt, Err: runErr})
		x.afterTerminal(node)
		return
	}
}

// failBeforeRun 处理无需执行即判定失败的情况（如输入路径非法）。
func (x *execution) failBeforeRun(node *taskNode, reason string, cause error) {
	m := x.machines[node.spec.Name]
	if m.transition(StateReady, StateFailed, reason) {
		x.settleTerminal(node, &TaskResult{Name: node.spec.Name, State: StateFailed, Attempts: 0, Err: cause})
		x.afterTerminal(node)
	}
}

// recordSuccess 记录成功结果与对外传递缓存键。
func (x *execution) recordSuccess(node *taskNode, inputDigest, paramsHash string, depKeys map[string]string, cached bool, cacheReason string) {
	name := node.spec.Name
	key := cacheKey(name, inputDigest, paramsHash, depKeys)
	attempts := 0
	if !cached {
		attempts = x.machines[name].attempts()
	}
	x.mu.Lock()
	x.keys[name] = key
	x.mu.Unlock()
	x.settleTerminal(node, &TaskResult{
		Name:        name,
		State:       StateSucceeded,
		Attempts:    attempts,
		Cached:      cached,
		CacheReason: cacheReason,
	})
}

// afterTerminal 在任务抵达终态后处理下游：成功放行，否则取消传播。
func (x *execution) afterTerminal(node *taskNode) {
	state := x.machines[node.spec.Name].current()
	nexts := append([]*taskNode(nil), node.next...)
	sort.Slice(nexts, func(i, j int) bool { return nexts[i].spec.Name < nexts[j].spec.Name })

	if state != StateSucceeded {
		reason := "upstream task " + node.spec.Name + " " + state.String()
		visited := map[string]bool{node.spec.Name: true}
		queue := append([]*taskNode(nil), nexts...)
		for len(queue) > 0 {
			nxt := queue[0]
			queue = queue[1:]
			if visited[nxt.spec.Name] {
				continue
			}
			visited[nxt.spec.Name] = true
			queue = append(queue, nxt.next...)
			x.requestCancel(nxt, reason)
		}
		return
	}

	for _, nxt := range nexts {
		x.mu.Lock()
		if x.machines[nxt.spec.Name].current().Terminal() {
			x.mu.Unlock()
			continue
		}
		x.pending[nxt.spec.Name]--
		zero := x.pending[nxt.spec.Name] == 0
		x.mu.Unlock()
		if zero && x.machines[nxt.spec.Name].transition(StateWaiting, StateReady, "all dependencies succeeded") {
			x.ready <- nxt
		}
	}
}

// requestCancel 取消单个非终态节点；运行中的节点由其 worker 自行结算。
func (x *execution) requestCancel(node *taskNode, reason string) {
	name := node.spec.Name
	if cancel, ok := x.taskCancel[name]; ok {
		cancel()
	}
	changed, workerAlive := x.machines[name].cancel(reason)
	if !changed || workerAlive {
		return
	}
	x.settleTerminal(node, &TaskResult{Name: name, State: StateCanceled, Err: context.Canceled})
}

func (x *execution) cancelTaskByName(name, reason string) error {
	node, ok := x.g.nodes[name]
	if !ok {
		return fmt.Errorf("unknown task: %s", name)
	}
	if x.machines[name].current().Terminal() {
		return nil
	}
	x.requestCancel(node, reason)
	x.afterTerminal(node)
	return nil
}

// cancelAll 在根上下文取消时把所有非终态任务迁移到 Canceled。
func (x *execution) cancelAll(reason string) {
	for _, name := range x.g.order {
		node := x.g.nodes[name]
		if cancel, ok := x.taskCancel[name]; ok {
			cancel()
		}
		changed, workerAlive := x.machines[name].cancel(reason)
		if !changed || workerAlive {
			continue
		}
		x.settleTerminal(node, &TaskResult{Name: name, State: StateCanceled, Err: context.Canceled})
	}
}

// settleTerminal 写入终态结果并恰好递减一次等待组；幂等。
// 任务抵达终态的所有路径（worker 成功/失败/取消，或取消传播）都必须经过这里。
func (x *execution) settleTerminal(node *taskNode, res *TaskResult) {
	x.mu.Lock()
	if x.settled[res.Name] {
		x.mu.Unlock()
		return
	}
	x.settled[res.Name] = true
	x.results[res.Name] = res
	x.mu.Unlock()
	x.wg.Done()
}

func (x *execution) dependencyKeys(node *taskNode) map[string]string {
	x.mu.Lock()
	defer x.mu.Unlock()
	depKeys := make(map[string]string, len(node.deps))
	for _, dep := range node.deps {
		depKeys[dep.spec.Name] = x.keys[dep.spec.Name]
	}
	return depKeys
}

// backoff 在重试之间等待，可被任务/根上下文立即打断；返回 false 表示已取消。
func (x *execution) backoff(task string, attempt int) bool {
	delay := retryBaseDelay << (attempt - 1)
	if delay > 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-x.taskCtx[task].Done():
		return false
	case <-x.finished:
		return false
	}
}

func (x *execution) removeArtifacts(node *taskNode) {
	for _, out := range node.spec.Outputs {
		if target, err := safeJoin(x.cfg.RootDir, out); err == nil {
			_ = os.Remove(target)
		}
	}
}

// missingArtifacts 返回第一个不存在的产物路径；全部存在返回空串。
func missingArtifacts(root string, outputs []string) string {
	for _, out := range outputs {
		target, err := safeJoin(root, out)
		if err != nil {
			return out
		}
		if _, err := os.Stat(target); err != nil {
			return out
		}
	}
	return ""
}
