package buildengine

import (
	"context"
	"time"
)

// TaskStatus 是任务在其生命周期中的状态。
//
// 状态机（见 README“任务状态机”一节）：
//
//	pending ──► waiting ──► running ──► success
//	                           │
//	                           ├─(可重试失败)─► retrying ──► running ...
//	                           ├─(最终失败)──► failed
//	                           └─(context 取消)► canceled
//
// 命中增量缓存的任务直接记为 skipped（语义等同 success，但不执行 Run）。
type TaskStatus string

const (
	StatusPending  TaskStatus = "pending"
	StatusWaiting  TaskStatus = "waiting"
	StatusRunning  TaskStatus = "running"
	StatusRetrying TaskStatus = "retrying"
	StatusSuccess  TaskStatus = "success"
	StatusFailed   TaskStatus = "failed"
	StatusCanceled TaskStatus = "canceled"
	// StatusSkipped 表示命中增量缓存，任务体未执行。
	StatusSkipped TaskStatus = "skipped"
)

// IsTerminal 报告状态是否为终态。
func (s TaskStatus) IsTerminal() bool {
	switch s {
	case StatusSuccess, StatusFailed, StatusCanceled, StatusSkipped:
		return true
	}
	return false
}

// ArtifactSpec 声明任务产出的本地产物（文件名即 Name，内容由任务写入暂存目录）。
type ArtifactSpec struct {
	Name string
	Kind string // 产物类型标签，供上下游兼容性校验，例如 "text"、"json"
}

// ArtifactRef 声明任务消费的前置任务产物及其期望的 Kind。
type ArtifactRef struct {
	Name     string
	WantKind string
}

// TaskSpec 是单个构建任务的声明式规格。
type TaskSpec struct {
	Name       string
	DependsOn  []string
	Produces   []ArtifactSpec
	Consumes   []ArtifactRef
	Inputs     map[string]string // 任意键值输入，参与输入摘要计算
	Version    int               // 任务实现版本；变化后旧缓存自动失效
	MaxRetries int               // 首次尝试之外的最大重试次数，0 表示不重试
	// Run 是任务执行体。它必须把 Produces 中声明的产物写入 rt.StagingDir()，
	// 并可通过 rt.ArtifactPath 只读访问依赖产物。
	Run func(ctx context.Context, rt *Runtime) error
}

// Runtime 提供给任务执行体的本地运行环境。
// 每次尝试使用全新的暂存目录，失败尝试的暂存目录不会进入缓存。
type Runtime struct {
	attempt      int
	stagingDir   string
	artifactPath map[string]string
}

// Attempt 返回当前尝试序号（1 表示首次执行）。
func (rt *Runtime) Attempt() int { return rt.attempt }

// StagingDir 返回本次尝试的暂存目录，任务须把声明产出的产物写到该目录下
// （相对路径与 ArtifactSpec.Name 一致）。
func (rt *Runtime) StagingDir() string { return rt.stagingDir }

// ArtifactPath 返回某个被消费产物在磁盘上的只读路径。
func (rt *Runtime) ArtifactPath(name string) (string, bool) {
	p, ok := rt.artifactPath[name]
	return p, ok
}

// TaskResult 是单个任务的最终执行结果。
type TaskResult struct {
	Name     string
	Status   TaskStatus
	Attempts int // 实际执行次数（不含缓存命中）
	Cached   bool
	// OutputDir 为产物落地目录（命中缓存时指向缓存条目，否则指向当次临时目录）。
	OutputDir string
	Started   time.Time
	Ended     time.Time
	Err       error
}

// BuildResult 是一次构建的汇总结果。
type BuildResult struct {
	Tasks map[string]*TaskResult
}

// Status 返回指定任务的终态；任务不存在时返回空串。
func (r *BuildResult) Status(name string) TaskStatus {
	if tr, ok := r.Tasks[name]; ok {
		return tr.Status
	}
	return ""
}

// Logger 用于打印任务状态转换与缓存判定依据（默认丢弃）。
type Logger interface {
	Printf(format string, args ...any)
}

type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Engine 负责校验任务图、调度并发执行与增量缓存。
type Engine struct {
	cache          *CacheStore
	maxConcurrency int
	logger         Logger
	now            func() time.Time
}

// Option 配置 Engine。
type Option func(*Engine)

// WithCacheDir 启用基于本地目录的增量缓存；传入空路径表示禁用缓存。
func WithCacheDir(dir string) Option {
	return func(e *Engine) {
		if dir != "" {
			e.cache = NewCacheStore(dir)
		}
	}
}

// WithMaxConcurrency 设置同时执行任务体的最大并发数，<=0 表示不限。
func WithMaxConcurrency(n int) Option {
	return func(e *Engine) { e.maxConcurrency = n }
}

// WithLogger 注入诊断日志（任务状态转换、缓存命中/失效原因）。
func WithLogger(l Logger) Option {
	return func(e *Engine) {
		if l != nil {
			e.logger = l
		}
	}
}

// NewEngine 构建一个增量构建执行引擎。
func NewEngine(opts ...Option) *Engine {
	e := &Engine{
		maxConcurrency: 0,
		logger:         discardLogger{},
		now:            time.Now,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}
