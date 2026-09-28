package buildengine

// task.go 定义声明式构建配置、任务状态与执行报告等核心类型。

// TaskState 是任务生命周期中的状态。
//
// 状态机：
//
//	Pending -> Waiting (图构建后、依赖未完成)
//	Pending -> Ready   (无未完成依赖)
//	Waiting -> Ready    (所有依赖成功)
//	Waiting -> Canceled (某个依赖失败/取消且本任务不可再运行)
//	Ready   -> Running
//	Running -> Succeeded
//	Running -> Failed   (可重试时内部迁移: Failed -> Waiting -> Ready -> Retrying)
//	Running -> Retrying (一次尝试失败后、下次尝试开始前)
//	Retrying -> Running
//	Ready/Waiting/Running -> Canceled (取消传播或根上下文取消)
//
// Failed 是终态（重试次数耗尽）；Succeeded、Canceled 同为终态。
type TaskState int

const (
	StatePending TaskState = iota
	StateWaiting
	StateReady
	StateRunning
	StateRetrying
	StateSucceeded
	StateFailed
	StateCanceled
)

func (s TaskState) String() string {
	switch s {
	case StatePending:
		return "Pending"
	case StateWaiting:
		return "Waiting"
	case StateReady:
		return "Ready"
	case StateRunning:
		return "Running"
	case StateRetrying:
		return "Retrying"
	case StateSucceeded:
		return "Succeeded"
	case StateFailed:
		return "Failed"
	case StateCanceled:
		return "Canceled"
	default:
		return "Unknown"
	}
}

// Terminal 报告状态是否为终态。
func (s TaskState) Terminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCanceled
}

// TaskSpec 是单个任务的声明式规格。
type TaskSpec struct {
	// Name 任务唯一名称。
	Name string
	// Tool 指定执行器名称，必须在执行器注册表中存在。
	Tool string
	// Deps 依赖的前置任务名称列表。
	Deps []string
	// Inputs 输入文件路径（相对根目录），参与输入摘要计算。
	Inputs []string
	// Outputs 声明产出的产物路径（相对根目录），成功执行后必须存在。
	Outputs []string
	// Params 传给执行器的不透明参数，同样参与缓存键计算。
	Params map[string]string
	// MaxAttempts 最大尝试次数（含首次），<=0 时使用引擎默认值。
	MaxAttempts int
}

// Config 是一次构建的完整声明式配置。
type Config struct {
	// RootDir 输入与产物路径解析所基于的本地根目录。
	RootDir string
	// Tasks 任务规格列表，顺序无关，依赖关系由 Deps 声明。
	Tasks []TaskSpec
	// CacheDir 增量缓存目录；为空时禁用缓存。
	CacheDir string
	// MaxConcurrency 最大并发执行数，<=0 时使用默认值。
	MaxConcurrency int
	// DefaultMaxAttempts 每个任务的默认最大尝试次数，<=0 时默认为 1。
	DefaultMaxAttempts int
}

// Event 描述一次任务状态转换，用于日志与测试断言。
type Event struct {
	Task    string
	From    TaskState
	To      TaskState
	Attempt int
	// Reason 给出转换原因，如 "dependency failed"、"cache hit"。
	Reason string
}

// EventHook 在每次状态转换后被调用（回调中不应做长时间阻塞操作）。
type EventHook func(Event)

// TaskResult 是单个任务的最终结果。
type TaskResult struct {
	Name     string
	State    TaskState
	Attempts int
	// Cached 为 true 表示该任务通过增量缓存命中而未实际执行。
	Cached bool
	// CacheReason 记录缓存判定依据（命中键、失效原因），便于解释。
	CacheReason string
	Err         error
}

// Report 是一次构建执行的汇总报告。
type Report struct {
	Results map[string]*TaskResult
	// Err 为构建级错误（如校验失败、根上下文取消）。
	Err error
}

// Succeeded 判断整个构建是否成功（所有任务 Succeeded，允许 Cached）。
func (r *Report) Succeeded() bool {
	if r.Err != nil {
		return false
	}
	for _, res := range r.Results {
		if res.State != StateSucceeded {
			return false
		}
	}
	return true
}
