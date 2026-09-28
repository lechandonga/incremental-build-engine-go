package buildengine

import "sync"

// state.go 实现单个任务的并发安全状态机，所有状态转换都从这里发出事件，
// 从而保证“运行、等待、成功、失败、重试、取消”之间状态一致且可解释。

// allowedTransitions 定义合法的状态迁移表。
var allowedTransitions = map[TaskState]map[TaskState]bool{
	StatePending: {
		StateWaiting:  true,
		StateReady:    true,
		StateCanceled: true,
	},
	StateWaiting: {
		StateReady:    true,
		StateCanceled: true,
	},
	StateReady: {
		StateRunning:   true,
		StateSucceeded: true, // 缓存命中：直接成功
		StateCanceled:  true,
	},
	StateRunning: {
		StateSucceeded: true,
		StateFailed:    true,
		StateRetrying:  true,
		StateCanceled:  true,
	},
	StateRetrying: {
		StateRunning:  true,
		StateCanceled: true,
	},
}

// stateMachine 以互斥锁保护单个任务的状态一致性。
type stateMachine struct {
	mu      sync.Mutex
	name    string
	state   TaskState
	attempt int
	hook    EventHook
}

func newStateMachine(name string, hook EventHook) *stateMachine {
	return &stateMachine{name: name, state: StatePending, hook: hook}
}

// current 返回当前状态。
func (m *stateMachine) current() TaskState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// emit 必须在持锁状态下调用。
func (m *stateMachine) emit(from, to TaskState, reason string) {
	if m.hook != nil {
		m.hook(Event{Task: m.name, From: from, To: to, Attempt: m.attempt, Reason: reason})
	}
}

// transition 要求当前状态恰好为 from 且迁移合法，原子完成迁移并发出事件。
func (m *stateMachine) transition(from, to TaskState, reason string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != from || m.state.Terminal() {
		return false
	}
	if !allowedTransitions[from][to] {
		return false
	}
	old := m.state
	m.state = to
	m.emit(old, to, reason)
	return true
}

// force 用于取消传播：从任意非终态迁移到目标态（当前仅允许 Canceled）。
// 已经处于终态的任务不受影响，返回 false。
func (m *stateMachine) force(to TaskState, reason string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if to != StateCanceled || m.state.Terminal() {
		return false
	}
	old := m.state
	m.state = to
	m.emit(old, to, reason)
	return true
}

// cancel 把任务迁移到 Canceled，返回：
//   - changed：是否实际发生了状态迁移；
//   - workerAlive：迁移前任务正由 worker goroutine 处理（Running/Retrying），
//     此时由该 worker 自行结算终态，取消方不得重复结算。
func (m *stateMachine) cancel(reason string) (changed, workerAlive bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.Terminal() {
		return false, false
	}
	old := m.state
	workerAlive = old == StateRunning || old == StateRetrying
	m.state = StateCanceled
	m.emit(old, StateCanceled, reason)
	return true, workerAlive
}

// beginAttempt 把尝试计数 +1 并返回新值。
func (m *stateMachine) beginAttempt() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.attempt++
	return m.attempt
}

// attempts 返回已经开始的尝试次数。
func (m *stateMachine) attempts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attempt
}
