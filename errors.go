package buildengine

// errors.go 存放构建图校验阶段可区分的错误类型。
// 所有校验错误都实现 ValidationError 标记接口，调用方可通过 errors.As 精确定位。

// ValidationError 是所有构建配置校验错误的标记接口。
type ValidationError interface {
	error
	ValidationFailure()
	// TaskName 返回出错任务名（循环依赖可能涉及多个任务）。
	TaskName() string
}

// DuplicateTaskError 表示配置中存在同名的重复任务。
type DuplicateTaskError struct {
	Task string
}

func (e *DuplicateTaskError) Error() string      { return "duplicate task: " + e.Task }
func (e *DuplicateTaskError) ValidationFailure() {}
func (e *DuplicateTaskError) TaskName() string   { return e.Task }

// MissingDependencyError 表示任务声明了不存在的依赖。
type MissingDependencyError struct {
	Task       string
	Dependency string
}

func (e *MissingDependencyError) Error() string {
	return "task " + e.Task + " depends on missing task " + e.Dependency
}
func (e *MissingDependencyError) ValidationFailure() {}
func (e *MissingDependencyError) TaskName() string   { return e.Task }

// CyclicDependencyError 表示任务依赖图中存在环，Cycle 按环上顺序给出任务名。
type CyclicDependencyError struct {
	Cycle []string
}

func (e *CyclicDependencyError) Error() string {
	msg := "cyclic dependency detected:"
	for _, name := range e.Cycle {
		msg += " " + name + " ->"
	}
	return msg
}
func (e *CyclicDependencyError) ValidationFailure() {}
func (e *CyclicDependencyError) TaskName() string {
	if len(e.Cycle) == 0 {
		return ""
	}
	return e.Cycle[0]
}

// IncompatibleArtifactError 表示两个任务声明产出同一个产物路径，无法判定归属。
type IncompatibleArtifactError struct {
	Task     string
	Other    string
	Artifact string
}

func (e *IncompatibleArtifactError) Error() string {
	return "incompatible artifact: tasks " + e.Task + " and " + e.Other + " both produce " + e.Artifact
}
func (e *IncompatibleArtifactError) ValidationFailure() {}
func (e *IncompatibleArtifactError) TaskName() string   { return e.Task }

// UnknownToolError 表示任务引用的执行器（工具）未注册。
type UnknownToolError struct {
	Task string
	Tool string
}

func (e *UnknownToolError) Error() string {
	return "task " + e.Task + " uses unregistered tool " + e.Tool
}
func (e *UnknownToolError) ValidationFailure() {}
func (e *UnknownToolError) TaskName() string   { return e.Task }
