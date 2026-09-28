package buildengine

import (
	"fmt"
	"strings"
)

// ValidationKind 用于区分不同类别的构建配置校验失败。
type ValidationKind string

const (
	// ValidationDuplicateTask: 同名任务被声明了不止一次。
	ValidationDuplicateTask ValidationKind = "duplicate_task"
	// ValidationMissingDependency: 依赖的任务不存在。
	ValidationMissingDependency ValidationKind = "missing_dependency"
	// ValidationCycle: 任务依赖图中存在环。
	ValidationCycle ValidationKind = "cycle"
	// ValidationDuplicateArtifact: 同一个产物名被多个任务产出。
	ValidationDuplicateArtifact ValidationKind = "duplicate_artifact"
	// ValidationMissingArtifact: 任务消费的产物没有任何任务产出。
	ValidationMissingArtifact ValidationKind = "missing_artifact"
	// ValidationIncompatibleArtifact: 消费方声明的产物 Kind 与产出方不匹配。
	ValidationIncompatibleArtifact ValidationKind = "incompatible_artifact"
)

// ValidationError 描述一条可定位到具体任务的配置校验错误。
type ValidationError struct {
	Kind   ValidationKind
	Task   string // 主要关联任务
	Other  string // 可选：对端任务（如重复声明、依赖边的另一端）
	Detail string // 产物名等补充信息
}

func (e *ValidationError) Error() string {
	switch e.Kind {
	case ValidationDuplicateTask:
		return fmt.Sprintf("validation[%s]: task %q declared more than once", e.Kind, e.Task)
	case ValidationMissingDependency:
		return fmt.Sprintf("validation[%s]: task %q depends on undefined task %q", e.Kind, e.Task, e.Other)
	case ValidationCycle:
		return fmt.Sprintf("validation[%s]: dependency cycle detected: %s", e.Kind, e.Detail)
	case ValidationDuplicateArtifact:
		return fmt.Sprintf("validation[%s]: artifact %q produced by both %q and %q", e.Kind, e.Detail, e.Task, e.Other)
	case ValidationMissingArtifact:
		return fmt.Sprintf("validation[%s]: task %q consumes artifact %q but no task produces it", e.Kind, e.Task, e.Detail)
	case ValidationIncompatibleArtifact:
		return fmt.Sprintf("validation[%s]: task %q consumes incompatible artifact (producer=%q): %s",
			e.Kind, e.Task, e.Other, e.Detail)
	default:
		return fmt.Sprintf("validation[%s]: %s", e.Kind, e.Detail)
	}
}

// ValidationErrors 聚合一次配置校验中发现的全部错误。
// 校验阶段不做任何执行，因此错误一次性完整暴露，绝不静默忽略。
type ValidationErrors struct {
	Errs []*ValidationError
}

func (ve *ValidationErrors) Error() string {
	parts := make([]string, len(ve.Errs))
	for i, e := range ve.Errs {
		parts[i] = e.Error()
	}
	return fmt.Sprintf("build configuration validation failed with %d error(s):\n  %s",
		len(ve.Errs), strings.Join(parts, "\n  "))
}

// HasKind 判断聚合错误中是否包含指定类别的校验错误。
func (ve *ValidationErrors) HasKind(kind ValidationKind) bool {
	for _, e := range ve.Errs {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

// ExecutionError 描述任务执行阶段的失败。
type ExecutionError struct {
	Task    string
	Attempt int
	Err     error
}

func (e *ExecutionError) Error() string {
	return fmt.Sprintf("task %q failed on attempt %d: %v", e.Task, e.Attempt, e.Err)
}

func (e *ExecutionError) Unwrap() error { return e.Err }

// CanceledError 表示因父 context 取消而被中止的任务。
type CanceledError struct {
	Task string
}

func (e *CanceledError) Error() string {
	return fmt.Sprintf("task %q canceled", e.Task)
}
