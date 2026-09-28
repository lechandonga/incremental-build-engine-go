package buildengine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// runner.go 定义任务执行器抽象及注册表，并用本地文件操作提供一个
// 内置的 "file" 执行器来模拟真实任务的产物与失败行为。

// RunContext 提供执行一次任务尝试所需的全部信息。
type RunContext struct {
	Task    *TaskSpec
	RootDir string
	Attempt int
}

// Runner 由具体工具实现，负责在本地产生声明的产物。
// 返回 error 表示本次尝试失败，引擎按重试策略决定是否再次调用。
// ctx 被取消时实现应尽快返回 ctx.Err()，且不得留下半成品产物。
type Runner interface {
	Run(ctx context.Context, rc RunContext) error
}

// RunnerFunc 让普通函数满足 Runner 接口。
type RunnerFunc func(ctx context.Context, rc RunContext) error

// Run 实现 Runner。
func (f RunnerFunc) Run(ctx context.Context, rc RunContext) error {
	return f(ctx, rc)
}

// registry 保存工具名到执行器的映射。
type registry struct {
	runners map[string]Runner
}

func newRegistry() *registry {
	return &registry{runners: make(map[string]Runner)}
}

// register 注册一个具名执行器。
func (r *registry) register(name string, runner Runner) {
	r.runners[name] = runner
}

// get 查找执行器。
func (r *registry) get(name string) (Runner, bool) {
	runner, ok := r.runners[name]
	return runner, ok
}

// FileRunnerName 是内置文件执行器的注册名。
const FileRunnerName = "file"

// newFileRunner 创建内置执行器：
//   - Params["fail_times"]=N 时，前 N 次尝试直接失败（模拟不稳定任务，不写任何产物）；
//   - Params["content:"+output] 可指定单个产物内容，否则写入确定性的默认内容；
//   - 所有产物通过“临时文件 + 原子 rename”落盘，失败/取消不会留下半成品。
func newFileRunner() Runner {
	return RunnerFunc(func(ctx context.Context, rc RunContext) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		failTimes := 0
		if rc.Task.Params != nil {
			if v, ok := rc.Task.Params["fail_times"]; ok {
				if n, err := strconv.Atoi(v); err == nil {
					failTimes = n
				}
			}
		}
		if rc.Attempt <= failTimes {
			return fmt.Errorf("task %s: simulated failure on attempt %d/%d", rc.Task.Name, rc.Attempt, failTimes)
		}

		root := rc.RootDir
		for _, out := range rc.Task.Outputs {
			if err := ctx.Err(); err != nil {
				return err
			}
			target, err := safeJoin(root, out)
			if err != nil {
				return fmt.Errorf("task %s: invalid output %q: %w", rc.Task.Name, out, err)
			}
			content := defaultArtifactContent(rc.Task.Name, rc.Attempt)
			if rc.Task.Params != nil {
				if v, ok := rc.Task.Params["content:"+out]; ok {
					content = v
				}
			}
			if err := atomicWriteFile(target, []byte(content)); err != nil {
				return fmt.Errorf("task %s: write artifact %q: %w", rc.Task.Name, out, err)
			}
		}
		return nil
	})
}

func defaultArtifactContent(task string, attempt int) string {
	return fmt.Sprintf("task=%s\nattempt=%d\n", task, attempt)
}

// safeJoin 将相对路径限制在根目录之内，避免路径穿越。
func safeJoin(root, rel string) (string, error) {
	clean := filepath.Clean(rel)
	if filepath.IsAbs(clean) || clean == ".." || filepath.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root: %s", rel)
	}
	return filepath.Join(root, clean), nil
}

// atomicWriteFile 先写同目录临时文件再 rename，保证产物要么完整出现要么不存在。
func atomicWriteFile(target string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(target), ".tmp-artifact-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, target)
}
