# Incremental Build Engine (Go)

一个可复用的增量构建执行组件：从声明式配置构建任务图，按依赖顺序并发执行，
基于输入摘要与前置任务的传递缓存键做增量判定，并提供确定性的失败重试与取消语义。
任务产物与依赖完全用本地文件和进程模拟，无外部服务依赖。

## 包结构

| 文件 | 职责 |
| --- | --- |
| `task.go` | 声明式配置（`Config`/`TaskSpec`）、任务状态枚举、事件与报告类型 |
| `errors.go` | 可区分的校验错误类型，均实现 `ValidationError` 标记接口 |
| `graph.go` | 图构建、拓扑排序、重复/缺失/循环依赖与产物冲突校验 |
| `state.go` | 单任务并发安全状态机，所有状态转换统一发出事件 |
| `runner.go` | `Runner` 执行器抽象、注册表，以及内置 `file` 执行器（原子写产物） |
| `cache.go` | 输入/参数/依赖摘要、缓存条目原子写入、快照校验与失效 |
| `executor.go` | 引擎：worker 池调度、并发上限、重试退避、取消传播 |

## 快速使用

```go
eng := buildengine.NewEngine() // 已内置 "file" 执行器
rep := eng.Build(ctx, buildengine.Config{
    RootDir:        rootDir,
    CacheDir:       cacheDir,   // 为空则禁用增量缓存
    MaxConcurrency: 4,
    DefaultMaxAttempts: 2,
    Tasks: []buildengine.TaskSpec{
        {Name: "compile", Tool: "file", Deps: []string{"fetch"},
         Inputs: []string{"src/main.go"}, Outputs: []string{"out/bin"}},
        {Name: "fetch", Tool: "file", Outputs: []string{"out/dep"}},
    },
})
if !rep.Succeeded() { /* rep.Err 或 Results[name].State/Err */ }
```

- `Engine.OnEvent(func(Event))`：订阅任务状态转换（含尝试次数与原因）。
- `Engine.OnCacheDecision(func(CacheDecision))`：订阅缓存查询/写入的判定依据。
- `Engine.Start(ctx, cfg)` 返回 `*Handle`，可在执行中 `CancelTask(name)`，
  `Wait()` 阻塞到全部任务到达终态。
- 自定义执行器实现 `Runner` 接口后用 `Engine.Register(name, runner)` 注册。
- 内置 `file` 执行器按 `Params` 工作：
  - `content:<产物相对路径>` 指定产物内容；
  - `fail_times=N` 让前 N 次尝试失败，用于模拟不稳定任务与重试。

## 配置校验（失败即整体拒绝执行）

校验在任何任务开始之前完成，错误确定且可通过 `errors.As` 区分，定位到具体任务：

| 错误类型 | 触发条件 |
| --- | --- |
| `*DuplicateTaskError` | 配置中出现同名任务（`TaskName()` 给出任务名） |
| `*MissingDependencyError` | 任务依赖未在配置中声明（含 `Task`/`Dependency`） |
| `*CyclicDependencyError` | 依赖图存在环（含自依赖），`Cycle` 给出闭环路径，首尾同名 |
| `*IncompatibleArtifactError` | 两个任务声明产出同一产物路径（`Task`/`Other`/`Artifact`） |
| `*UnknownToolError` | 任务引用了未注册的执行器 |

校验失败时 `Build` 返回的报告中 `Results` 为空：不会部分执行，也不会静默忽略。
同一任务重复声明同一依赖会被去重；产物路径在比较前做 `filepath.Clean`。

## 任务状态机

```
Pending ──► Waiting ──► Ready ──► Running ──► Succeeded        (终态)
              │           │         │
              │           │         ├──► Retrying ──► Running  （尝试失败、仍有重试额度）
              │           │         ├──► Failed                (终态：重试耗尽)
              │           │         └──► Canceled              (终态)
              └───────────┴──────────────────────► Canceled    (终态)
Running/Retrying 在取消时同样迁移到 Canceled
```

要点：

- `Pending→Waiting/Ready` 在调度启动时一次性确定；所有依赖成功后 `Waiting→Ready`。
- 每次尝试都从 `Running` 出发；失败且还有尝试额度时 `Running→Retrying→Running`，
  尝试次数记录在事件与 `TaskResult.Attempts` 中。
- 只有 `Succeeded`/`Failed`/`Canceled` 是终态（`TaskState.Terminal()`），终态不可再迁移。
- 每次转换都通过同一把状态锁完成并发出 `Event{Task, From, To, Attempt, Reason}`，
  因此日志可以完整解释“任务为什么处于当前状态”。

## 增量缓存与失效规则

缓存目录布局：

```
<CacheDir>/v<version>/<sha256(taskName)>/entry.json
<CacheDir>/v<version>/<sha256(taskName)>/outputs/<产物相对路径>
```

命中必须同时满足下列条件，任一不满足即失效并通过 `CacheDecision.Reason` 解释：

1. **版本兼容**：`entry.version == CacheVersion`，否则 `cache version mismatch`；
2. **输入摘要一致**：声明输入文件内容哈希（缺失文件以 `MISSING` 标记参与摘要），
   变化时原因为 `input digest changed`；
3. **参数摘要一致**：`Params` 的排序键值哈希变化时为 `params changed`；
4. **依赖键一致**：每个前置任务的*传递*缓存键（递归包含其输入与全部上游）变化时
   为 `dependency key changed`——任何上游变化都会沿链失效下游；
5. **快照完好**：缓存产物全部存在且摘要匹配；缺失 → `cached artifact missing: …`，
   内容被篡改 → `cached artifact corrupt: …`，元数据 JSON 损坏 → `cache entry corrupt …`。

损坏条目会被立即删除（自动失效），随后任务正常重新执行并重新写缓存。
缓存命中时把快照**原子地**恢复回工作区（临时文件 + rename）。

写入安全性：

- 缓存只在任务完全成功、且声明产物全部存在并校验后写入；
- 写入采用“临时目录 → 换入”，失败只留下临时目录，不会破坏既有成功条目；
- 因此**失败与重试绝不触碰缓存**，不会污染之前的好结果；
- 工作区产物写入同样是原子的；任务失败或取消后已写出的半成品产物会被清理。

## 并发与取消语义

- worker 池数量等于 `MaxConcurrency`（默认 `runtime.NumCPU()`，且不超过任务数），
  就绪任务通过带缓冲通道派发；同层任务按名称顺序获得确定的调度次序。
- 每个任务在任意时刻至多有一个 goroutine 执行；终态结算（结果写入 + 等待组递减）
  以幂等的 `settleTerminal` 统一收口，重复取消不会重复结算。
- `Handle.CancelTask(name)`：
  - `Running/Retrying`：取消该任务的 context，执行器应尽快返回；重试退避会立即中断；
  - `Waiting/Ready`：直接迁移到 `Canceled`；
  - 取消沿依赖边向**所有传递下游**传播，下游任务永远不会开始执行；
  - 与被取消任务无依赖关系的任务继续正常执行；
  - 对已终态任务调用是空操作（no-op）。
- 根 `context` 取消等价于取消全部非终态任务，`Report.Err` 为 `context.Canceled`。
- 取消/失败后：半成品产物被删除，下游全部 `Canceled`，不留下“不确定产物”，
  也不会让依赖它的任务继续。
- 任务失败（重试用尽）同样触发上述下游取消传播。

## 本地验证

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全量单元测试（测试日志会打印状态转换 [state] 与缓存判定 [cache]）
go test ./... -v

# 竞态检测（需 cgo，建议重复多次）
CGO_ENABLED=1 go test ./... -race -count=3

# 只跑某类场景
go test ./... -v -run TestValidation     # 循环/缺失/重复/产物冲突/未注册工具
go test ./... -v -run TestRetry          # 重试后成功、重试耗尽、缓存不被污染
go test ./... -v -run TestCache          # 输入/依赖变化、损坏、缺失、版本失效
go test ./... -v -run TestCancel         # 运行中/等待中取消、根取消、退避中取消
go test ./... -v -run TestCombined       # 并发取消 + 重试成功 + 缓存失效组合场景
```

测试覆盖的关键断言：

- 循环依赖返回闭环任务路径，且校验失败时没有任何任务被执行；
- 并发上限不被突破（自定义探针执行器统计同时在跑数）；
- 取消运行中任务后：本任务与传递下游均为 `Canceled`、下游从未 `Running`、
  无关任务成功、半成品产物不存在；
- 重试 2 次失败后成功：最终 `Succeeded`、`Attempts==3`、缓存仅在成功后写入一次；
- 缓存命中跳过执行；改输入/改上游/篡改快照/删快照/改版本分别触发对应失效原因并重新执行；
- 组合场景中最终状态、每任务执行次数、缓存判定原因与产物内容全部一致且可解释。
