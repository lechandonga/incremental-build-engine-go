# Incremental Build Engine (Go)

一个可复用的增量构建执行组件：从**声明式配置**构建任务图，按依赖顺序**并发执行**，
基于**输入摘要 + 前置任务产物摘要**做增量缓存，并用**本地文件与普通 Go 函数（进程内
“任务执行体”）**模拟构建产物与依赖。所有关键行为（状态转换、缓存命中/失效原因）都可通过
`Logger` 解释，且在依赖变化、并发取消、失败重试下结果保持稳定。

## 安装与最小示例

```go
import buildengine "github.com/lechandonga/incremental-build-engine-go"

eng := buildengine.NewEngine(
    buildengine.WithCacheDir(filepath.Join(t.TempDir(), "cache")), // 增量缓存目录
    buildengine.WithMaxConcurrency(4),                             // 最大并发
    buildengine.WithLogger(log.New(os.Stderr, "", log.LstdFlags)), // 状态/缓存日志
)

res, err := eng.Build(ctx, []buildengine.TaskSpec{{
    Name:    "compile",
    Version: 1,                                   // 任务实现版本
    Inputs:  map[string]string{"src": "..."},     // 任意声明式输入
    Produces: []buildengine.ArtifactSpec{{Name: "main.o", Kind: "object"}},
    Run: func(ctx context.Context, rt *buildengine.Runtime) error {
        // 必须把声明的产物写到 rt.StagingDir() 下
        return os.WriteFile(filepath.Join(rt.StagingDir(), "main.o"), obj, 0o644)
    },
}, {
    Name:      "link",
    DependsOn: []string{"compile"},
    Consumes:  []buildengine.ArtifactRef{{Name: "main.o", WantKind: "object"}},
    Produces:  []buildengine.ArtifactSpec{{Name: "app", Kind: "binary"}},
    Run: func(ctx context.Context, rt *buildengine.Runtime) error {
        src, _ := rt.ArtifactPath("main.o") // 依赖产物的只读路径
        // ... 生成 app 到 rt.StagingDir()/app ...
    },
}})
```

## 配置校验（先校验、后执行）

`Build` 在执行任何任务**之前**聚合全部配置错误并返回 `*ValidationErrors`；校验不通过时
不会有任何任务被部分执行或静默忽略。每条错误都是带任务定位的 `*ValidationError`，
用 `Kind` 区分：

| Kind | 含义 | 定位字段 |
| --- | --- | --- |
| `duplicate_task` | 同名任务声明了多次 | `Task` |
| `missing_dependency` | 依赖了不存在的任务（或消费了某产物却未依赖其产出任务） | `Task` + `Other` |
| `cycle` | 依赖图存在环 | `Detail` 中给出具体环路径，如 `a -> b -> c -> a` |
| `duplicate_artifact` | 同一产物名被多个任务产出 | `Task` / `Other` / `Detail=产物名` |
| `missing_artifact` | 消费的产物没有任何任务产出 | `Task` / `Detail=产物名` |
| `incompatible_artifact` | 消费方 `WantKind` 与产出方 `Kind` 不匹配 | `Task` / `Other` / `Detail` |

可用 `errors.As(err, &ve)` 配合 `ve.HasKind(kind)` 精确分支处理。

## 任务状态机

```
                 ┌──────────────── 缓存命中 ────────────────┐
                 ▼                                          │
 pending ──► waiting ───────────────► running ──► success ──┘（等同成功：skipped）
               │  ▲                    │  │
               │  │  可重试失败        │  ├─(attempt <= MaxRetries)─► retrying ─┘
               │  └────────────────────┘  │
               │                          ├─(重试耗尽)────────────────► failed
               │                          └─(ctx 取消/ctx.Err()!=nil)─► canceled
               └─(依赖失败或被取消)──────────────────────────────────► canceled
```

要点：

- `pending`/`waiting` 为执行前状态：任务先入图等待直接依赖全部到达终态。
- `running` 为任务体实际执行；同一次构建中**每次重试是一次新的 `running`**，
  之间经 `retrying` 显式转换，日志可观察到。
- 终态四个：`success`、`skipped`（命中缓存，未执行任务体）、`failed`、`canceled`。
  可通过 `TaskStatus.IsTerminal()` 判断。
- 所有状态转换都在互斥锁保护下写入 `TaskResult`，并经 `Logger.Printf` 输出
  （形如 `task=link transition: waiting -> running (attempt=1)`）。

## 并发与取消语义

- 所有任务 goroutine 按拓扑序一次性启动，但每个任务必须等待其**直接依赖到达终态**
  才能继续；互不相关的任务自然并行。
- 任务体执行受 `WithMaxConcurrency(n)` 的信号量约束（`n<=0` 表示不限并发）。
  等待信号量期间若被取消，任务体不会再启动。
- 取消来源有两种：
  1. **外部取消**：调用方 `context` 被取消；
  2. **级联取消**：任一任务终态失败时引擎内部 cancel 整个构建。
- 级联规则：依赖失败/取消的任务一律转为 `canceled` 且**绝不执行**；
  任务体收到取消后，即使忽略 `ctx` 返回了 nil，引擎也**不提交产物**。
- 每次尝试都使用独立的**全新暂存目录**；取消/失败后暂存目录被删除，
  `TaskResult.OutputDir` 为空，因此取消不会留下不确定产物。
- `Build` 总是在所有 goroutine 到达终态后才返回；出错时同时返回
  部分 `*BuildResult`（含每个任务的终态、尝试次数、错误），便于解释。

## 增量缓存与失效规则

缓存条目布局：

```
<cache-dir>/<sha256-key>/meta.json
<cache-dir>/<sha256-key>/art/<artifact-name...>
```

**缓存 key 的判定依据**（日志中会打印）：

- 任务名与任务 `Version`（实现变了 → key 变）；
- `InputDigest`：对 `Inputs` 按键排序后与版本一起做 SHA-256；
- `DepDigest`：对**直接依赖任务的全部产物文件内容**按（依赖名, 产物名）排序流式
  SHA-256 后聚合。输入没变但上游产物变了，下游 key 同样变化 → 级联重跑；
- 缓存格式版本 `cacheFormatVersion`：组件布局升级后旧缓存整体不兼容。

**自动失效（绝不复用旧结果）**——`lookup` 时逐条检查，命中任一条件即删除坏条目并按
MISS 重新执行：

1. 条目/`meta.json` 不存在（正常冷缓存）；
2. `meta.json` JSON 损坏；
3. `format_version` 与当前组件不一致；
4. `task_version`、输入摘要、依赖摘要或 key 任一与当前规格不符；
5. 任一**声明产物在磁盘上缺失或不是普通文件**。

**重试不污染成功结果**：任务成功后，引擎先把暂存产物复制到同级 `.tmp-*` 临时条目，
校验全部声明产物齐全、写好 `meta.json`，最后 `rename` 原子替换。失败尝试的暂存目录
立即删除，已有的成功条目在重试期间始终保持可读；崩溃最多留下“无缓存”，不会产生
半成品命中（写入路径为：临时目录 → 校验 → 原子 rename）。

## 本地验证

```bash
# 全部单测（竞态检测需要 CGO；无 CGO 时去掉 -race）
CGO_ENABLED=1 go test -race ./...

# 查看任务状态转换与缓存判定日志（HIT/MISS/COMMIT 及原因）
go test -v ./...

go vet ./...
gofmt -l .   # 应无输出
```

测试覆盖：

- 六类配置校验（`graph_test.go`），并断言校验失败时零任务执行；
- 最大并发限制、失败级联取消、外部 context 取消（`executor_test.go`，`-race` 下验证）；
- 缓存命中跳过、输入/依赖变化重跑、meta 损坏、产物缺失、格式版本不兼容自动失效
  （`cache_test.go`）；
- 重试后成功、重试耗尽级联取消、失败尝试不污染缓存（`retry_test.go`）；
- 组合场景：重试成功 → 全量命中 → 输入变化级联失效重跑 → 并发外部取消，
  并断言**最终状态、各任务执行次数与产物内容**一致（`integration_test.go`）。
