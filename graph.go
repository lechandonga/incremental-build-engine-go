package buildengine

import (
	"path/filepath"
	"sort"
)

// graph.go 负责从声明式配置构建任务图并执行严格校验：
// 重复任务、缺失依赖、循环依赖、产物冲突、未注册工具。
// 任一校验失败都返回可区分类型的错误，且不会产生可执行的图。

// taskNode 是图中的一个任务节点。
type taskNode struct {
	spec *TaskSpec
	deps []*taskNode // 前置依赖节点
	next []*taskNode // 下游节点（谁依赖我）
}

// graph 是校验通过后的任务图。
type graph struct {
	nodes map[string]*taskNode
	order []string // 拓扑顺序（依赖在前）
}

// buildGraph 从配置构建任务图。校验顺序是确定的：
// 重复任务 -> 缺失依赖/未注册工具 -> 产物冲突 -> 循环依赖。
func buildGraph(cfg Config, reg *registry) (*graph, error) {
	// 1. 重复任务（按首次出现位置报告）。
	seen := make(map[string]bool, len(cfg.Tasks))
	for _, t := range cfg.Tasks {
		if seen[t.Name] {
			return nil, &DuplicateTaskError{Task: t.Name}
		}
		seen[t.Name] = true
	}

	nodes := make(map[string]*taskNode, len(cfg.Tasks))
	for i := range cfg.Tasks {
		nodes[cfg.Tasks[i].Name] = &taskNode{spec: &cfg.Tasks[i]}
	}

	// 2. 缺失依赖与未注册工具；同时连边（去重）。
	// 按任务名排序处理，保证错误定位的确定性。
	names := make([]string, 0, len(nodes))
	for name := range nodes {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		node := nodes[name]
		if _, ok := reg.get(node.spec.Tool); !ok {
			return nil, &UnknownToolError{Task: name, Tool: node.spec.Tool}
		}
		depSeen := make(map[string]bool, len(node.spec.Deps))
		for _, dep := range node.spec.Deps {
			if depSeen[dep] {
				continue
			}
			depSeen[dep] = true
			depNode, ok := nodes[dep]
			if !ok {
				return nil, &MissingDependencyError{Task: name, Dependency: dep}
			}
			node.deps = append(node.deps, depNode)
			depNode.next = append(depNode.next, node)
		}
	}

	// 3. 产物冲突：两个任务声明产出同一（清洗后的）路径。
	producer := make(map[string]string)
	for _, name := range names {
		node := nodes[name]
		outSeen := make(map[string]bool, len(node.spec.Outputs))
		for _, out := range node.spec.Outputs {
			clean := filepath.Clean(out)
			if outSeen[clean] {
				continue
			}
			outSeen[clean] = true
			if other, conflict := producer[clean]; conflict {
				return nil, &IncompatibleArtifactError{Task: name, Other: other, Artifact: clean}
			}
			producer[clean] = name
		}
	}

	// 4. 循环依赖（DFS 染色并重建环路径）。
	if cycle := findCycle(nodes, names); cycle != nil {
		return nil, &CyclicDependencyError{Cycle: cycle}
	}

	// 5. 拓扑排序（Kahn），同层按名字排序保证确定性。
	indeg := make(map[string]int, len(nodes))
	for name, node := range nodes {
		indeg[name] = len(node.deps)
	}
	var order []string
	frontier := []string{}
	for _, name := range names {
		if indeg[name] == 0 {
			frontier = append(frontier, name)
		}
	}
	for len(frontier) > 0 {
		sort.Strings(frontier)
		name := frontier[0]
		frontier = frontier[1:]
		order = append(order, name)
		nextNames := make([]string, 0, len(nodes[name].next))
		for _, nxt := range nodes[name].next {
			nextNames = append(nextNames, nxt.spec.Name)
		}
		sort.Strings(nextNames)
		for _, nn := range nextNames {
			indeg[nn]--
			if indeg[nn] == 0 {
				frontier = append(frontier, nn)
			}
		}
	}

	return &graph{nodes: nodes, order: order}, nil
}

// findCycle 使用 DFS 三色标记检测环，返回环上任务名（首尾同名为闭环）。
func findCycle(nodes map[string]*taskNode, names []string) []string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(nodes))
	var stack []string

	var dfs func(name string) []string
	dfs = func(name string) []string {
		color[name] = gray
		stack = append(stack, name)
		// 固定下游遍历顺序，环报告稳定。
		nexts := append([]*taskNode(nil), nodes[name].next...)
		sort.Slice(nexts, func(i, j int) bool { return nexts[i].spec.Name < nexts[j].spec.Name })
		for _, nxt := range nexts {
			switch color[nxt.spec.Name] {
			case white:
				if cyc := dfs(nxt.spec.Name); cyc != nil {
					return cyc
				}
			case gray:
				// 从栈中 nxt 首次出现处截取，构成闭环。
				start := 0
				for i, n := range stack {
					if n == nxt.spec.Name {
						start = i
						break
					}
				}
				cyc := append([]string(nil), stack[start:]...)
				cyc = append(cyc, nxt.spec.Name)
				return cyc
			}
		}
		stack = stack[:len(stack)-1]
		color[name] = black
		return nil
	}

	for _, name := range names {
		if color[name] == white {
			if cyc := dfs(name); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}
