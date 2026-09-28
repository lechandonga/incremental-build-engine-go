package buildengine

import "fmt"

// Graph 是通过全部校验的任务依赖图。
type Graph struct {
	tasks map[string]*TaskSpec
	// deps[t] = t 直接依赖的任务名列表（声明顺序）。
	deps map[string][]string
	// order 为 Kahn 拓扑序；同一段内无依赖关系的任务可并行执行。
	order []string
	// producer[artifactName] = 产出该产物的任务名。
	producer map[string]string
}

// ProducerOf 返回产物名对应的产出任务。
func (g *Graph) ProducerOf(artifact string) (string, bool) {
	t, ok := g.producer[artifact]
	return t, ok
}

// BuildGraph 校验声明式配置并构建任务图。
// 发现的所有校验问题会一次性聚合到 *ValidationErrors 中返回；
// 校验不通过时返回 nil 图，调用方不得执行任何任务。
func BuildGraph(specs []TaskSpec) (*Graph, error) {
	verrs := &ValidationErrors{}
	g := &Graph{
		tasks:    make(map[string]*TaskSpec),
		deps:     make(map[string][]string),
		producer: make(map[string]string),
	}

	// 1) 重复任务校验。
	for i := range specs {
		s := &specs[i]
		if _, exists := g.tasks[s.Name]; exists {
			verrs.Errs = append(verrs.Errs, &ValidationError{
				Kind: ValidationDuplicateTask,
				Task: s.Name,
			})
			continue
		}
		g.tasks[s.Name] = s
	}
	// 2) 缺失依赖校验（同时去重复制依赖列表）。
	for _, s := range specs {
		seen := map[string]bool{}
		for _, d := range s.DependsOn {
			if seen[d] {
				continue
			}
			seen[d] = true
			if _, ok := g.tasks[d]; !ok {
				verrs.Errs = append(verrs.Errs, &ValidationError{
					Kind:  ValidationMissingDependency,
					Task:  s.Name,
					Other: d,
				})
				continue
			}
			g.deps[s.Name] = append(g.deps[s.Name], d)
		}
	}

	// 3) 产物权责表：产物名 -> 产出任务与其 Kind。
	type producer struct {
		task string
		kind string
	}
	artifactProducer := map[string]producer{}
	for _, s := range specs {
		for _, a := range s.Produces {
			if p, ok := artifactProducer[a.Name]; ok {
				verrs.Errs = append(verrs.Errs, &ValidationError{
					Kind:   ValidationDuplicateArtifact,
					Task:   p.task,
					Other:  s.Name,
					Detail: a.Name,
				})
				continue
			}
			artifactProducer[a.Name] = producer{task: s.Name, kind: a.Kind}
			g.producer[a.Name] = s.Name
		}
	}

	// 4)+5) 消费产物：必须存在产出方，且 Kind 必须兼容。
	for _, s := range specs {
		for _, ref := range s.Consumes {
			p, ok := artifactProducer[ref.Name]
			if !ok {
				verrs.Errs = append(verrs.Errs, &ValidationError{
					Kind:   ValidationMissingArtifact,
					Task:   s.Name,
					Detail: ref.Name,
				})
				continue
			}
			if ref.WantKind != "" && ref.WantKind != p.kind {
				verrs.Errs = append(verrs.Errs, &ValidationError{
					Kind:  ValidationIncompatibleArtifact,
					Task:  s.Name,
					Other: p.task,
					Detail: fmt.Sprintf("artifact %q want kind %q, producer provides %q",
						ref.Name, ref.WantKind, p.kind),
				})
			}
			// 消费某产物的任务必须（传递）依赖产出方；这里校验直接依赖，
			// 传递依赖由拓扑执行顺序天然保证。
			if !contains(g.deps[s.Name], p.task) {
				verrs.Errs = append(verrs.Errs, &ValidationError{
					Kind:   ValidationMissingDependency,
					Task:   s.Name,
					Other:  p.task,
					Detail: "producer of artifact " + ref.Name,
				})
			}
		}
	}

	// 6) 循环依赖检测：Kahn 算法，未被剥离的节点必在环上；
	// 再用 DFS 抽出一条具体的环路径用于错误定位。
	indeg := make(map[string]int, len(g.tasks))
	children := make(map[string][]string, len(g.tasks))
	for name := range g.tasks {
		indeg[name] = 0
	}
	for t, ds := range g.deps {
		indeg[t] = len(ds)
		for _, d := range ds {
			children[d] = append(children[d], t)
		}
	}
	queue := []string{}
	for name, n := range indeg {
		if n == 0 {
			queue = append(queue, name)
		}
	}
	visited := 0
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		visited++
		g.order = append(g.order, cur)
		for _, ch := range children[cur] {
			indeg[ch]--
			if indeg[ch] == 0 {
				queue = append(queue, ch)
			}
		}
	}
	if visited != len(g.tasks) {
		cycle := findCycle(g.tasks, g.deps)
		verrs.Errs = append(verrs.Errs, &ValidationError{
			Kind:   ValidationCycle,
			Task:   firstOrEmpty(cycle),
			Detail: formatCycle(cycle),
		})
	}

	if len(verrs.Errs) > 0 {
		return nil, verrs
	}
	return g, nil
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

// findCycle 用 DFS 三色标记从任一在环节点提取一条环路径。
func findCycle(tasks map[string]*TaskSpec, deps map[string][]string) []string {
	const white, gray, black = 0, 1, 2
	color := map[string]int{}
	var stack []string

	var dfs func(string) []string
	dfs = func(u string) []string {
		color[u] = gray
		stack = append(stack, u)
		for _, v := range deps[u] {
			if _, ok := tasks[v]; !ok {
				continue // 缺失依赖已单独报错
			}
			switch color[v] {
			case gray:
				// 找到回边，从栈中切出环：v ... u -> v
				for i, x := range stack {
					if x == v {
						cyc := append([]string{}, stack[i:]...)
						return append(cyc, v)
					}
				}
			case white:
				if cyc := dfs(v); cyc != nil {
					return cyc
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[u] = black
		return nil
	}

	for name := range tasks {
		if color[name] == white {
			if cyc := dfs(name); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

func formatCycle(cycle []string) string {
	if len(cycle) == 0 {
		return "<unknown>"
	}
	out := ""
	for i, t := range cycle {
		if i > 0 {
			out += " -> "
		}
		out += t
	}
	return out
}

func firstOrEmpty(xs []string) string {
	if len(xs) == 0 {
		return ""
	}
	return xs[0]
}
