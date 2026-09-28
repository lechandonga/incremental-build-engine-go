package buildengine

import (
	"context"
	"errors"
	"testing"
)

// makeValidTask 构造一个最小合法任务。
func makeValidTask(name string, deps ...string) TaskSpec {
	return TaskSpec{
		Name:    name,
		Tool:    FileRunnerName,
		Deps:    deps,
		Outputs: []string{"out/" + name + ".txt"},
	}
}

func TestValidationDuplicateTask(t *testing.T) {
	cfg := Config{
		Tasks: []TaskSpec{makeValidTask("a"), makeValidTask("a")},
	}
	rep := NewEngine().Build(context.Background(), cfg)
	var dup *DuplicateTaskError
	if !errors.As(rep.Err, &dup) {
		t.Fatalf("expected *DuplicateTaskError, got %v", rep.Err)
	}
	if dup.TaskName() != "a" {
		t.Fatalf("expected task a, got %s", dup.TaskName())
	}
	if len(rep.Results) != 0 {
		t.Fatalf("validation failure must not execute any task, results=%v", rep.Results)
	}
}

func TestValidationMissingDependency(t *testing.T) {
	cfg := Config{Tasks: []TaskSpec{makeValidTask("a", "ghost")}}
	rep := NewEngine().Build(context.Background(), cfg)
	var miss *MissingDependencyError
	if !errors.As(rep.Err, &miss) {
		t.Fatalf("expected *MissingDependencyError, got %v", rep.Err)
	}
	if miss.Task != "a" || miss.Dependency != "ghost" {
		t.Fatalf("unexpected error payload: %+v", miss)
	}
}

func TestValidationCyclicDependency(t *testing.T) {
	// a -> b -> c -> a
	cfg := Config{Tasks: []TaskSpec{
		makeValidTask("a", "c"),
		makeValidTask("b", "a"),
		makeValidTask("c", "b"),
	}}
	rep := NewEngine().Build(context.Background(), cfg)
	var cyc *CyclicDependencyError
	if !errors.As(rep.Err, &cyc) {
		t.Fatalf("expected *CyclicDependencyError, got %v", rep.Err)
	}
	if len(cyc.Cycle) < 2 || cyc.Cycle[0] != cyc.Cycle[len(cyc.Cycle)-1] {
		t.Fatalf("cycle must be a closed loop, got %v", cyc.Cycle)
	}
	t.Logf("detected cycle path: %v", cyc.Cycle)
}

func TestValidationSelfCycle(t *testing.T) {
	cfg := Config{Tasks: []TaskSpec{makeValidTask("a", "a")}}
	rep := NewEngine().Build(context.Background(), cfg)
	var cyc *CyclicDependencyError
	if !errors.As(rep.Err, &cyc) {
		t.Fatalf("expected *CyclicDependencyError for self dependency, got %v", rep.Err)
	}
}

func TestValidationIncompatibleArtifacts(t *testing.T) {
	a := makeValidTask("a")
	a.Outputs = []string{"shared/x.txt"}
	b := makeValidTask("b")
	b.Outputs = []string{"shared/x.txt"}
	cfg := Config{Tasks: []TaskSpec{a, b}}
	rep := NewEngine().Build(context.Background(), cfg)
	var inc *IncompatibleArtifactError
	if !errors.As(rep.Err, &inc) {
		t.Fatalf("expected *IncompatibleArtifactError, got %v", rep.Err)
	}
	if inc.Artifact != "shared/x.txt" {
		t.Fatalf("unexpected artifact: %s", inc.Artifact)
	}
	t.Logf("conflict tasks: %s vs %s on %s", inc.Task, inc.Other, inc.Artifact)
}

func TestValidationUnknownTool(t *testing.T) {
	task := makeValidTask("a")
	task.Tool = "nonexistent-tool"
	cfg := Config{Tasks: []TaskSpec{task}}
	rep := NewEngine().Build(context.Background(), cfg)
	var unk *UnknownToolError
	if !errors.As(rep.Err, &unk) {
		t.Fatalf("expected *UnknownToolError, got %v", rep.Err)
	}
	if unk.Tool != "nonexistent-tool" || unk.Task != "a" {
		t.Fatalf("unexpected error payload: %+v", unk)
	}
}

func TestValidationErrorsAreDistinguishable(t *testing.T) {
	// 同一错误不应同时匹配不同类型，保证调用方可以精确分支。
	checks := []error{
		&DuplicateTaskError{Task: "a"},
		&MissingDependencyError{Task: "a", Dependency: "b"},
		&CyclicDependencyError{Cycle: []string{"a", "a"}},
		&IncompatibleArtifactError{Task: "a", Other: "b", Artifact: "x"},
		&UnknownToolError{Task: "a", Tool: "t"},
	}
	for _, err := range checks {
		var v ValidationError
		if !errors.As(err, &v) {
			t.Fatalf("%T should satisfy ValidationError", err)
		}
	}
}
