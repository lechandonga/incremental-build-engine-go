package buildengine

import (
	"context"
	"errors"
	"testing"
)

func noopRun(ctx context.Context, rt *Runtime) error { return nil }

func TestBuildGraphValid(t *testing.T) {
	specs := []TaskSpec{
		{Name: "a", Produces: []ArtifactSpec{{Name: "a.out", Kind: "text"}}, Run: noopRun},
		{Name: "b", DependsOn: []string{"a"},
			Produces: []ArtifactSpec{{Name: "b.out", Kind: "text"}},
			Consumes: []ArtifactRef{{Name: "a.out", WantKind: "text"}}, Run: noopRun},
	}
	g, err := BuildGraph(specs)
	if err != nil {
		t.Fatalf("expected valid graph, got %v", err)
	}
	if len(g.order) != 2 || g.order[0] != "a" || g.order[1] != "b" {
		t.Fatalf("unexpected topological order: %v", g.order)
	}
	if p, ok := g.ProducerOf("a.out"); !ok || p != "a" {
		t.Fatalf("producer lookup wrong: %q %v", p, ok)
	}
}

func expectKind(t *testing.T, err error, kind ValidationKind) *ValidationErrors {
	t.Helper()
	var ve *ValidationErrors
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationErrors, got %T: %v", err, err)
	}
	if !ve.HasKind(kind) {
		t.Fatalf("expected validation kind %q in errors: %v", kind, ve)
	}
	return ve
}

func TestValidationDuplicateTask(t *testing.T) {
	_, err := BuildGraph([]TaskSpec{
		{Name: "a", Run: noopRun},
		{Name: "a", Run: noopRun},
	})
	expectKind(t, err, ValidationDuplicateTask)
}

func TestValidationMissingDependency(t *testing.T) {
	_, err := BuildGraph([]TaskSpec{
		{Name: "a", DependsOn: []string{"ghost"}, Run: noopRun},
	})
	ve := expectKind(t, err, ValidationMissingDependency)
	found := false
	for _, e := range ve.Errs {
		if e.Kind == ValidationMissingDependency && e.Task == "a" && e.Other == "ghost" {
			found = true
		}
	}
	if !found {
		t.Fatalf("error should locate task=a dep=ghost: %v", ve)
	}
}

func TestValidationCycle(t *testing.T) {
	_, err := BuildGraph([]TaskSpec{
		{Name: "a", DependsOn: []string{"c"}, Run: noopRun},
		{Name: "b", DependsOn: []string{"a"}, Run: noopRun},
		{Name: "c", DependsOn: []string{"b"}, Run: noopRun},
	})
	ve := expectKind(t, err, ValidationCycle)
	for _, e := range ve.Errs {
		t.Logf("validation error: %v", e)
	}
}

func TestValidationDuplicateArtifact(t *testing.T) {
	_, err := BuildGraph([]TaskSpec{
		{Name: "a", Produces: []ArtifactSpec{{Name: "x.out", Kind: "text"}}, Run: noopRun},
		{Name: "b", Produces: []ArtifactSpec{{Name: "x.out", Kind: "text"}}, Run: noopRun},
	})
	ve := expectKind(t, err, ValidationDuplicateArtifact)
	for _, e := range ve.Errs {
		if e.Kind == ValidationDuplicateArtifact && e.Detail != "x.out" {
			t.Fatalf("artifact name not located: %v", e)
		}
	}
}

func TestValidationMissingAndIncompatibleArtifact(t *testing.T) {
	// 缺失：b 消费 nobody.out
	_, err := BuildGraph([]TaskSpec{
		{Name: "a", Produces: []ArtifactSpec{{Name: "a.out", Kind: "json"}}, Run: noopRun},
		{Name: "b", DependsOn: []string{"a"},
			Consumes: []ArtifactRef{{Name: "a.out", WantKind: "text"}, {Name: "nobody.out"}}, Run: noopRun},
	})
	ve := expectKind(t, err, ValidationIncompatibleArtifact)
	if !ve.HasKind(ValidationMissingArtifact) {
		t.Fatalf("expected also missing artifact error: %v", ve)
	}
	for _, e := range ve.Errs {
		t.Logf("validation error: %v", e)
	}
}

func TestValidationFailureRunsNothing(t *testing.T) {
	// 校验失败时 Build 必须不执行任何任务。
	ran := false
	_, err := NewEngine().Build(context.Background(), []TaskSpec{
		{Name: "a", DependsOn: []string{"a"}, Run: func(ctx context.Context, rt *Runtime) error {
			ran = true
			return nil
		}},
	})
	expectKind(t, err, ValidationCycle)
	if ran {
		t.Fatal("no task must run when validation fails")
	}
}
