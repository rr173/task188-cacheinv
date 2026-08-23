package replay

import (
	"testing"

	"task188-cacheinv/internal/model"
)

func TestResolveOnlyReusesFinishedDifferentScenario(t *testing.T) {
	finished := &model.Scenario{ID: 2, Status: model.ScenarioConverged}
	if !Resolve(&model.Scenario{ID: 1}, finished) {
		t.Fatal("expected converged scenario to be reusable")
	}
	if Resolve(&model.Scenario{ID: 2}, finished) {
		t.Fatal("a scenario must not reuse itself")
	}
	if Resolve(&model.Scenario{ID: 1}, &model.Scenario{ID: 2, Status: model.ScenarioTimeoutNotConverged}) {
		t.Fatal("unfinished scenario must not be reused")
	}
}

func TestMergeUnconvergedSortsAndDeduplicates(t *testing.T) {
	got := MergeUnconverged([]string{"replica-b", "replica-a"}, []string{"replica-c", "replica-a"})
	want := []string{"replica-a", "replica-b", "replica-c"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}
