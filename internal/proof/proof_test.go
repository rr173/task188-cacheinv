package proof

import (
	"testing"

	"task188-cacheinv/internal/model"
)

func TestCheckMonotonicObservationsFindsRegression(t *testing.T) {
	msgs := []model.Message{
		{MsgID: "new", Kind: model.MsgUpdate, Key: "k", Version: 8, ReplicaID: "r1"},
		{MsgID: "old", Kind: model.MsgInvalidate, Key: "k", Version: 7, ReplicaID: "r1"},
	}
	problems := CheckMonotonicObservations(msgs)
	if len(problems) != 1 {
		t.Fatalf("got %d problems: %v", len(problems), problems)
	}
	if problems[0] == "" {
		t.Fatal("regression report must describe the offending message")
	}
}

func TestLocateViolationStepsOrdersBySequence(t *testing.T) {
	viols := []model.Violation{
		{StepSeq: 3, Kind: "retry", Message: "third"},
		{StepSeq: 1, Kind: "ack", Message: "first"},
	}
	got := LocateViolationSteps(viols)
	if len(got) != 2 || got[0] != "step 1 [ack] first" || got[1] != "step 3 [retry] third" {
		t.Fatalf("ordered violations = %v", got)
	}
}
