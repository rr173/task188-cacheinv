package delivery

import (
	"testing"
	"time"
)

func TestGrantAndValidateAck(t *testing.T) {
	m := NewManager(time.Now)
	m.GrantLease("r1", "k", 7, 100*time.Millisecond)

	st, err := m.ValidateAck("r1", "k", 7, true)
	if err != nil || st.Kind != "ok" {
		t.Fatalf("expected ok ack, got %+v err=%v", st, err)
	}
	// 版本不匹配。
	st, err = m.ValidateAck("r1", "k", 6, true)
	if err != nil {
		t.Fatalf("version mismatch should not be hard error: %v", err)
	}
	if st.Kind != "version_mismatch" {
		t.Fatalf("expected version_mismatch, got %s", st.Kind)
	}
	// 无租约。
	if _, err := m.ValidateAck("r2", "k", 7, true); err == nil {
		t.Fatal("expected ack without lease error")
	}
	// 未知副本。
	if _, err := m.ValidateAck("ghost", "k", 7, false); err == nil {
		t.Fatal("expected unknown replica error")
	}
}

func TestLeaseExpiry(t *testing.T) {
	base := time.Now()
	now := func() time.Time { return base }
	m := NewManager(now)
	m.GrantLease("r1", "k", 1, 50*time.Millisecond)

	// 快进时间源。
	now = func() time.Time { return base.Add(100 * time.Millisecond) }
	m.now = now
	if _, err := m.ValidateAck("r1", "k", 1, true); err == nil {
		t.Fatal("expected expired lease error")
	}
}

func TestRetryBound(t *testing.T) {
	if d := DecideRetry(3, 3); !d.Allowed {
		t.Fatalf("retry 3 of max 3 should be allowed: %s", d.Reason)
	}
	if d := DecideRetry(4, 3); d.Allowed {
		t.Fatal("retry 4 of max 3 should be rejected")
	}
}
