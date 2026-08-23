package versioning

import (
	"testing"
	"time"

	"task188-cacheinv/internal/model"
)

// TestInvalidateAlreadyObservedVersionKeepsMonotonic 验证 bug 修复：
// 副本已观察到某版本后，再收到同一版本的失效消息，不得当成版本回退拒绝；
// 应保留版本单调性，并进入待确认失效（pending_invalidation）。
func TestInvalidateAlreadyObservedVersionKeepsMonotonic(t *testing.T) {
	svc := NewService(newFakeRepo(), time.Now)
	if _, err := svc.ObserveVersion("r1", "k", 10); err != nil {
		t.Fatalf("observe v10: %v", err)
	}
	// 失效已观察到的同一版本：不应回退、不应报错。
	rkv, err := svc.InvalidateKey("r1", "k", 10)
	if err != nil {
		t.Fatalf("invalidate same observed version must not be rejected, got: %v", err)
	}
	if rkv.Version != 10 {
		t.Fatalf("version must not regress, got %d", rkv.Version)
	}
	if rkv.Status != model.KeyPendingInvalidation {
		t.Fatalf("expected pending_invalidation, got %s", rkv.Status)
	}
	// 重复失效同版本应幂等：状态保持 pending_invalidation，不回退。
	rkv2, err := svc.InvalidateKey("r1", "k", 10)
	if err != nil {
		t.Fatalf("idempotent re-invalidate: %v", err)
	}
	if rkv2.Status != model.KeyPendingInvalidation {
		t.Fatalf("expected idempotent pending_invalidation, got %s", rkv2.Status)
	}
	// 确认失效后仍可推进到更高版本（乱序补齐），不构成回退。
	rkv3, err := svc.ConfirmInvalidation("r1", "k")
	if err != nil {
		t.Fatalf("confirm: %v", err)
	}
	if rkv3.Status != model.KeyStale {
		t.Fatalf("expected stale after confirm, got %s", rkv3.Status)
	}
	if _, err := svc.ObserveVersion("r1", "k", 12); err != nil {
		t.Fatalf("observe forward after invalidation: %v", err)
	}
}
