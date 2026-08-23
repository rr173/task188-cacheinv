// Package versioning 维护源端版本状态：单调递增、逻辑时钟定序与副本观察版本转换。
package versioning

import (
	"fmt"
	"time"

	"task188-cacheinv/internal/model"
)

// Service 是版本模块的门面。
type Service struct {
	repo VersionRepo
	now  func() time.Time
}

// VersionRepo 是版本模块对存储的最小依赖。
type VersionRepo interface {
	UpsertSourceVersion(sv *model.SourceVersion) error
	GetSourceVersion(keyspaceID, key string) (*model.SourceVersion, error)
	ListSourceVersions(keyspaceID string) ([]model.SourceVersion, error)
	UpsertReplicaKeyVersion(rkv *model.ReplicaKeyVersion) error
	GetReplicaKeyVersion(replicaID, key string) (*model.ReplicaKeyVersion, error)
	ListReplicaKeyVersions(replicaID string) ([]model.ReplicaKeyVersion, error)
}

// NewService 构造版本模块。
func NewService(repo VersionRepo, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{repo: repo, now: now}
}

// ApplySourceUpdate 提交源更新：新版本必须严格大于当前源版本（拒绝版本倒退）。
// 返回写入后的源版本。
func (s *Service) ApplySourceUpdate(keyspaceID, key string, version int64, valueHash string) (*model.SourceVersion, error) {
	if version <= 0 {
		return nil, fmt.Errorf("%w: version must be positive", model.ErrInvalidInput)
	}
	if existing, err := s.repo.GetSourceVersion(keyspaceID, key); err == nil {
		if version <= existing.Version {
			return nil, fmt.Errorf("%w: key=%s proposed=%d current=%d",
				model.ErrVersionRegression, key, version, existing.Version)
		}
	} else if err != model.ErrNotFound {
		return nil, err
	}
	sv := &model.SourceVersion{
		KeyspaceID: keyspaceID,
		Key:        key,
		Version:    version,
		ValueHash:  valueHash,
		UpdatedAt:  s.now().UTC(),
	}
	if err := s.repo.UpsertSourceVersion(sv); err != nil {
		return nil, fmt.Errorf("upsert source version: %w", err)
	}
	return sv, nil
}

// GetSourceVersion 查询源端版本。
func (s *Service) GetSourceVersion(keyspaceID, key string) (*model.SourceVersion, error) {
	return s.repo.GetSourceVersion(keyspaceID, key)
}

// ListSourceVersions 列出键空间全部源版本。
func (s *Service) ListSourceVersions(keyspaceID string) ([]model.SourceVersion, error) {
	return s.repo.ListSourceVersions(keyspaceID)
}

// GetReplicaKeyVersion 查询副本对某键的观察版本。
func (s *Service) GetReplicaKeyVersion(replicaID, key string) (*model.ReplicaKeyVersion, error) {
	return s.repo.GetReplicaKeyVersion(replicaID, key)
}

// ObserveVersion 副本观察到一个版本（乱序保护的核心）：
//   - 若新观察版本 < 当前观察版本 → 返回 model.ErrVersionRegression（拒绝回滚）；
//   - 若新观察版本 == 当前观察版本 → 幂等，状态保持 valid/superseded；
//   - 否则推进到新版本并置为 valid（随后可被失效流转到 stale）。
func (s *Service) ObserveVersion(replicaID, key string, version int64) (*model.ReplicaKeyVersion, error) {
	if version <= 0 {
		return nil, fmt.Errorf("%w: version must be positive", model.ErrInvalidInput)
	}
	cur, err := s.repo.GetReplicaKeyVersion(replicaID, key)
	now := s.now().UTC()
	if err == model.ErrNotFound {
		rkv := &model.ReplicaKeyVersion{
			ReplicaID: replicaID,
			Key:       key,
			Version:   version,
			Status:    model.KeyValid,
			UpdatedAt: now,
		}
		if err := s.repo.UpsertReplicaKeyVersion(rkv); err != nil {
			return nil, err
		}
		return rkv, nil
	}
	if err != nil {
		return nil, err
	}
	if version < cur.Version {
		return nil, fmt.Errorf("%w: replica=%s key=%s proposed=%d observed=%d",
			model.ErrVersionRegression, replicaID, key, version, cur.Version)
	}
	if version == cur.Version {
		// 幂等观察：重复消息不改变状态。
		return cur, nil
	}
	status := model.KeyValid
	if cur.Status == model.KeyStale {
		status = model.KeyValid // 过期后补齐新版本
	}
	rkv := &model.ReplicaKeyVersion{
		ReplicaID: replicaID,
		Key:       key,
		Version:   version,
		Status:    status,
		UpdatedAt: now,
	}
	if err := s.repo.UpsertReplicaKeyVersion(rkv); err != nil {
		return nil, err
	}
	return rkv, nil
}

// InvalidateKey 处理失效消息：副本版本进入待失效/过期状态。
// 若失效目标版本低于副本当前版本，说明乱序失效先到，拒绝回滚（保持较新版本）。
func (s *Service) InvalidateKey(replicaID, key string, targetVersion int64) (*model.ReplicaKeyVersion, error) {
	cur, err := s.repo.GetReplicaKeyVersion(replicaID, key)
	now := s.now().UTC()
	if err == model.ErrNotFound {
		rkv := &model.ReplicaKeyVersion{
			ReplicaID: replicaID,
			Key:       key,
			Version:   targetVersion,
			Status:    model.KeyStale,
			UpdatedAt: now,
		}
		if err := s.repo.UpsertReplicaKeyVersion(rkv); err != nil {
			return nil, err
		}
		return rkv, nil
	}
	if err != nil {
		return nil, err
	}
	if targetVersion < cur.Version {
		// 乱序失效：目标版本比当前观察版本旧，不允许回滚新版本。
		return nil, fmt.Errorf("%w: stale invalidation replica=%s key=%s target=%d observed=%d",
			model.ErrVersionRegression, replicaID, key, targetVersion, cur.Version)
	}
	status := model.KeyStale
	if cur.Status == model.KeyValid && cur.Version == targetVersion {
		status = model.KeyPendingInvalidation
	}
	rkv := &model.ReplicaKeyVersion{
		ReplicaID: replicaID,
		Key:       key,
		Version:   targetVersion,
		Status:    status,
		UpdatedAt: now,
	}
	if err := s.repo.UpsertReplicaKeyVersion(rkv); err != nil {
		return nil, err
	}
	return rkv, nil
}

// ConfirmInvalidation 副本确认失效完成，版本进入 stale 等待新版本。
func (s *Service) ConfirmInvalidation(replicaID, key string) (*model.ReplicaKeyVersion, error) {
	cur, err := s.repo.GetReplicaKeyVersion(replicaID, key)
	if err != nil {
		return nil, err
	}
	rkv := &model.ReplicaKeyVersion{
		ReplicaID: replicaID,
		Key:       key,
		Version:   cur.Version,
		Status:    model.KeyStale,
		UpdatedAt: s.now().UTC(),
	}
	if err := s.repo.UpsertReplicaKeyVersion(rkv); err != nil {
		return nil, err
	}
	return rkv, nil
}

// IsConvergedFor 判断副本对某键是否与源端一致（版本相同且状态有效）。
func (s *Service) IsConvergedFor(replicaID, key string, sourceVersion int64) bool {
	rkv, err := s.repo.GetReplicaKeyVersion(replicaID, key)
	if err != nil {
		return false
	}
	return rkv.Version == sourceVersion && (rkv.Status == model.KeyValid || rkv.Status == model.KeySuperseded)
}

// SupersedeKey 标记副本某键已被更高版本替代（保留版本号）。
func (s *Service) SupersedeKey(replicaID, key string, version int64) error {
	now := s.now().UTC()
	return s.repo.UpsertReplicaKeyVersion(&model.ReplicaKeyVersion{
		ReplicaID: replicaID,
		Key:       key,
		Version:   version,
		Status:    model.KeySuperseded,
		UpdatedAt: now,
	})
}
