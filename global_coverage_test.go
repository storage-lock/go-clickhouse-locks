package clickhouse_locks

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_storage "github.com/storage-lock/go-clickhouse-storage"
	"github.com/storage-lock/go-storage"
	go_iterator "github.com/golang-infrastructure/go-iterator"
	storage_lock "github.com/storage-lock/go-storage-lock"
	"github.com/stretchr/testify/assert"
)

// fakeConn 实现 clickhouse.Conn（= driver.Conn）的最小桩，仅为测试提供可比较的非空 key。
// 成功路径走内存假存储，不会真正调用这些方法。
// 注意：结构体带 id 字段——空结构体指针可能被复用为同一地址（zerobase），会导致
// BeanFactory 缓存误命中，错误用例必须用不同 id 构造独立连接。
type fakeConn struct{ id int }

func (f *fakeConn) Contributors() []string { return nil }
func (f *fakeConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{}, nil
}
func (f *fakeConn) Select(ctx context.Context, dest any, query string, args ...any) error {
	return nil
}
func (f *fakeConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return nil, errors.New("fakeConn: no rows")
}
func (f *fakeConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return &fakeRow{}
}
func (f *fakeConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, errors.New("fakeConn: no batch")
}
func (f *fakeConn) Exec(ctx context.Context, query string, args ...any) error { return nil }
func (f *fakeConn) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return nil
}
func (f *fakeConn) Ping(ctx context.Context) error { return nil }
func (f *fakeConn) Stats() driver.Stats          { return driver.Stats{} }
func (f *fakeConn) Close() error                 { return nil }

type fakeRow struct{}

func (r *fakeRow) Err() error              { return nil }
func (r *fakeRow) Scan(dest ...any) error  { return nil }
func (r *fakeRow) ScanStruct(dest any) error { return nil }

var (
	_ clickhouse.Conn = (*fakeConn)(nil)
	_ driver.Batch    = (*fakeBatch)(nil)
)

type fakeBatch struct{}

func (b *fakeBatch) Abort() error                        { return nil }
func (b *fakeBatch) Append(v ...any) error               { return nil }
func (b *fakeBatch) AppendStruct(v any) error            { return nil }
func (b *fakeBatch) Column(i int) driver.BatchColumn     { return nil }
func (b *fakeBatch) Flush() error                        { return nil }
func (b *fakeBatch) Send() error                         { return nil }
func (b *fakeBatch) IsSent() bool                        { return false }
func (b *fakeBatch) Rows() int                           { return 0 }
func (b *fakeBatch) Columns() []column.Interface         { return nil }

// fakeStorage 内存假存储：满足 CAS+可靠时间，走通锁创建与加解锁，不依赖真实 ClickHouse。
type fakeStorage struct {
	data map[string]string
	vers map[string]storage.Version
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{data: map[string]string{}, vers: map[string]storage.Version{}}
}

func (f *fakeStorage) GetName() string { return "fake-clickhouse-storage" }
func (f *fakeStorage) Capabilities() []storage.StorageCapability {
	return []storage.StorageCapability{storage.CapabilityCAS, storage.CapabilityReliableTime, storage.CapabilityAtomicDelete}
}
func (f *fakeStorage) Init(ctx context.Context) error  { return nil }
func (f *fakeStorage) Close(ctx context.Context) error { return nil }
func (f *fakeStorage) GetTime(ctx context.Context) (time.Time, error) {
	return time.Now(), nil
}
func (f *fakeStorage) Get(ctx context.Context, lockId string) (string, error) {
	v, ok := f.data[lockId]
	if !ok {
		return "", storage_lock.ErrLockNotFound
	}
	return v, nil
}
func (f *fakeStorage) CreateWithVersion(ctx context.Context, lockId string, version storage.Version, info *storage.LockInformation) error {
	if _, ok := f.data[lockId]; ok {
		return storage_lock.ErrLockAlreadyExists
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = version
	return nil
}
func (f *fakeStorage) UpdateWithVersion(ctx context.Context, lockId string, expected, newVersion storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrVersionMiss
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	f.data[lockId] = info.ToJsonString()
	f.vers[lockId] = newVersion
	return nil
}
func (f *fakeStorage) DeleteWithVersion(ctx context.Context, lockId string, expected storage.Version, info *storage.LockInformation) error {
	v, ok := f.vers[lockId]
	if !ok {
		return storage_lock.ErrLockNotFound
	}
	if v != expected {
		return storage_lock.ErrVersionMiss
	}
	delete(f.data, lockId)
	delete(f.vers, lockId)
	return nil
}
func (f *fakeStorage) List(ctx context.Context) (go_iterator.Iterator[*storage.LockInformation], error) {
	infos := make([]*storage.LockInformation, 0, len(f.data))
	for _, s := range f.data {
		info, err := storage.LockInformationFromJsonString(s)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return go_iterator.FromSlice(infos), nil
}

func withFakeClickHouseStorage() func() {
	old := newClickHouseStorage
	newClickHouseStorage = func(ctx context.Context, options *clickhouse_storage.ClickHouseStorageOptions) (storage.Storage, error) {
		return newFakeStorage(), nil
	}
	return func() { newClickHouseStorage = old }
}

func TestClickHouseClientConnectionManager(t *testing.T) {
	conn := clickhouse.Conn(&fakeConn{id: 1})
	m := &clickhouseClientConnectionManager{conn: conn}
	assert.Equal(t, "clickhouse-client-connection-manager", m.Name())

	ctx := context.Background()
	got, err := m.Take(ctx)
	assert.Nil(t, err)
	assert.Equal(t, conn, got)
	assert.Nil(t, m.Return(ctx, got))
	assert.Nil(t, m.Shutdown(ctx))
}

func TestGetClickHouseLockFactoryByConn(t *testing.T) {
	restore := withFakeClickHouseStorage()
	defer restore()

	ctx := context.Background()
	conn := clickhouse.Conn(&fakeConn{id: 1})

	factory, err := GetClickHouseLockFactoryByConn(ctx, conn)
	assert.Nil(t, err)
	assert.NotNil(t, factory)

	// 缓存命中
	factory2, err := GetClickHouseLockFactoryByConn(ctx, conn)
	assert.Nil(t, err)
	assert.Equal(t, factory, factory2)

	// 端到端加解锁
	lock, err := NewClickHouseLockByConn(ctx, conn, "test-clickhouse-lock")
	assert.Nil(t, err)
	assert.NotNil(t, lock)
	assert.Nil(t, lock.Lock(ctx, "owner-1"))
	assert.Nil(t, lock.UnLock(ctx, "owner-1"))
}

func TestGetClickHouseLockFactoryByConn_StorageError(t *testing.T) {
	old := newClickHouseStorage
	newClickHouseStorage = func(ctx context.Context, options *clickhouse_storage.ClickHouseStorageOptions) (storage.Storage, error) {
		return nil, errors.New("boom")
	}
	defer func() { newClickHouseStorage = old }()

	ctx := context.Background()
	// 独立连接避免命中成功用例的缓存
	errConn := clickhouse.Conn(&fakeConn{id: 2})

	_, err := GetClickHouseLockFactoryByConn(ctx, errConn)
	assert.NotNil(t, err)
	_, err = NewClickHouseLockByConn(ctx, errConn, "x")
	assert.NotNil(t, err)
}

func TestNewClickHouseStorage_DefaultPassthrough(t *testing.T) {
	// 直调默认透传体：空 options 触发 Check 快速失败，不联网。
	ctx := context.Background()
	_, err := newClickHouseStorage(ctx, &clickhouse_storage.ClickHouseStorageOptions{})
	assert.NotNil(t, err)
}
