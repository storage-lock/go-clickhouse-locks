package clickhouse_locks

import (
	"context"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_storage "github.com/storage-lock/go-clickhouse-storage"
	"github.com/storage-lock/go-storage"
	storage_lock "github.com/storage-lock/go-storage-lock"
	storage_lock_factory "github.com/storage-lock/go-storage-lock-factory"
)

type clickhouseClientConnectionManager struct {
	conn clickhouse.Conn
}

func (m *clickhouseClientConnectionManager) Name() string {
	return "clickhouse-client-connection-manager"
}

func (m *clickhouseClientConnectionManager) Take(ctx context.Context) (clickhouse.Conn, error) {
	return m.conn, nil
}

func (m *clickhouseClientConnectionManager) Return(ctx context.Context, connection clickhouse.Conn) error {
	return nil
}

func (m *clickhouseClientConnectionManager) Shutdown(ctx context.Context) error {
	return nil
}

var clickhouseClientBeanFactory = storage_lock_factory.NewStorageLockFactoryBeanFactory[clickhouse.Conn, clickhouse.Conn]()

// newClickHouseStorage 构造存储，包级变量以便单测注入假存储/构造失败。
// 默认透传真实 clickhouse_storage.NewClickHouseStorage。
// 本地无 ClickHouse 真后端时，单测注入内存假存储走通成功路径。
var newClickHouseStorage = func(ctx context.Context, options *clickhouse_storage.ClickHouseStorageOptions) (storage.Storage, error) {
	return clickhouse_storage.NewClickHouseStorage(ctx, options)
}

func NewClickHouseLockByConn(ctx context.Context, conn clickhouse.Conn, lockId string) (*storage_lock.StorageLock, error) {
	factory, err := GetClickHouseLockFactoryByConn(ctx, conn)
	if err != nil {
		return nil, err
	}
	return factory.CreateLock(lockId)
}

func GetClickHouseLockFactoryByConn(ctx context.Context, conn clickhouse.Conn) (*storage_lock_factory.StorageLockFactory[clickhouse.Conn], error) {
	return clickhouseClientBeanFactory.GetOrInit(ctx, conn, func(ctx context.Context) (*storage_lock_factory.StorageLockFactory[clickhouse.Conn], error) {
		cm := clickhouse_storage.NewClickHouseConnectionManagerFromConn(conn)
		options := clickhouse_storage.NewClickHouseStorageOptions().
			SetConnectionManager(cm).
			SetTimeProvider(&clickhouse_storage.LocalTimeProvider{})
		storage, err := newClickHouseStorage(ctx, options)
		if err != nil {
			return nil, err
		}
		return storage_lock_factory.NewStorageLockFactory[clickhouse.Conn](storage, nil), nil
	})
}
